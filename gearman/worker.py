import sys
import random
import logging

from gearman import compat
from gearman.connection_manager import GearmanConnectionManager
from gearman.worker_handler import GearmanWorkerCommandHandler
from gearman.errors import ConnectionError, ServerUnavailable
from gearman.threadpool import GearmanGeventPool
from gearman.notification import _NotificationHandler, _NotificationConnection
import gearman.util as util
import gearman.log as log

POLL_TIMEOUT_IN_SECONDS = 60.0

class GearmanWorker(GearmanConnectionManager):
    """
    GearmanWorker :: Interface to accept jobs from a Gearman server
    """

    command_handler_class = GearmanWorkerCommandHandler

    def __init__(self, host_ls, concurrency=None, 
                 thread_pool_class=GearmanGeventPool):
        if not host_ls:
            raise RuntimeError("Job server address required")

        super(GearmanWorker, self).__init__(host_ls=host_ls)

        self._abilities = {}
        self._client_id = None

        self._notification_connection = None
        self._setup_notification_facility()
        self._terminated = False
        self._concurrency = concurrency
        self._thread_pool = thread_pool_class(concurrency) \
                if concurrency else None
        self.lock = thread_pool_class.Lock() \
                if concurrency else self.lock

        self._update_state()

    def _update_state(self):
        self.handler_state['abilities'] = self._abilities.keys()
        self.handler_state['client_id'] = self._client_id

    def _setup_notification_facility(self):
        self._notification_connection = _NotificationConnection(
            self, _NotificationHandler)
        self.connection_ls.append(self._notification_connection)
        self.has_internal_connection = True

    def _notify(self, **kwargs):
        self._notification_connection.send_command(**kwargs)

    def _enter_wonly_mode():
        for connection in self.connection_ls:
            connection.set_write_only()

    def handle_read(self, connection):
        if isinstance(connection, _NotificationConnection):
            command = connection.read_command()
            if command == 's':
                for connection in self.connection_ls:
                    connection.internal or connection.handler.prepare()
        else:
            super(GearmanWorker, self).handle_read(connection)

    def establish_connections(self):
        """Return a shuffled list of connections that are alive, and try to 
        reconnect to dead connections if necessary."""
        random.shuffle(self.connection_ls)
        connection_ls = []

        for connection in self.connection_ls:
            try:
                connection_ls.append(self.establish_connection(connection))
            except ConnectionError as error:
                log.msg("%r" % error, loglevel=logging.ERROR)

            log.msg("%r" % connection, loglevel=logging.DEBUG)

        if compat.all((c.internal for c in connection_ls)):
            raise ServerUnavailable('No valid server connections: %r' % self.connection_ls)

        return connection_ls

    def register_task(self, task, callback):
        """Register a function with this worker

        def function_callback(calling_gearman_worker, job):
            return job.data
        """
        self._abilities[task] = callback
        self._update_state()

        for connection in self.connection_ls:
            if connection.connected and not connection.internal:
                connection.handler.set_abilities(
                    self.handler_state['abilities']
                )

        return task

    def unregister_task(self, task):
        """Unregister a function with worker"""
        self._abilities.pop(task, None)
        self._update_state()

        for connection in self.connection_ls:
            if connection.connected and not connection.internal:
                connection.handler.set_abilities(
                    self.handler_state['abilities']
                )

        return task

    def set_client_id(self, client_id):
        """Notify the server that we should be identified as this client ID"""
        self._client_id = client_id
        self._update_state()

        for connection in self.connection_ls:
            if connection.connected and not connection.internal:
                connection.handler.set_abilities(
                    self.handler_state['abilities']
                )

        return client_id

    def work(self, poll_timeout=POLL_TIMEOUT_IN_SECONDS):
        """Loop indefinitely, complete tasks from all connections."""
        workable = True

        while workable:
            workable = self.poll(self.establish_connections(), 
                                 lambda: True, lambda: not self.terminated,
                                 timeout=poll_timeout)

            # try to finish unfinished jobs before exit this loop
            if not workable and self.has_pending_stuff():
                # not gonna grap another new job, because worker's dying
                self._enter_wonly_mode()
                workable = True

        if self._thread_pool:
            self._thread_pool.terminate()

        self.shutdown()

    def terminate(self):
        self._terminated = True
        self._notify()

    def send_job_status(self, job, numerator, denominator):
        job.handler.send_job_status(job, numerator=numerator, 
                                    denominator=denominator)
        self._notify()

    def send_job_complete(self, job, data):
        job.handler.send_job_complete(job, data=data)
        self._notify(command='s')

    def send_job_failure(self, job):
        job.handler.send_job_failure(job)
        self._notify(command='s')

    def send_job_exception(self, job, data):
        job.handler.send_job_exception(job, data=("%r" % (data,)))
        job.handler.send_job_failure(job)
        self._notify(command='s')

    def send_job_data(self, job, data):
        job.handler.send_job_data(job, data=data)
        self._notify()

    def send_job_warning(self, job, data):
        job.handler.send_job_warning(job, data=data)
        self._notify()

    def process_job(self, handler, job_handle, task, unique, data):
        callback = self._abilities[task]
        job = self.job_class(handler, job_handle, task, unique, data)

        if not self._concurrency:
            try:
                result = callback(self, job)
                self.send_job_complete(job, result)
            except Exception:
                self.send_job_exception(job, sys.exc_info())
        else:
            try:
                self._thread_pool.spawn(callback, self, job)
            except Exception:
                self.send_job_exception(job, sys.exc_info())

    def reserve_thread(self):
        if not self._concurrency:
            return True
        else:
            return self._thread_pool.reserve()

    def release_thread(self):
        if not self._concurrency:
            return
        else:
            self._thread_pool.release()

    def has_pending_stuff(self):
        if not self._concurrency:
            return False
        else:
            return self._thread_pool.busy()

