import collections
import time
import logging
import weakref

from gearman.commandhandler import GearmanCommandHandler
from gearman.job import JOB_UNKNOWN, JOB_PENDING, JOB_CREATED, \
        JOB_FAILED, JOB_COMPLETE
from gearman.errors import InvalidClientState
import gearman.protocol as protocol
import gearman.log as log

class GearmanClientCommandHandler(GearmanCommandHandler):
    """Maintains the state of this connection on behalf of a GearmanClient"""
    def __init__(self, connection=None):
        super(GearmanClientCommandHandler, self).__init__(connection=connection)

        # When we first submit jobs, we don't have a handle assigned yet... 
        # these handles will be returned in the order of submission
        self.request_queue = collections.deque()
        self.request_trace = weakref.WeakValueDictionary()

    def send_job_request(self, request):
        """Register a newly created job request"""
        self._assert_request_state(request, JOB_UNKNOWN)

        cmd_type = protocol.get_background_cmd_type(request.background, 
                                                    request.priority)

        data = self.encode_data(request.job.data)
        self.send_command(cmd_type, task=request.job.task, 
                          unique=request.job.unique, data=data)

        # Once this command is sent, our request needs to wait for a handle
        request.state = JOB_PENDING

        self.request_queue.append(request)

    def send_get_status_of_job(self, request):
        """Forward the status of a job"""
        self.request_trace[request.job.handle] = request
        self.send_command(protocol.GEARMAN_COMMAND_GET_STATUS, 
                          job_handle=request.job.handle)

    def on_io_error(self):
        for pending_request in self.request_queue:
            pending_request.state = JOB_UNKNOWN

        for inflight_request in self.request_trace.itervalues():
            inflight_request.state = JOB_UNKNOWN

    def _assert_request_state(self, request, expected_state):
        if request.state != expected_state:
            raise InvalidClientState(
                "Expected handle (%s) to be in state %r, got %r" % (
                    request.job.handle, expected_state, request.state))


    def recv_job_created(self, job_handle):
        if not self.request_queue:
            raise InvalidClientState('Received a job_handle with no pending requests')

        # If our client got a JOB_CREATED, our request now has a server handle
        request = self.request_queue.popleft()
        self._assert_request_state(request, JOB_PENDING)

        # Update the state of this request
        request.job.handle = job_handle
        request.state = JOB_CREATED
        self.request_trace[request.job.handle] = request

        return True

    def recv_work_data(self, job_handle, data):
        # Queue a WORK_DATA update
        request = self.request_trace[job_handle]
        self._assert_request_state(request, JOB_CREATED)

        request.data_updates.append(self.decode_data(data))

        return True

    def recv_work_warning(self, job_handle, data):
        # Queue a WORK_WARNING update
        request = self.request_trace[job_handle]
        self._assert_request_state(request, JOB_CREATED)

        request.warning_updates.append(self.decode_data(data))

        return True

    def recv_work_complete(self, job_handle, data):
        # Update the state of our request and store our returned result
        request = self.request_trace[job_handle]
        self._assert_request_state(request, JOB_CREATED)

        request.result = self.decode_data(data)
        request.state = JOB_COMPLETE
        self.request_trace.pop(request.job.handle, None)

        return True

    def recv_work_fail(self, job_handle):
        # Update the state of our request and mark this job as failed
        request = self.request_trace[job_handle]
        self._assert_request_state(request, JOB_CREATED)

        request.state = JOB_FAILED
        self.request_trace.pop(request.job.handle, None)

        return True

    def recv_work_exception(self, job_handle, data):
        # Using GEARMAND_COMMAND_WORK_EXCEPTION is not recommended at time of this writing [2010-02-24]
        # http://groups.google.com/group/gearman/browse_thread/thread/5c91acc31bd10688/529e586405ed37fe
        #
        request = self.request_trace[job_handle]
        self._assert_request_state(request, JOB_CREATED)

        request.exception = self.decode_data(data)

        return True

    def recv_work_status(self, job_handle, numerator, denominator):
        # Queue a WORK_STATUS update
        request = self.request_trace[job_handle]
        self._assert_request_state(request, JOB_CREATED)

        # The protocol spec is ambiguous as to what type the numerator and denominator is...
        # But according to Eskil, gearmand interprets these as integers
        request.status = {
            'handle': job_handle,
            'known': True,
            'running': True,
            'numerator': int(numerator),
            'denominator': int(denominator),
            'time_received': time.time()
        }

        return True

    def recv_status_res(self, job_handle, known, running, numerator, denominator):
        # If we received a STATUS_RES update about this request, update our 
        # known status
        request = self.request_trace[job_handle]

        job_known = bool(known == '1')

        # Make our status response Python friendly
        request.status = {
            'handle': job_handle,
            'known': job_known,
            'running': bool(running == '1'),
            'numerator': int(numerator),
            'denominator': int(denominator),
            'time_received': time.time()
        }

        # If the server doesn't know about this request, we no longer need to track it
        if not job_known:
            self.request_trace.pop(request.job.handle, None)

        return True
