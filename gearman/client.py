import collections
from gearman import compat
import logging
import os
import random
import weakref

import gearman.util

from gearman.connectionmanager import GearmanConnectionManager
from gearman.clienthandler import GearmanClientCommandHandler
from gearman.job import PRIORITY_NONE, PRIORITY_LOW, PRIORITY_HIGH, \
        JOB_UNKNOWN, JOB_PENDING
from gearman.errors import ConnectionError, ExceededConnectionAttempts, \
        ServerUnavailable
import gearman.log as log

# This number must be <= GEARMAN_UNIQUE_SIZE in gearman/libgearman/constants.h
RANDOM_UNIQUE_BYTES = 16

class GearmanClient(GearmanConnectionManager):
    """
    GearmanClient :: Interface to submit jobs to a Gearman server
    """
    command_handler_class = GearmanClientCommandHandler

    def __init__(self, host_ls=None, random_unique_bytes=RANDOM_UNIQUE_BYTES):
        super(GearmanClient, self).__init__(host_ls=host_ls)

        self.random_unique_bytes = random_unique_bytes

        # The authoritative copy of all requests that this client knows about
        # Ignores the fact if a request has been bound to a connection or not
        self.request_trace = weakref.WeakKeyDictionary(
            compat.defaultdict(collections.deque))

    def submit_job(self, task, data, unique=None, priority=PRIORITY_NONE, 
                   background=False, block=True, max_retries=0, 
                   timeout=None):
        """Submit a single job to any gearman server"""
        job = dict(task=task, data=data, unique=unique, priority=priority)

        completed_job_ls = self.submit_multiple_jobs(
            [job], background=background, 
            block=block, 
            max_retries=max_retries, 
            timeout=timeout)

        return gearman.util.advance_list(completed_job_ls)

    def submit_multiple_jobs(self, job_ls, background=False, 
                             block=True, max_retries=0, 
                             timeout=None):
        """Takes a list of job_ls with dicts of

        {'task': task, 'data': data, 'unique': unique, 'priority': priority}
        """
        assert type(job_ls) in (list, tuple, set), \
                "Expected multiple jobs, received 1?"

        # Convert all job dicts to job request objects
        request_ls = [
            self._create_job_request(job, background=background, 
                                     max_retries=max_retries) 
            for job in job_ls]

        return self._submit_job_requests(request_ls, block=block, 
                                         timeout=timeout)

    def get_job_status(self, request, timeout=None):
        """Fetch the job status of a single request"""
        request_ls = self.get_job_statuses([request], timeout=timeout)
        return gearman.util.advance_list(request_ls)

    def get_job_statuses(self, request_ls, timeout=None):
        """Fetch the job status of a multiple requests"""
        assert type(request_ls) in (list, tuple, set), "Expected multiple job requests, received 1?"

        for request in request_ls:
            request.status['last_time_received'] = request.status.get('time_received')
            request.job.handler.send_get_status_of_job(request)

        return self._wait_for_statuses(request_ls, timeout=timeout)


    def _submit_job_requests(self, request_ls, block=True, 
                                 timeout=None):
        """Take GearmanJobRequests, assign them connections, and request that 
        they be done.

        * Blocks until our jobs are accepted (should be fast) OR times out
        * Optionally blocks until jobs are all complete

        You MUST check the status of your requests after calling this function 
        as "timed_out" or "state == JOB_UNKNOWN" maybe True
        """
        assert type(request_ls) in (list, tuple, set), \
                "Expected multiple job requests, received 1?"

        stopwatch = gearman.util.Stopwatch(timeout)

        # We should always wait until our job is accepted, this should be fast
        time_remaining = stopwatch.get_time_remaining()

        self._blocking_submit(request_ls, timeout=time_remaining)

        # Optionally, we'll allow a user to wait until all jobs are complete 
        # with the same timeout
        time_remaining = stopwatch.get_time_remaining()

        if block and bool(time_remaining != 0.0):
            request_ls = self._wait_until_complete(request_ls, 
                                                   timeout=time_remaining)
        return request_ls

    def _blocking_submit(self, request_ls, timeout=None):
        """Go into a select loop until all our jobs have moved to STATE_PENDING
        """
        assert type(request_ls) in (list, tuple, set), \
                "Expected multiple job requests, received 1?"

        # Poll until we know we've gotten acknowledgement that our job's been 
        # accepted. If our connection fails while we're waiting for it to be 
        # accepted, automatically retry right here
        def has_pending_jobs():
            for request in request_ls:
                if request.state == JOB_UNKNOWN:
                    self.send_job_request(request)

            return compat.any(request.state == JOB_PENDING for
                              request in request_ls)

        self.poll(self.connection_ls, has_pending_jobs, has_pending_jobs, 
                  timeout=timeout)

        # Mark any job still in the queued state to timeout
        for request in request_ls:
            request.timed_out = (request.state == JOB_PENDING)

    def _wait_until_complete(self, request_ls, timeout=None):
        """Go into a select loop until all our jobs have completed or failed"""
        assert type(request_ls) in (list, tuple, set), \
                "Expected multiple job requests, received 1?"

        # Poll until we get responses for all our functions
        # Do NOT attempt to auto-retry connection failures as we have no idea 
        # how for a worker got
        def has_incomplete_jobs():
            for request in request_ls:
                if not request.complete and request.state != JOB_UNKNOWN:
                    return True

            return False

        self.poll(self.connection_ls, has_incomplete_jobs, has_incomplete_jobs,
                  timeout=timeout)

        # Mark any job still in the queued state to timeout
        for request in request_ls:
            request.timed_out = not request.complete

            if not request.timed_out:
                self.request_trace.pop(request, None)

        return request_ls 

    def _wait_for_statuses(self, request_ls, timeout=None):
        """Go into a select loop until we received statuses on all our requests"""
        assert type(request_ls) in (list, tuple, set), "Expected multiple job requests, received 1?"

        def is_status_changed(request):
            return request.status['time_received'] != request.status['last_time_received']

        # Poll to make sure we send out our request for a status update
        def has_untouched_jobs():
            for request in request_ls:
                if request.state != JOB_UNKNOWN and not is_status_changed(request):
                    return True

            return False

        self.poll(self.connection_ls, has_untouched_jobs, has_untouched_jobs, 
                  timeout=timeout)

        for request in request_ls:
            request.status = request.status or {}
            request.timed_out = not is_status_changed(request)

        return request_ls 

    def _create_job_request(self, job, background=False, max_retries=0):
        """Takes a dictionary with fields  
            {'task': task, 'unique': unique, 'data': data, 'priority': priority,
            'background': background}
        """
        # Make sure we have a unique identifier for ALL our tasks
        unique = job.get('unique')
        if not unique:
            unique = os.urandom(self.random_unique_bytes).encode('hex')

        priority = job.get('priority', PRIORITY_NONE)

        max_attempts = max_retries + 1

        request = self.job_request_class(
            self.job_class(handler=None, handle=None, task=job["task"], 
                           unique=unique, data=job["data"]),
            priority=priority, background=background, max_attempts=max_attempts)
        return request

    def _create_handler(self, request):
        """Return a live connection for the given hash"""
        # We'll keep track of the connections we're attempting to use so if we 
        # ever have to retry, we can use this history
        connections = self.request_trace.get(request, None)
        if not connections:
            shuffled_ls = list(self.connection_ls)
            random.shuffle(shuffled_ls)

            connections = collections.deque(shuffled_ls)
            self.request_trace[request] = connections 

        failed, chosen = 0, None
        for c in connections:
            try:
                chosen = self.establish_connection(c)
                break
            except ConnectionError as exc:
                failed += 1

        if not chosen:
            raise ServerUnavailable('Found no valid connections: %r' % 
                                    self.connection_ls)

        # Rotate our server list so we'll skip all our broken servers
        connections.rotate(-failed)
        return chosen.handler

    def send_job_request(self, request):
        """Attempt to send out a job request"""
        if request.connect_attempts >= request.max_connect_attempts:
            raise ExceededConnectionAttempts(
                'Exceeded %d connection attempt(s) :: %r' % 
                (request.max_connect_attempts, request))

        request.job.handler = self._create_handler(request)
        request.connect_attempts += 1
        request.timed_out = False

        request.job.handler.send_job_request(request)
        return request
