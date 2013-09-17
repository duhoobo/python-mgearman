import weakref
import collections
from gearman.constants import PRIORITY_NONE, JOB_UNKNOWN, JOB_PENDING, \
        JOB_CREATED, JOB_FAILED, JOB_COMPLETE

class GearmanJob(object):
    """Represents the basics of a job... used in GearmanClient / GearmanWorker 
    to represent job states"""
    def __init__(self, handler, handle, task, unique, data):
        self.handler = weakref.proxy(handler)

        self.handle = handle
        self.task = task
        self.unique = unique
        self.data = data

    @property
    def handler(self):
        return self.handler

    @handler.setter
    def handler(self, handler):
        self.handler = handler

    def to_dict(self):
        return dict(task=self.task, job_handle=self.handle, unique=self.unique, 
                    data=self.data)

    def __repr__(self):
        return '<GearmanJob handler/handle=(%r, %r), task=%s, unique=%s, data=%r>' % (
            self.handler, self.handle, self.task, self.unique, self.data)


class GearmanJobRequest(object):
    """Represents a job request... used in GearmanClient to represent job 
    states"""
    def __init__(self, job, priority=PRIORITY_NONE, 
                 background=False, max_attempts=1):
        self.job = job 

        self.priority = priority
        self.background = background

        self.connect_attempts = 0
        self.max_connect_attempts = max_attempts

        self.initialize()

    def initialize(self):
        # Holds WORK_COMPLETE responses
        self.result = None

        # Holds WORK_EXCEPTION responses
        self.exception = None

        # Queues to hold WORK_WARNING, WORK_DATA responses
        self.warning_updates = collections.deque()
        self.data_updates = collections.deque()

        # Holds WORK_STATUS / STATUS_REQ responses
        self.status = {}

        self.state = JOB_UNKNOWN
        self.timed_out = False

    def reset(self):
        self.initialize()
        self.handle = None

    @property
    def status_updates(self):
        """Deprecated since 2.0.1, removing in next major release"""
        output_queue = collections.deque()
        if self.status:
            output_queue.append((self.status.get('numerator', 0), self.status.get('denominator', 0)))

        return output_queue

    @property
    def server_status(self):
        """Deprecated since 2.0.1, removing in next major release"""
        return self.status

    @property
    def job(self):
        return self.job

    @property
    def complete(self):
        background_complete = bool(self.background and 
                                   self.state in (JOB_CREATED))
        foreground_complete = bool(not self.background 
                                   and self.state in (JOB_FAILED, JOB_COMPLETE))

        actually_complete = background_complete or foreground_complete
        return actually_complete

    def __repr__(self):
        formatted_representation = "<GearmanJobRequest task=%r, unique=%r, priority=%r, background=%r, state=%r, timed_out=%r>"
        return formatted_representation % (self.job.task, self.job.unique, 
                                           self.priority, self.background, 
                                           self.state, self.timed_out)
