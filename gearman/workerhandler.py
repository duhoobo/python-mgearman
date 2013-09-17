import logging
from gearman.commandhandler import GearmanCommandHandler
import gearman.protocol as protocol
import gearman.log as log

class GearmanWorkerCommandHandler(GearmanCommandHandler):
    """GearmanWorker state machine on a per connection basis

    A worker can be in the following distinct states:
        SLEEP         -> Doing nothing, can be awoken
        AWAKE         -> Transitional state (for NOOP)
        AWAITING_JOB  -> Awaiting a server response
    """
    def __init__(self, connection=None):
        super(GearmanWorkerCommandHandler, self).__init__(connection)

        self._grabbing = False
        self._waiting = False
        self._abilities = []
        self._client_id = None

    def set_state(self, abilities=None, client_id=None):
        self.set_client_id(client_id)
        self.set_abilities(abilities)

        self._sleep()

    def set_abilities(self, abilities):
        assert type(abilities) in (list, tuple)
        self._abilities = abilities

        self.send_command(protocol.GEARMAN_COMMAND_RESET_ABILITIES)
        for ability in self._abilities:
            self.send_command(protocol.GEARMAN_COMMAND_CAN_DO, task=ability)

    def set_client_id(self, client_id):
        self._client_id = client_id

        if self._client_id is not None:
            self.send_command(protocolGEARMAN_COMMAND_SET_CLIENT_ID, 
                              client_id=self._client_id)

    def send_job_status(self, current_job, numerator, denominator):
        self.send_command(protocol.GEARMAN_COMMAND_WORK_STATUS, 
                          job_handle=current_job.handle, 
                          numerator=str(numerator), 
                          denominator=str(denominator))

    def send_job_complete(self, current_job, data):
        """Removes a job from the queue if its backgrounded"""
        self.send_command(protocol.GEARMAN_COMMAND_WORK_COMPLETE, 
                          job_handle=current_job.handle, 
                          data=self.encode_data(data))

    def send_job_failure(self, current_job):
        """Removes a job from the queue if its backgrounded"""
        self.send_command(protocol.GEARMAN_COMMAND_WORK_FAIL, 
                          job_handle=current_job.handle)

    def send_job_exception(self, current_job, data):
        # Using GEARMAND_COMMAND_WORK_EXCEPTION is not recommended at time of 
        # this writing [2010-02-24]
        # http://groups.google.com/group/gearman/browse_thread/thread/5c91acc31bd10688/529e586405ed37fe
        #
        self.send_command(protocol.GEARMAN_COMMAND_WORK_EXCEPTION, 
                          job_handle=current_job.handle, 
                          data=self.encode_data(data))

    def send_job_data(self, current_job, data):
        self.send_command(protocol.GEARMAN_COMMAND_WORK_DATA, 
                          job_handle=current_job.handle, 
                          data=self.encode_data(data))

    def send_job_warning(self, current_job, data):
        self.send_command(protocol.GEARMAN_COMMAND_WORK_WARNING, 
                          job_handle=current_job.handle, 
                          data=self.encode_data(data))

    def _grab_job(self):
        self.send_command(protocol.GEARMAN_COMMAND_GRAB_JOB_UNIQ)

    def _sleep(self):
        self.send_command(protocol.GEARMAN_COMMAND_PRE_SLEEP)

    def prepare(self):
        if self._waiting:
            log.msg("%r is ready for another job" % self)
            self._sleep()
            self._waiting = False

    def recv_noop(self):
        """Transition from being SLEEP --> AWAITING_JOB / SLEEP

          AWAITING_JOB -> AWAITING_JOB :: Noop transition, we're already awaiting a job
        SLEEP -> AWAKE -> AWAITING_JOB :: Transition if we can acquire the worker job lock
        SLEEP -> AWAKE -> SLEEP        :: Transition if we can NOT acquire a worker job lock
        """

        if self.connection.write_only:
            return True

        if self.connection.manager.reserve():
            self.grabbing = True
            self._grab_job()
        else:
            log.msg("%r hibernated" % self)
            self._waiting = True

        return True

    def recv_no_job(self):
        """Transition from being AWAITING_JOB --> SLEEP

        AWAITING_JOB -> SLEEP :: Always transition to sleep if we have nothing 
                                 to do
        """
        self.grabbing = False
        self.connection.manager.release()
        self._sleep()

        return True

    def recv_error(self):
        if self.grabbing:
            self.grabbing = False
            self.connection.manager.release()

        self._sleep()
        return True

    def recv_job_assign_uniq(self, job_handle, task, unique, data):
        """Transition from being AWAITING_JOB --> EXECUTE_JOB --> SLEEP

        AWAITING_JOB -> EXECUTE_JOB -> SLEEP :: Always transition once we're 
                                                given a job
        """
        assert task in self._abilities, '%s not found in %r' % (
            task, self._abilities)

        self.connection.manager.process_job(self, job_handle, task, unique,
                                            self.decode_data(data))
        self.grabbing = False
        self._sleep()

        return True

    def recv_job_assign(self, job_handle, task, data):
        """JOB_ASSIGN and JOB_ASSIGN_UNIQ are essentially the same"""
        return self.recv_job_assign_uniq(job_handle=job_handle, task=task, 
                                         unique=None, data=data)

    def close(self):
        if self.grabbing:
            self.grabbing = False
            self.connection.manager.release()

