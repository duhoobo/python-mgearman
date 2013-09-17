import logging
import time

from gearman import util

from gearman.connection_manager import GearmanConnectionManager
from gearman.admin_client_handler import GearmanAdminClientCommandHandler
from gearman.errors import ConnectionError, InvalidAdminClientState, \
        ServerUnavailable
from gearman.protocol import GEARMAN_COMMAND_ECHO_RES, \
        GEARMAN_COMMAND_ECHO_REQ, GEARMAN_SERVER_COMMAND_STATUS, \
        GEARMAN_SERVER_COMMAND_VERSION, GEARMAN_SERVER_COMMAND_WORKERS, \
        GEARMAN_SERVER_COMMAND_MAXQUEUE, GEARMAN_SERVER_COMMAND_SHUTDOWN
import gearman.log as log

ECHO_STRING = "ping? pong!"
DEFAULT_ADMIN_CLIENT_TIMEOUT = 10.0


class GearmanAdmin(GearmanConnectionManager):
    """GearmanAdmin:: Interface to send/receive administrative commands to a Gearman server

    This client acts as a BLOCKING client and each call will poll until it receives a satisfactory server response

    http://gearman.org/index.php?id=protocol
    See section 'Administrative Protocol'
    """
    command_handler_class = GearmanAdminClientCommandHandler

    def __init__(self, host_ls=None, timeout=DEFAULT_ADMIN_CLIENT_TIMEOUT):
        super(GearmanAdmin, self).__init__(host_ls=host_ls)
        self.timeout = timeout

        self.connection = util.advance_list(self.connection_ls)
        self.handler = None

    def _create_handler(self):
        try:
            self.establish_connection(self.connection)
        except ConnectionError:
            raise ServerUnavailable('Found no valid connections in list: %r' % 
                                    self.connection_ls)

        return self.connection.handler

    def ping_server(self):
        """Sends off a debugging string to execute an application ping on the 
        Gearman server"""
        start_time = time.time()

        self.handler = self.create_handler()
        self.handler.send_echo_request(ECHO_STRING)
        response = self._wait_for_response(GEARMAN_COMMAND_ECHO_REQ)
        if response != ECHO_STRING:
            raise InvalidAdminClientState("Echo string mismatch: got %s, expected %s" % (server_response, ECHO_STRING))

        return time.time() - start_time

    def send_maxqueue(self, task, max_size):
        """Sends a request to change the maximum queue size for a given task"""

        self.handler = self._create_handler()
        self.handler.send_text_command('%s %s %s' % (
            GEARMAN_SERVER_COMMAND_MAXQUEUE, task, max_size))
        return self._wait_for_response(GEARMAN_SERVER_COMMAND_MAXQUEUE)

    def send_shutdown(self, graceful=True):
        """Sends a request to shutdown the connected gearman server"""

        command = GEARMAN_SERVER_COMMAND_SHUTDOWN
        if graceful:
            command += ' graceful'

        self.handler = self._create_handler()
        self.handler.send_text_command(command)
        return self._wait_for_response(GEARMAN_SERVER_COMMAND_SHUTDOWN)

    def get_status(self):
        """Retrieves a list of all registered tasks and reports how many 
        items/workers are in the queue"""

        self.handler = self._create_handler()
        self.handler.send_text_command(GEARMAN_SERVER_COMMAND_STATUS)
        return self._wait_for_response(GEARMAN_SERVER_COMMAND_STATUS)

    def get_version(self):
        """Retrieves the version number of the Gearman server"""
        self.handler = self._create_handler()
        self.handler.send_text_command(GEARMAN_SERVER_COMMAND_VERSION)
        return self._wait_for_response(GEARMAN_SERVER_COMMAND_VERSION)

    def get_workers(self):
        """Retrieves a list of workers and reports what tasks they're operating 
        on"""

        self.handler = self._create_handler()
        self.handler.send_text_command(GEARMAN_SERVER_COMMAND_WORKERS)
        return self._wait_for_response(GEARMAN_SERVER_COMMAND_WORKERS)

    def _wait_for_response(self, expected_cmd_type):

        def has_pending_response():
            return (not self.handler.response_ready)

        self.poll([self.connection], has_pending_response, has_pending_response,
                  timeout=self.timeout)

        if not self.handler.response_ready:
            raise InvalidAdminClientState('Admin client timed out after %f second(s)' % self.timeout)

        cmd_type, cmd_resp = self.handler.pop_response()
        if cmd_type != expected_cmd_type:
            raise InvalidAdminClientState('Received an unexpected response...
                                          got command %r, expecting command %r'
                                          % (cmd_type, expected_cmd_type))

        return cmd_resp

