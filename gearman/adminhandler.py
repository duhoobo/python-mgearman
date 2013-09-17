import collections
import logging

from gearman.commandhandler import GearmanCommandHandler
from gearman.errors import ProtocolError, InvalidAdminClientState
import gearman.protocol as protocol
import gearman.log as log

GEARMAN_ALLOWED_ADMIN_COMMANDS = set([
    protocol.GEARMAN_ADMIN_COMMAND_STATUS, 
    protocol.GEARMAN_ADMIN_COMMAND_VERSION, 
    protocol.GEARMAN_ADMIN_COMMAND_WORKERS, 
    protocol.GEARMAN_ADMIN_COMMAND_MAXQUEUE, 
    protocol.GEARMAN_ADMIN_COMMAND_SHUTDOWN,
])


class GearmanAdminCommandHandler(GearmanCommandHandler):
    """Special GEARMAN_COMMAND_TEXT_COMMAND command handler that'll parse text 
    responses from the server"""

    STATUS_RESPONSE_FIELDS = 4
    WORKERS_RESPONSE_FIELDS = 4

    def __init__(self, connection=None):
        super(GearmanAdminCommandHandler, self).__init__(connection=connection)
        self._sent_commands = collections.deque()
        self._recv_responses = collections.deque()

        self._status_response = []
        self._workers_response = []

    @property
    def response_ready(self):
        return bool(self._recv_responses)

    def pop_response(self):
        if not self._sent_commands or not self._recv_responses:
            raise InvalidAdminClientState('Attempted to pop a response for a command that is not ready')

        sent_command = self._sent_commands.popleft()
        recv_response = self._recv_responses.popleft()
        return sent_command, recv_response

    def send_text_command(self, command_line):
        """Send our administrative text command"""

        command = None
        for allowed_command in GEARMAN_ALLOWED_ADMIN_COMMANDS:
            if command_line.startswith(allowed_command):
                command = allowed_command
                break

        if not command:
            raise ProtocolError('Attempted to send an unknown server command: %r' % command_line)

        self._sent_commands.append(command)

        output_text = '%s\n' % command_line
        self.send_command(protocol.GEARMAN_COMMAND_TEXT_COMMAND, raw_text=output_text)

    def send_echo_request(self, echo_string):
        """Send our administrative text command"""
        self._sent_commands.append(protocol.GEARMAN_COMMAND_ECHO_REQ)
        self.send_command(protocol.GEARMAN_COMMAND_ECHO_REQ, data=echo_string)

    def recv_echo_res(self, data):
        self._recv_responses.append(data)
        return False

    def recv_text_command(self, raw_text):
        """Catch GEARMAN_COMMAND_TEXT_COMMAND's and forward them onto their 
        respective recv_server_* callbacks"""

        if not self._sent_commands:
            raise InvalidAdminClientState('Received an unexpected server response')

        # Peek at the first command
        cmd_type = self._sent_commands[0]
        method_name = 'recv_server_%s' % cmd_type

        method = getattr(self, method_name, None)
        if not method:
            log.msg('Could not handle command: %r - %r' % (cmd_type, raw_text),
                   logging.ERROR)
            raise ValueError('Could not handle command: %r - %r' % (cmd_type, 
                                                                    raw_text))
        return method(raw_text)

    def recv_server_status(self, raw_text):
        """Slowly assemble a server status message line by line"""

        if raw_text == '.':
            self._recv_responses.append(tuple(self._status_response))
            self._status_response = []
            return False

        token_ls = raw_text.split('\t')
        if len(token_ls) != self.STATUS_RESPONSE_FIELDS:
            raise ProtocolError('Received %d tokens, expected %d tokens: %r' % 
                                (len(token_ls), self.STATUS_RESPONSE_FIELDS,
                                 token_ls))
        status = {}
        status['task'] = token_ls[0]
        status['queued'] = int(token_ls[1])
        status['running'] = int(token_ls[2])
        status['workers'] = int(token_ls[3])
        self._status_response.append(status)

        return True

    def recv_server_version(self, raw_text):
        """Version response is a simple passthrough"""

        self._recv_responses.append(raw_text)
        return False

    def recv_server_workers(self, raw_text):
        """Slowly assemble a server workers message line by line"""

        if raw_text == '.':
            self._recv_responses.append(tuple(self._workers_response))
            self._workers_response = []
            return False

        token_ls = raw_text.split(' ')
        if len(token_ls) < self.WORKERS_RESPONSE_FIELDS:
            raise ProtocolError('Received %d tokens, expected >= 4 tokens: %r' %
                                (len(token_ls), token_ls))

        if token_ls[3] != ':':
            raise ProtocolError('Malformed worker response: %r' % (token_ls, ))

        workers = {}
        workers['fd'] = token_ls[0]
        workers['ip'] = token_ls[1]
        workers['client_id'] = token_ls[2]
        workers['tasks'] = tuple(token_ls[4:])
        self._workers_response.append(workers)
        return True

    def recv_server_maxqueue(self, raw_text):
        """Maxqueue response is a simple passthrough"""

        if raw_text != 'OK':
            raise ProtocolError("Expected 'OK', received: %s" % raw_text)

        self._recv_responses.append(raw_text)
        return False

    def recv_server_shutdown(self, raw_text):
        """Shutdown response is a simple passthrough"""

        self._recv_responses.append(None)
        return False
