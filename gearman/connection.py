import array
import collections
import logging
import socket
import struct
import time
import weakref
import logging

from gearman.errors import ConnectionError, ProtocolError, ServerUnavailable
from gearman.constants import DEFAULT_GEARMAN_PORT
import gearman.protocol as protocol
import gearman.log as log


class GearmanBaseConnection(object):
    def __init__(self, manager, handler_class):
        self.manager = weakref.proxy(manager)
        self.handler = handler_class(self)
        self.internal = False
        self.connected = False

    def fileno(self):
        raise NotImplementedError
    def writable(self):
        raise NotImplementedError
    def readable(self):
        raise NotImplementedError
    def connect(self, **kwargs):
        raise NotImplementedError
    def read_command(self):
        raise NotImplementedError
    def send_command(self, command, **kwargs):
        raise NotImplementedError
    def close(self):
        raise NotImplementedError
    def on_io_error(self):
        raise NotImplementedError
    def throw_exception(self, message=None, exception=None):
        raise NotImplementedError
    def __repr__(self):
        raise NotImplementedError


class GearmanConnection(GearmanBaseConnection):
    """A connection between a client/worker and a server.  Can be used to 
    reconnect (unlike a socket)

    Wraps a socket and provides the following functionality:
        Full read/write methods for Gearman BINARY commands and responses
        Full read/write methods for Gearman SERVER commands and responses (using
        GEARMAN_COMMAND_TEXT_COMMAND)

        Manages raw data buffers for socket-level operations
        Manages command buffers for gearman-level operations

    All I/O and buffering should be done in this class
    """
    connect_cooldown_seconds = 1.0

    def __init__(self, manager, handler_klass, host=None, 
                 port=DEFAULT_GEARMAN_PORT):
        super(GearmanConnection, self).__init__(manager, handler_klass)
        self._server_host = host
        self._server_port = port

        if host is None:
            raise ServerUnavailable("No host specified")

        self._reset_connection()

    def _reset_connection(self):
        """Reset the state of this connection"""
        self.connected = False
        self.sock = None
        self.write_only = False

        self.allowed_connect_time = 0.0

        # Reset all our raw data buffers
        self._incoming_buffer = array.array('c')
        self._outgoing_buffer = ''

        # Toss all commands we may have sent or received
        self._incoming_commands = collections.deque()
        self._outgoing_commands = collections.deque()

    def fileno(self):
        """Implements fileno() for use with select.select()"""
        if not self.sock:
            self.throw_exception(message='no socket set')

        return self.sock.fileno()

    def get_address(self):
        """Returns the host and port"""
        return (self._server_host, self._server_port)

    def writable(self):
        """Returns True if we have data to write"""
        with self.manager.lock:
            return self.connected and bool(self._outgoing_commands or 
                                           self._outgoing_buffer)

    def readable(self):
        """Returns True if we might have data to read"""
        return self.connected and not self.write_only

    def connect(self, **state):
        """Connect to the server. Raise ConnectionError if connection fails."""
        if self.connected:
            self.throw_exception(message='connection already established')

        current_time = time.time()
        if current_time < self.allowed_connect_time:
            self.throw_exception(message='attempted to connect too often')

        self.allowed_connect_time = current_time + self.connect_cooldown_seconds

        self._reset_connection()
        self._create_client_socket()
        self.handler.set_state(**state)

        self.connected = True

    def _create_client_socket(self):
        """Creates a client side socket and subsequently binds/configures our 
        socket options"""
        if self.sock:
            self.throw_exception(message='socket already bound')

        try:
            new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            new_sock.connect((self._server_host, self._server_port))

            new_sock.setblocking(0)
            new_sock.settimeout(0.0)
            new_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 
                                     struct.pack('L', 1))
        except socket.error, socket_exception:
            new_sock.close()
            self.throw_exception(exception=socket_exception)

        self.sock = new_sock 

    def read_command(self):
        """Reads a single command from the command queue"""
        if not self._incoming_commands:
            return None

        return self._incoming_commands.popleft()

    def read_commands_from_buffer(self):
        """Reads data from buffer --> command_queue"""
        received_commands = 0
        while True:
            cmd_type, cmd_args, cmd_len = self._unpack_command(self._incoming_buffer)
            if not cmd_len:
                break

            received_commands += 1

            # Store our command on the command queue
            # Move the self._incoming_buffer forward by the number of bytes we just read
            self._incoming_commands.append((cmd_type, cmd_args))
            self._incoming_buffer = self._incoming_buffer[cmd_len:]

        self.handler.fetch_commands()

        return received_commands

    def read_data_from_socket(self, bytes_to_read=4096):
        """Reads data from socket --> buffer"""
        if not self.connected:
            self.throw_exception(message='disconnected')

        recv_buffer = ''
        try:
            recv_buffer = self.sock.recv(bytes_to_read)
        except socket.error, socket_exception:
            self.throw_exception(exception=socket_exception)

        if len(recv_buffer) == 0:
            self.throw_exception(message='remote disconnected')

        self._incoming_buffer.fromstring(recv_buffer)
        return len(self._incoming_buffer)

    def _unpack_command(self, given_buffer):
        """Conditionally unpack a binary command or a text based server command"""
        if not given_buffer:
            cmd_type, cmd_args, cmd_len = None, None, 0
        elif given_buffer[0] == protocol.NULL_CHAR:
            # We'll be expecting a response if we know we're a client side command
            cmd_type, cmd_args, cmd_len = protocol.parse_binary_command(
                given_buffer)
        else:
            cmd_type, cmd_args, cmd_len = protocol.parse_text_command(
                given_buffer)

        log.msg("%s - recv - %s - %r" %  (
            id(self), protocol.get_command_name(cmd_type), cmd_args), 
            loglevel=logging.DEBUG)

        return cmd_type, cmd_args, cmd_len

    def send_command(self, cmd_type, cmd_args):
        """Adds a single gearman command to the outgoing command queue"""
        with self.manager.lock:
            self._outgoing_commands.append((cmd_type, cmd_args))

    def send_commands_to_buffer(self):
        """Sends and packs commands -> buffer"""
        packed_data = [self._outgoing_buffer]

        with self.manager.lock:
            if not self._outgoing_commands:
                return

            while self._outgoing_commands:
                cmd_type, cmd_args = self._outgoing_commands.popleft()
                packed_command = self._pack_command(cmd_type, cmd_args)
                packed_data.append(packed_command)

        self._outgoing_buffer = ''.join(packed_data)

    def send_data_to_socket(self):
        """Send data from buffer -> socket

        Returns remaining size of the output buffer
        """
        if not self.connected:
            self.throw_exception(message='disconnected')

        if not self._outgoing_buffer:
            return 0

        try:
            bytes_sent = self.sock.send(self._outgoing_buffer)
        except socket.error, socket_exception:
            self.throw_exception(exception=socket_exception)

        if bytes_sent == 0:
            self.throw_exception(message='remote disconnected')

        self._outgoing_buffer = self._outgoing_buffer[bytes_sent:]
        return len(self._outgoing_buffer)

    def _pack_command(self, cmd_type, cmd_args):
        """Converts a command to its raw binary format"""
        if cmd_type not in protocol.GEARMAN_PARAMS_FOR_COMMAND:
            raise ProtocolError('Unknown command: %r' % 
                                protocol.get_command_name(cmd_type))

        log.msg("%s - send - %s - %r" % (
            id(self), protocol.get_command_name(cmd_type), cmd_args), 
            logging.DEBUG)

        if cmd_type == protocol.GEARMAN_COMMAND_TEXT_COMMAND:
            return protocol.pack_text_command(cmd_type, cmd_args)
        else:
            return protocol.pack_binary_command(cmd_type, cmd_args)

    def close(self):
        """Shutdown our existing socket and reset all of our connection data"""
        try:
            if self.sock:
                self.sock.close()
        except socket.error:
            pass

        self.handler.close()
        self._reset_connection()

    def on_io_error(self):
        self.handler.on_io_error()

    def throw_exception(self, message=None, exception=None):
        # Mark us as disconnected but do NOT call self._reset_connection()
        # Allows catchers of ConnectionError a chance to inspect the last state of this connection
        self.connected = False

        if exception:
            message = repr(exception)

        rewritten_message = "<%s:%d> %s" % (self._server_host, self._server_port, message)
        raise ConnectionError(rewritten_message)

    def __repr__(self):
        return ('<GearmanConnection %s:%d connected=%s internal=%s>' %
            (self._server_host, self._server_port, self.connected, self.internal))

