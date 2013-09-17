import logging

import gearman.io
import gearman.util
from gearman.connection import GearmanConnection
from gearman.errors import ConnectionError, ServerUnavailable
from gearman.job import GearmanJob, GearmanJobRequest
from gearman import compat

gearman_logger = logging.getLogger(__name__)

class DataEncoder(object):
    @classmethod
    def encode(cls, encodable_object):
        raise NotImplementedError

    @classmethod
    def decode(cls, decodable_string):
        raise NotImplementedError

class NoopEncoder(DataEncoder):
    """Provide common object dumps for all communications over gearman"""
    @classmethod
    def _enforce_byte_string(cls, given_object):
        if type(given_object) != str:
            raise TypeError("Expecting byte string, got %r" % type(given_object))

    @classmethod
    def encode(cls, encodable_object):
        cls._enforce_byte_string(encodable_object)
        return encodable_object

    @classmethod
    def decode(cls, decodable_string):
        cls._enforce_byte_string(decodable_string)
        return decodable_string

class NoopLock(object):
    def acquire(self): 
        pass

    def release(self):
        pass

    def __enter__(self):
        self.acquire()

    def __exit__(self, type, value, traceback):
        self.release()
        return False

class GearmanConnectionManager(object):
    """Abstract base class for any Gearman-type client that needs to 
    connect/listen to multiple connections

    Mananges and polls a group of gearman connections
    Forwards all communication between a connection and a command handler
    The state of a connection is represented within the command handler

    Automatically encodes all 'data' fields as specified in protocol.py
    """
    command_handler_class = GearmanCommandHandler
    connection_class = GearmanConnection
    job_class = GearmanJob
    job_request_class = GearmanJobRequest  

    data_encoder = NoopEncoder

    def __init__(self, host_ls=None):
        self.connection_ls = []
        self.has_internal_connection = False
        self.lock = NoopLock()
        self.handler_state = {}

        host_ls = host_ls or []
        for host in host_ls:
            self.add_connection(host)

        self._poller = gearman.io.get_connection_poller()

    def shutdown(self):
        # Shutdown all our connections one by one
        for connection in self.connection_ls:
            connection.close()

        self._poller.close()

    def add_connection(self, address):
        """Add a new connection to this connection manager"""
        host, port = gearman.util.disambiguate_server_parameter(address)

        connection = self.connection_class(self, self.command_handler_class
                                           host=host, port=port)
        self.connection_ls.append(connection)

        return connection

    def establish_connection(self, connection):
        if connection.connected:
            return connection

        connection.connect(**self.handler_state)

        return connection

    def _poll_once(self, connections, timeout=None):
        # a timeout of -1 when used with epoll will block until there
        # is activity. Select does not support negative timeouts, so this
        # is translated to a timeout=None when falling back to select
        timeout = timeout or -1 
        readable, writable, exceptional = set(), set(), set()
        lookup = dict((c.fileno(), c) for c in connections)

        for fileno, events in self._poller.poll(timeout=timeout):
            connection = lookup[fileno]

            if not connection:
                continue
            if events & gearman.io.READ:
                readable.add(connection)
            if events & gearman.io.WRITE:
                writable.add(connection)
            if events & gearman.io.ERROR:
                exceptional.add(connection)

        return readable, writable, exceptional 

    def _process(self, readable, writable, exceptional):
        broken = set()

        for connection in readable:
            try:
                self.handle_read(connection)
            except ConnectionError:
                self.handle_error(connection)
                broken.add(connection)

        for connection in writable:
            try:
                self.handle_write(connection)
            except ConnectionError:
                self.handle_error(connection)
                broken.add(connection)

        for connection in exceptional:
            self.handle_error(connection)

        return readable, writable, (exceptional | broken)

    def _register(self, connections):
        for connection in connections:
            events = 0
            if connection.readable():
                events |= gearman.io.READ
            if connection.writable():
                events |= gearman.io.WRITE
            self._poller.register(connection, events)

    def poll(self, connections, before_poll, after_poll, timeout=None):
        """Continue to poll our connections until we receive a stopping 
        condition"""
        stopwatch = gearman.util.Stopwatch(timeout)
        connections = set(connections)

        workable = before_poll()
        pollable = compat.any(not c.internal and c.connected for c in connections)
        internal = compat.any(c.internal and c.connected for c in connections) \
                if self.has_internal_connection else True

        while workable and pollable and internal:
            time_remaining = stopwatch.get_time_remaining()
            if time_remaining == 0.0:
                break

            self._register(connections)

            readable, writable, exceptional = self._poll_once(
                connections, timeout=time_remaining)

            readable, writable, exceptional = self._process(
                readable, writable, exceptional)

            any_activity = compat.any([readable, writable, exceptional])

            for connection in connections:
                self._poller.unregister(connection)

            # Do not retry dead connections on the next iteration of the loop, 
            # as we closed them in handle_error
            connections -= exceptional

            workable = after_poll()
            pollable = compat.any(not c.internal for c in connections)
            internal = compat.any(c.internal for c in connections) \
                    if self.has_internal_connection else True

        return workable

    def handle_read(self, connection):
        """Handle all our pending socket data"""
        # Transfer data from socket -> buffer
        connection.read_data_from_socket()

        # Transfer command from buffer -> command queue
        connection.read_commands_from_buffer()

    def handle_write(self, connection):
        # Transfer command from command queue -> buffer
        connection.send_commands_to_buffer()

        # Transfer data from buffer -> socket
        connection.send_data_to_socket()

    def handle_error(self, connection):
        connection.on_io_error()
        connection.close()

    def on_server_error(self, error_code, error_text):
        gearman_logger.error('Received error from server: %s: %s' % (error_code,
                                                                     error_text))
        return False

