import logging

import gearman.io
import gearman.util
from gearman.connection import GearmanConnection
from gearman.constants import _DEBUG_MODE_
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

class GearmanConnectionManager(object):
    """Abstract base class for any Gearman-type client that needs to 
    connect/listen to multiple connections

    Mananges and polls a group of gearman connections
    Forwards all communication between a connection and a command handler
    The state of a connection is represented within the command handler

    Automatically encodes all 'data' fields as specified in protocol.py
    """
    command_handler_class = None
    connection_class = GearmanConnection

    job_class = GearmanJob
    job_request_class = GearmanJobRequest

    data_encoder = NoopEncoder

    def __init__(self, host_list=None):
        assert self.command_handler_class is not None, \
                'GearmanClientBase did not receive a command handler class'

        self.connection_list = []

        host_list = host_list or []
        for hostport_tuple in host_list:
            self.add_connection(hostport_tuple)

        self.handler_to_connection_map = {}
        self.connection_to_handler_map = {}

        self.handler_initial_state = {}

        self.has_internal_connection = False

    def shutdown(self):
        # Shutdown all our connections one by one
        for gearman_connection in self.connection_list:
            gearman_connection.close()

    ###################################
    # Connection management functions #
    ###################################

    def add_connection(self, hostport_tuple):
        """Add a new connection to this connection manager"""
        gearman_host, gearman_port = gearman.util.disambiguate_server_parameter(hostport_tuple)

        client_connection = self.connection_class(host=gearman_host, port=gearman_port)
        self.connection_list.append(client_connection)

        return client_connection

    def establish_connection(self, connection):
        """Attempt to connect... if not previously connected, create a new 
        CommandHandler to manage this connection's state
        !NOTE! This function can throw a ConnectionError which deriving 
        ConnectionManagers should catch
        """
        assert connection in self.connection_list, \
                "Unknown connection - %r" % connection

        if connection.connected:
            return connection

        # !NOTE! May throw a ConnectionError
        connection.connect()

        # Initiate a new command handler every time we start a new connection
        current_handler = self.command_handler_class(connection_manager=self)

        # Handler to connection map for CommandHandler -> Connection interactions
        # Connection to handler map for Connection -> CommandHandler interactions
        self.handler_to_connection_map[current_handler] = connection
        self.connection_to_handler_map[connection] = current_handler

        current_handler.initial_state(**self.handler_initial_state)
        return connection

    def poll_connections_once(self, poller, connection_map, timeout=None):
        # a timeout of -1 when used with epoll will block until there
        # is activity. Select does not support negative timeouts, so this
        # is translated to a timeout=None when falling back to select
        timeout = timeout or -1 

        readable = set()
        writable = set()
        errors = set()
        for fileno, events in poller.poll(timeout=timeout):
            connection = connection_map.get(fileno)
            if not connection:
                continue
            if events & gearman.io.READ:
                readable.add(connection)
            if events & gearman.io.WRITE:
                writable.add(connection)
            if events & gearman.io.ERROR:
                errors.add(connection)

        return readable, writable, errors

    def handle_connection_activity(self, rd_connections, wr_connections, ex_connections):
        """Process all connection activity... executes all handle_* callbacks"""
        dead_connections = set()
        for connection in rd_connections:
            try:
                self.handle_read(connection)
            except ConnectionError:
                dead_connections.add(connection)

        for connection in wr_connections:
            try:
                self.handle_write(connection)
            except ConnectionError:
                dead_connections.add(connection)

        for connection in ex_connections:
            self.handle_error(connection)

        for connection in dead_connections:
            self.handle_error(connection)

        failed_connections = ex_connections | dead_connections
        return rd_connections, wr_connections, failed_connections

    def _register_connections_with_poller(self, connections, poller):
        for connection in connections:
            events = 0
            if connection.readable():
                events |= gearman.io.READ
            if connection.writable():
                events |= gearman.io.WRITE
            poller.register(connection, events)

    def poll_connections_until_stopped(self, connections, callback_fxn, 
                                       timeout=None):
        """Continue to poll our connections until we receive a stopping 
        condition"""
        stopwatch = gearman.util.Stopwatch(timeout)
        connections = set(connections)

        any_activity = False
        callback_ok = callback_fxn(any_activity)
        connection_ok = compat.any(c.connected and not c.internal 
                                   for c in connections)

        internal_connection_ok = compat.any(c.connected and c.internal
                                            for c in connections) \
                if self.has_internal_connection else True

        poller = gearman.io.get_connection_poller()

        while connection_ok and callback_ok and internal_connection_ok:

            time_remaining = stopwatch.get_time_remaining()
            if time_remaining == 0.0:
                break

            self._register_connections_with_poller(connections, poller)

            # Do a single robust select and handle all connection activity
            readable, writable, death = self.poll_connections_once(
                poller, dict([(c.fileno(), c) 
                              for c in connections if c.connected]), 
                timeout=time_remaining)

            # Handle reads and writes and close all of the dead connections
            readable, writable, death = self.handle_connection_activity(
                readable, writable, death)

            any_activity = compat.any([readable, writable, death])

            for connection in connections:
                poller.unregister(connection)

            # Do not retry dead connections on the next iteration of the loop, 
            # as we closed them in handle_error
            connections -= death 

            callback_ok = callback_fxn(any_activity)
            connection_ok = compat.any(c.connected and not c.internal 
                                       for c in connections)

            internal_connection_ok = compat.any(c.connected and c.internal
                                                for c in connections) \
                    if self.has_internal_connection else True

        poller.close()

        # We should raise here if we have no alive connections (don't go into a 
        # select polling loop with no connections)
        if not connection_ok:
            raise ServerUnavailable('Found no valid connections in list: %r' % self.connection_list)

        return bool(connection_ok and callback_ok)

    def handle_read(self, connection):
        """Handle all our pending socket data"""
        current_handler = self.connection_to_handler_map[connection]

        # Transfer data from socket -> buffer
        connection.read_data_from_socket()

        # Transfer command from buffer -> command queue
        connection.read_commands_from_buffer()

        # Notify the handler that we have commands to fetch
        current_handler.fetch_commands()

    def handle_write(self, connection):
        # Transfer command from command queue -> buffer
        connection.send_commands_to_buffer()

        # Transfer data from buffer -> socket
        connection.send_data_to_socket()

    def handle_error(self, connection):
        dead_handler = self.connection_to_handler_map.pop(connection, None)
        if dead_handler:
            dead_handler.on_io_error()

        self.handler_to_connection_map.pop(dead_handler, None)
        connection.close()

    ##################################
    # Callbacks for Command Handlers #
    ##################################

    def read_command(self, command_handler):
        """CommandHandlers call this function to fetch pending commands

        NOTE: CommandHandlers have NO knowledge as to which connection they're 
                representing
              ConnectionManagers must forward inbound commands to CommandHandlers
        """
        gearman_connection = self.handler_to_connection_map[command_handler]
        cmd_tuple = gearman_connection.read_command()
        if cmd_tuple is None:
            return cmd_tuple

        cmd_type, cmd_args = cmd_tuple
        return cmd_type, cmd_args

    def send_command(self, command_handler, cmd_type, cmd_args):
        """CommandHandlers call this function to send pending commands

        NOTE: CommandHandlers have NO knowledge as to which connection they're representing
              ConnectionManagers must forward outbound commands to Connections
        """
        gearman_connection = self.handler_to_connection_map[command_handler]
        gearman_connection.send_command(cmd_type, cmd_args)

    def on_gearman_error(self, error_code, error_text):
        gearman_logger.error('Received error from server: %s: %s' % (error_code, error_text))
        return False
