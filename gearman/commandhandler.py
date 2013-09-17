import weakref
import logging
from gearman.errors import UnknownCommandError
import gearman.protocol as protocol
import gearman.log as log

class GearmanCommandHandler(object):
    """A command handler manages the state which we should be in given a certain
    stream of commands

    GearmanCommandHandler does no I/O and only understands sending/receiving 
    commands
    """
    def __init__(self, connection=None):
        self.connection = weakref.proxy(connection)

    def set_state(self, *args, **kwargs):
        """Called by a Connection Manager after we've been instantiated and 
        we're ready to send off commands"""
        pass

    def on_io_error(self):
        pass

    def decode_data(self, data):
        """Convenience function :: handle binary string -> object unpacking"""
        return self.connection.manager.data_encoder.decode(data)

    def encode_data(self, data):
        """Convenience function :: handle object -> binary string packing"""
        return self.connection.manager.data_encoder.encode(data)

    def fetch_commands(self):
        """Called by a Connection Manager to notify us that we have pending 
        commands"""
        fetch_again = True
        while fetch_again:
            cmd_tuple = self.connection.read_command()
            if cmd_tuple is None:
                break

            cmd_type, cmd_args = cmd_tuple
            fetch_again = self.dispatch_command(cmd_type, **cmd_args)

    def send_command(self, cmd_type, **cmd_args):
        """Hand off I/O to the connection mananger"""
        self.connection.send_command(cmd_type, cmd_args)

    def dispatch_command(self, cmd_type, **cmd_args):
        """Maps any command to a recv_* callback function"""

        command = protocol.get_command_name(cmd_type)
        if command is None or not command.startswith('GEARMAN_COMMAND_'):
            errmsg = 'Could not handle command: %r - %r' % (command, cmd_args)

            log.msg(errmsg, logging.ERROR)
            raise ValueError(errmsg)

        method_name = command.lower().replace('gearman_command_',
                                           'recv_')
        method = getattr(self, method_name, None)
        if method is None:
            errmsg = 'Could not handle command: %r - %r' % (
                protocol.get_command_name(cmd_type), cmd_args)

            log.msg(errmsg, logging.ERROR)
            raise UnknownCommandError(errmsg)

        return method(**cmd_args)

    def recv_error(self, error_code, error_text):
        """When we receive an error from the server, notify the connection 
        manager that we have a gearman error"""
        return self.connection.manager.on_server_error(error_code, error_text)

    def close(self):
        pass

