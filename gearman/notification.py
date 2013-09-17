import os
import fcntl
import weakref
import logging

from gearman.errors import ConnectionError

class _NotificationHandler(object):
    def __init__(self, manager=None):
        self.manager = weakref.proxy(manager)

    def set_state(self, **kwargs):
        pass

class _NotificationConnection(object):
    def __init__(self, manager, handler_class):
        self.manager = weakref.proxy(manager)
        self.handler = handler_class(self)

        self.peer_read = None
        self.peer_send = None
        self.connected = False
        self.internal = True

    def writable(self):
        return False

    def readable(self):
        return self.connected

    def fileno(self):
        if self.peer_read is None:
            self.throw_exception("read peer not open")

        return self.peer_read

    def connect(self, **kwargs):
        if self.connected:
            self.throw_exception(message='connection already established')

        try:
            self.peer_read, self.peer_send = os.pipe()

            self._setblocking(self.peer_read, False)
            self._setblocking(self.peer_send, False)

            self.connected = True
        except Exception as exception:
            self.close()
            self.throw_exception(exception=exception)

        self.handler.set_state(**kwargs)

    def _setblocking(self, fd, blocking=True):
        try:
            flags = fcntl.fcntl(fd, fcntl.F_GETFL)
            if not blocking:
                fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
            else:
                fcntl.fcntl(fd, fcntl.F_SETFL, flags & ~os.O_NONBLOCK)
        except IOError as io_exeception:
            self.throw_exception(exception=io_exception)

    def read_command(self):
        cmd = None
        try:
            cmd = os.read(self.peer_read, 1)
            logging.debug("Notification pipe got: %s" % cmd)
        except os.error as os_exception:
            self.throw_exception(exception=None)

        return cmd

    def send_command(self, command='w', **kwargs):
        try:
            _ = os.write(self.peer_send, command)
        except os.error as os_exception:
            if os_exception.errno != errno.EAGAIN:
                self.throw_exception(exception=None)

    def close(self):
        if self.peer_read is not None:
            os.close(self.peer_read)
            self.peer_read = None

        if self.peer_send is not None:
            os.close(self.peer_send)
            self.peer_send = None

        self.connected = False

    def throw_exception(self, message=None, exception=None):
        # Mark us as disconnected but do NOT call self._reset_connection()
        # Allows catchers of ConnectionError a chance to inspect the last state of this connection
        self.connected = False

        if exception:
            message = repr(exception)

        rewritten_message = "<os:%s> %s" % (os.name, message) 

        raise ConnectionError(rewritten_message)

    def __repr__(self):
        return ('<_NotificationConnection (r%s:w%s) connected=%s internal=%s>' %
                (self.peer_read, self.peer_send, self.connected, self.internal))

