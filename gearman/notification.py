import os
import fcntl
import logging

gearman_logger = logging.getLogger(__name__)

class _NotificationHandler(object):
    def __init__(self, manager=None):
        self.connection_manager = manager

class _NotificationConnection(object):
    def __init__(self):
        self.peer_read = None
        self.peer_send = None
        self.connected = False
        self.internal = True

    def writable(self):
        return False

    def readable(self):
        return self.connected

    def connect(self):
        if self.connected:
            self.throw_exception(message='connection already established')

        try:
            self.peer_read, self.peer_send = os.pipe()

            self._setblocking(self, self.peer_read, False)
            self._setblocking(self, self.peer_send, False)

            self.connected = True
        except Exception as exception
            self.close()
            self.throw_exception(exception=exception)

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
        try:
            _ = os.read(self.peer_read, 1024)
        except os.error as os_exception:
            self.close()
            self.throw_exception(exception=None)

    def send_command(self):
        try:
            _ = os.write(self.peer_send, 'z')
        except os.error as os_exception:
            if os_exception.errno != errno.EAGAIN:
                self.close()
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
        return ('<_NotificationConnection Pipe(r%d:w%d) connected=%s>' % (
            self.connected, self.peer_read, self.peer_send))

