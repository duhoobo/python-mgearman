import sys
import time
import logging
from datetime import datetime


context = {"is_error": 0}

class GearmanLogPublisher(object):
    def __init__(self):
        self.observers = []

    def add_observer(self, observer):
        assert callable(observer)
        self.observers.append(observer)

    def del_observer(self, observer):
        self.observers.remove(observer)

    def msg(self, *args, **kwargs):
        events = context
        events.update(kwargs)
        events["message"] = args
        events["time"] = time.time()

        for i in range(len(self.observers) - 1, -1, -1):
            try:
                self.observers[i](events)
            except KeyboardInterrupt:
                # Don't swallow keyboard interrupt
                raise
            except Exception as exc:
                observer = self.observers[i]
                self.observers[i] = lambda events: None
                try:
                    self._err("Log observer %r failed." % (obsrever,))
                except:
                    # sometimes err() will throw an exception, e.g RuntimeError
                    # due to blowing the stack; if that happens, there's not
                    # much we can do
                    pass
                self.observers[i] = observer

    def _err(self, why):
        self.msg(why=why, is_error=True)

try:
    log_publisher
except NameError:
    log_publisher = GearmanLogPublisher()
    add_observer = log_publisher.add_observer
    del_observer = log_publisher.del_observer
    msg = log_publisher.msg


class GearmanLogObserver(object):
    timefmt = None

    def emit(self, events):
        raise NotImplementedError
    def start(self):
        add_observer(self.emit)
    def stop(self):
        del_observer(self.emit)

    def format_events(self, events):
        if "message" in events:
            return ' '.join(events["message"])
        else:
            if events["is_error"]:
                return events["why"]
        return None

    def time_prefix(self, when):
        if self.timefmt is not None:
            return "%s " % datetime.fromtimestamp(when).strftime(self.timefmt)
        else:
            return ""

    def event_loglevel(self, events):
        if "loglevel" in events:
            level = events["loglevel"]
        elif events["is_error"]:
            level = logging.ERROR
        else:
            level = logging.INFO

        return level


class GearmanFileLogObserver(GearmanLogObserver):
    timefmt = "%H:%M:%S"
    def __init__(self, f, loglevel=logging.DEBUG):
        self.write = f.write
        self.flush = f.flush
        self.loglevel = loglevel

    def emit(self, events):
        level = self.event_loglevel(events)
        message = self.format_events(events);

        if level >= self.loglevel and message:
            self.write("%s%s\n" % (self.time_prefix(events["time"]),
                                   message))
            self.flush()


class GearmanLoggingObserver(GearmanLogObserver):
    def emit(self, events):
        level = self.event_loglevel(events)
        message = self.format_events(events);
        if message is None:
            return
        logging.log(level, "%s%s" % (self.time_prefix(events["time"]), 
                                     message))
                                   

def logging_with_observer(observer): 
    if default_observer:
        default_observer.stop()
        default_observer = None

    add_observer(observer)

try:
    default_observer
except NameError:
    default_observer = GearmanFileLogObserver(sys.stderr)
    default_observer.start()

