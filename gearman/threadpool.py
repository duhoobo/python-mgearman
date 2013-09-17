import sys
import gevent
import gevent.coros
import gevent.queue
import functools
import logging
import gearman.log as log
import gearman.errors

class GearmanThreadPool(object):
    def spawn(self, callback, manager, job):
        raise NotImplementedError

    def reserve(self):
        raise NotImplementedError

    def release(self):
        raise NotImplementedError

    def busy(self):
        raise NotImplementedError

    def terminate(self):
        raise NotImplementedError

class GearmanGeventPool(GearmanThreadPool):
    class _SafeManager(object):
        def __init__(self, manager, lock):
            self._lock = lock
            self._manager = manager

        def _safe_method(self, func):
            def wrapper(*args, **kwargs):
                with self._lock:
                    retval = func(*args, **kwargs)

                return retval
            return wrapper

        def __getattr__(self, name):
            return self._safe_method(getattr(self._manager, name))

    class Lock(gevent.coros.Semaphore):
        pass

    def __init__(self, concurrency):
        self._concurrency = concurrency

        self._callback_lock = gevent.coros.Semaphore()

        self._reaper = gevent.spawn(self._reap)
        self._reapq = gevent.queue.Queue(maxsize=self._concurrency)
        self._workers = {}

        self._reserved = 0
        self._running = 0
        self._terminated = False

    def _entry(self, worker_id, callback, manager, job):
        try:
            try:
                result = callback(manager, job)
                manager.send_job_complete(job, result)
            except gearman.errors.ConnectionError:
                raise
            except:
                manager.send_job_exception(job, sys.exc_info())
        except gearman.errors.ConnectionError as exc:
            log.msg("%s" % exc, logging.NOTICE)
        finally:
            self._reapq.put(worker_id)

    def _reap(self):
        while not self._terminated:
            try:
                worker_id = self._reapq.get(timeout=0.1)
                meta = self._workers[worker_id]
                del self._workers[worker_id]
                self._running -= 1

                meta["greenlet"].join(timeout=None)
            except gevent.queue.Empty:
                pass

        for worker_id, item in self.workers:
            item["greenlet"].kill(block=True)
            item["greenlet"].join(timeout=None)

    def spawn(self, callback, manager, job):
        # It wouldn't block
        manager = self._SafeManager(manager, self._callback_lock)
        worker_id = id(job)

        greenlet = gevent.spawn(self._entry, worker_id, callback, manager, job)

        self._reserved -= 1
        self._running += 1
        self._workers[id(job)] = {"greenlet": greenlet, "manager": manager, 
                                 "job": job}
                               
    def reserve(self):
        if self._concurrency - self._running - self._reserved <= 0:
            return False

        self._reserved += 1
        return True

    def release(self):
        self._reserved -= 1

    def busy(self):
        return (self._running > 0)

    def terminate(self):
        self._terminated = True
        self._reaper.join(timeout=None)

    def __repr__(self):
        return ("<GearmanGeventPool running:%d reserved:%d idle:%d>" %
                (self._running, self._reserved, 
                 self._concurrency - self._running - self._reserved))

