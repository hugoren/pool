import sys
import time
import greenlet

from functools import wraps
from collections import deque
from tornado.ioloop import IOLoop
from tornado.concurrent import Future
from pymysql.connections import Connection


class TimeoutException(Exception):
    pass


class Timeout(object):
    def __init__(self, deadline, ex=TimeoutException):
        self._greenlet = greenlet.getcurrent()
        self._ex = ex
        self._callback = None
        self._deadline = deadline
        self._delta = time.time() + deadline
        self._ioloop = IOLoop.current()

    def start(self, callback=None):
        errmsg = "{} timeout, deadline is {} seconds".format(str(self._greenlet),
                                                         self._deadline)
        if callback:
            self._callback = self._ioloop.add_timeout(self._delta, callback,self._ex(errmsg))
        else:
            self._callback = self._ioloop.add_timeout(
                self._delta, self._greenlet.throw, self._ex(errmsg))

    def cancel(self):
        assert self._callback, "Timeout not started"
        self._ioloop.remove_timeout(self._callback)
        self._greenlet = None


class Waiter:
    def __init__(self):
        self._greenlet = greenlet.getcurrent()
        self._main = self._greenlet.parent

    @property
    def greenlet(self):
        return self._greenle

    def switch(self, value):
        self._greenlet.switch(value)

    def throw(self, *exc_info):
        self._greenlet.throw(*exc_info)

    def get(self):
        return self._main.switch()

    def clear(self):
        pass


class Event:
    def __init__(self):
        self._waiter = []
        self._ioloop = IOLoop.current()

    def set(self):
        self._ioloop.add_callback(self._notify)

    def wait(self, timeout=None):
        current_greenlet = greenlet.getcurrent()
        self._waiter.append(current_greenlet.switch)
        waiter = Waiter()

        if timeout:
            timeout_checker = Timeout(timeout)
            timeout_checker.start(current_greenlet.throw)
            waiter.get()
            timeout_checker.cancel()
        else:
            waiter.get()

    def _notify(self):
        for waiter in self._waiter:
            waiter(self)


class GreenTask(greenlet.greenlet):
    def __init__(self, run, *args, **kwargs):
        super(GreenTask, self).__init__()
        self._run = run
        self._args = args
        self._kwargs = kwargs
        self._future = Future()
        self._result = None
        self._exc_info = ()

    @property
    def args(self):
        return self._args

    @property
    def kwargs(self):
        return self._kwargs

    def run(self):
        try:
            timeout = self.kwargs.pop("timeout", 0)
            if timeout:
                timer = Timeout(timeout)
                timer.start()
            self._result = self._run(*self.args, **self.kwargs)
            self._future.set_result(self._result)
        except:
            self._exc_info = sys.exc_info()
            self._future.set_exc_info(self._exc_info)
        finally:
            if timeout:
                timer.cancel()

    def start(self):
        self.switch()

    def __str__(self):
        func_name = "{} of {} ".format(self._run.__name__, self._run.__module__)
        return "<greenlet {} at {}>".format(func_name, hex(id(self)))

    def __repr__(self):
        return self.__str__()

    def wait(self):
        return self._future

    @classmethod
    def spawn(cls_green, *args, **kwargs):
        task = cls_green(*args, **kwargs)
        task.start()
        return task


def spawn(callable_obj, *args, **kwargs):
    return GreenTask.spawn(callable_obj, *args, **kwargs).wait()


def green(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        return GreenTask.spawn(func, *args, **kwargs).wait()
    return wrapper


class Pool:
    def __init__(self, max_size=32, wait_timeout=8, params={}):
        self._maxsize = max_size
        self._conn_params = params
        self._pool = deque(maxlen=self._maxsize)
        self._wait = deque()
        self._wait_timeout = wait_timeout
        self._count = 0
        self._started = False
        self._ioloop = IOLoop.current()
        self._evnet = Event()
        self._ioloop.add_future(spawn(self.start), lambda future: future)

    def create_raw_conn(self):
        pass

    def init_pool(self):
        self._count += 1
        conn = self.create_raw_conn()
        self._pool.append(conn)

    @property
    def size(self):
        return len(self._pool)

    def get_conn(self):
        while 1:
            if self._pool:
                return self._pool.popleft()
            elif self._count < self._maxsize:
                self.init_pool()
            else:
                self.wait_conn()

    def wait_conn(self):
        timer = None
        child_gr = greenlet.getcurrent()
        main = child_gr.parent
        try:
            if self._wait_timeout:
                timer = Timeout(self._wait_timeout)
                timer.start()
            self._wait.append(child_gr.parent)
            main.switch()
        except TimeoutException as e:
            raise Exception("timeout wait connections, connections size is {}".format(self.size))
        finally:
            if timer:
                timer.cancel()

    def release(self, conn):
        self._pool.append(conn)
        if self._wait:
            callback = self._wait.popleft()
            self._ioloop.add_callback(callback)

    def quit(self):
        self._started = Future
        self._evnet.set()

    def _close_all(self):
        for conn in tuple(self._pool):
            conn.close()
        self._pool = None

    def start(self):
        self._started = True
        self._evnet.wait()
        self._close_all()


class ConnectionPool(Pool):
    def __init__(self, max_size=32, keep_alive=7200, mysql_params={}):
        super(ConnectionPool, self).__init__(max_size=max_size, params=mysql_params)
        self._keep_alive = keep_alive

    def create_raw_conn(self):
        conn = Connection(**self._conn_params)
        if self._keep_alive:
            self._ioloop.add_timeout(time.time() + self._keep_alive, self._ping, conn)
        return conn

    @green
    def _ping(self):
        for conn in self._pool:
            conn.ping()
            self.release(conn)
        self._ioloop.add_timeout(time.time() + self._keep_alive, self._ping, conn)


if __name__ == "__main__":
   p = ConnectionPool(mysql_params={"user": "root", "password": "", "database": "", "port": 3306})
   cursor = p.create_raw_conn().cursor()
   cursor.execute("select * from django_session")
   result = cursor.fetchall()
   print(result)
