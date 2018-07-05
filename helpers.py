import time
from threading import Thread
from concurrent.futures import Future


def safe_cast(val, to_type, default=None):
    try:
        return to_type(val)
    except (ValueError, TypeError):
        return default


def _call_with_future(fn, future, args, kwargs):
    try:
        result = fn(*args, **kwargs)
        future.set_result(result)
    except Exception as e:
        future.set_exception(e)


def threaded(fn):
    def wrapper(*args, **kwargs):
        future = Future()
        Thread(target=_call_with_future, args=(fn, future, args, kwargs)).start()
        return future
    return wrapper


def timing(f):
    def wrap(*args, **kwargs):
        time1 = time.time()
        ret = f(*args, **kwargs)
        time2 = time.time()
        diff = (time2 - time1) * 1000.0
        print('{}() function took {:0.3f} ms <> {} <> {}'.format(f.__name__, diff, time1, time2))
        return ret

    return wrap
