import time
from threading import Thread
from concurrent.futures import Future


def pyqt_try_except(logger, class_name: str = 'Unknown Class', function_name: str = 'Unknown Function'):
    def outer_wrapper(fn):
        def inner_wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except BaseException as e:
                logger.exception('{} > {}(): Unknown EXCEPTION: {}', class_name, function_name, str(e))
        return inner_wrapper
    return outer_wrapper


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


def timing(fn):
    def wrapper(*args, **kwargs):
        time1 = time.time()
        ret = fn(*args, **kwargs)
        time2 = time.time()
        diff = (time2 - time1) * 1000.0
        print('{}() function took {:0.3f} ms <> {} <> {}'.format(f.__name__, diff, time1, time2))
        return ret
    return wrapper
