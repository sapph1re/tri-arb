import time
import asyncio
from threading import Thread
from concurrent.futures import Future
from pydispatch import dispatcher, robustapply


def catch_exceptions(logger, class_name: str = 'Unknown Class', function_name: str = 'Unknown Function'):
    def outer_wrapper(fn):
        def inner_wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except BaseException as e:
                logger.exception(f'{class_name} > {function_name}(): Unknown EXCEPTION: {e}')
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


def robust_make_func(callback, *args, **kwargs):
    """
    Returns a function callable without arguments that executes callback() with provided arguments.
    Filters out arguments that callback cannot handle.
    :param callback: function to execute with provided arguments
    :param args: arbitrary arguments passed to callback()
    :param kwargs: arbitrary keyword arguments passed to callback()
    :return: function, just call it with no arguments
    """
    def func():
        return robustapply.robustApply(callback, *args, **kwargs)
    return func


def dispatcher_connect_threadsafe(handler, signal, sender) -> callable:
    """
    Do dispatcher.connect() in a threadsafe manner.
    Schedules handler() execution as a handler for the arrived signal, but in the handler's original event loop.
    :param handler: signal handler
    :param signal: signal name
    :param sender: signal sender
    :return: disconnect function, simply call it to disconnect the handler from the signal.
    """
    loop = asyncio.get_event_loop()

    def dispatcher_receive(*args, **kwargs):
        loop.call_soon_threadsafe(robust_make_func(handler, *args, **kwargs))
    dispatcher.connect(dispatcher_receive, signal=signal, sender=sender, weak=False)

    def disconnect():
        dispatcher.disconnect(dispatcher_receive, signal=signal, sender=sender, weak=False)
    return disconnect
