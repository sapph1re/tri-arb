import time
import asyncio
import threading
from pydispatch import dispatcher, robustapply


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


def run_async_repeatedly(func, interval, loop, thread_name=None, *args, **kwargs):
    """
    In a new thread executes an async func() every <interval> seconds
    if func() execution takes more than <interval> seconds it will repeat right after the previous execution completes
    :param func: function to execute repeatedly
    :param loop: event loop in which the func will be run
    :param interval: number of seconds between executions
    :param thread_name: name of the thread to be created (useful for logging)
    :param args: arbitrary arguments passed to func()
    :param kwargs: arbitrary keyword arguments passed to func()
    :return: threading.Event, when you .set() it, execution stops
    """
    def _run(stop_event):
        while not stop_event.is_set():
            last_time = time.time()
            asyncio.run_coroutine_threadsafe(func(*args, **kwargs), loop).result()
            time_passed = time.time() - last_time
            if time_passed < interval:
                time.sleep(interval - time_passed)

    stop = threading.Event()
    thread = threading.Thread(target=_run, args=(stop,), name=thread_name)
    thread.setDaemon(True)
    thread.start()
    return stop
