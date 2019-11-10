import asyncio
import time
from logger import get_logger
logger = get_logger(__name__)


class BaseAPI:

    class Error(BaseException):
        def __init__(self, message: str = ''):
            self.message = message

    class ErrorNoRetry(Error):
        pass

    class OrderNotFound(ErrorNoRetry):
        pass

    class Stopping(BaseException):
        pass

    def __init__(self):
        self._stopping = False
        self._last_request_ts = 0
        self._priority_locks = {
            0: asyncio.Lock(),
            1: asyncio.Lock(),
            2: asyncio.Lock()
        }
        self._request_interval = 1      # seconds

    async def _safe_call(self, urgency: int, func, *args, **kwargs):
        # prioritizes, throttles, retries on error
        tries = 10
        try:
            while not self._stopping:
                try:
                    return await self._prioritize(urgency, self._throttle, func, *args, **kwargs)
                except BaseAPI.ErrorNoRetry:
                    raise
                except BaseAPI.Stopping:
                    raise
                except BaseException as e:
                    logger.warning(f'API call failed: {args} {kwargs}. Reason: {e}')
                    tries -= 1
                    if tries > 0:
                        await asyncio.sleep(0.5)
                        continue
                    else:
                        raise
        except BaseAPI.ErrorNoRetry as e:
            raise BaseAPI.Error(e.message)
        except BaseAPI.Stopping:
            raise BaseAPI.Error('API is stopping')
        except (asyncio.TimeoutError, BaseAPI.Error):
            raise BaseAPI.Error('Failed 10 times')

    async def _prioritize(self, urgency: int, func, *args, **kwargs):
        # give way to more urgent ones
        for level in sorted(self._priority_locks.keys()):
            if level >= urgency:
                await self._priority_locks[level].acquire()
        # do the stuff
        try:
            if self._stopping:
                # unless we are already stopping
                raise BaseAPI.Stopping
            result = await func(*args, **kwargs)
        finally:
            # now let other ones of same or lower urgency go through
            for level in sorted(self._priority_locks.keys()):
                if level >= urgency:
                    self._priority_locks[level].release()
        return result

    async def _throttle(self, func, *args, **kwargs):
        passed = time.time() - self._last_request_ts
        if passed < self._request_interval:
            await asyncio.sleep(self._request_interval - passed)
        r = await func(*args, **kwargs)
        self._last_request_ts = time.time()
        return r

    async def stop(self):
        self._stopping = True
        # all locks acquired means all requests are completed
        for level, lock in self._priority_locks.items():
            await lock.acquire()
