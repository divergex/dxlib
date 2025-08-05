import threading
import time
import queue

class PeriodicTimer:
    def __init__(self, interval, function, timeout=None, max_queue_size=0):
        self.interval = interval
        self.function = function
        self.timeout = timeout  # max allowed time per call; if None, no timeout check
        self._stop_event = threading.Event()
        self._thread = None
        self._queue = queue.Queue(maxsize=max_queue_size)

    def _run(self):
        next_call = time.monotonic() + self.interval
        while not self._stop_event.is_set():
            start_time = time.monotonic()
            result = self.function()
            duration = time.monotonic() - start_time

            if self.timeout is not None and duration > self.timeout:
                break
            self._queue.put_nowait(result)
            delay = max(0, next_call - time.monotonic())
            if self._stop_event.wait(delay):
                break
            next_call += self.interval

        self._stop_event.set()
        self._queue.put(None)

    def start(self):
        if not self._thread or not self._thread.is_alive():
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join()
            self._thread = None

    def __iter__(self):
        self.start()
        return self

    def __next__(self):
        result = self._queue.get()
        if result is None:
            raise StopIteration
        return result


if __name__ == "__main__":
    def my_func():
        return time.time()

    try:
        timer = PeriodicTimer(1.0, my_func, timeout=.1)

        for val in timer:
            print(val)
    except KeyboardInterrupt:
        pass
