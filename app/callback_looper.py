import time


class CallbackLooper:
    def __init__(self, callback, so_long_as, interval=1):
        self.callback = callback
        self.interval = interval
        self.so_long_as = so_long_as

    def start_loop(self):
        while self.so_long_as():
            self.callback()
            time.sleep(self.interval)
