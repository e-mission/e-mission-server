from __future__ import print_function
from timeit import default_timer

class Timer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.timer = default_timer
        
    def __enter__(self):
        self.start = self.timer()
        return self
        
    def __exit__(self, *args):
        end = self.timer()
        self.elapsed = end - self.start
        self.elapsed_ms = self.elapsed * 1000  # millisecs
        if self.verbose:
            print('elapsed time: %f ms' % self.elapsed_ms)
