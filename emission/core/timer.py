from __future__ import print_function
from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from builtins import object
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
