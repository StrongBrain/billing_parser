""" This module consists of thread safe decorator for generators to avoid:
    ValueError: generator already executing when threads are switching.
"""

import threading

class ThreadSafe(object):
    """Takes an iterator/generator and makes it thread-safe by
    serializing call to the `next` method of given iterator/generator.
    """
    def __init__(self, iterable):
        self.iterable = iterable
        self.lock = threading.Lock()

    def __iter__(self):
        return self

    def next(self):
        with self.lock:
            return self.iterable.next()

def threadsafe_generator(func):
    """A decorator that takes a generator function and makes it thread-safe.
    """
    def wrapper(*a, **kw):
        """ Make function thread safe. """
        return ThreadSafe(func(*a, **kw))
    return wrapper
