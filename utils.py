""" This module provides helpful functions. """

import csv
import os
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

def calculate_objects(data_obj):
    """ Calculate number of objects in data object. """
    count = 0
    for key in data_obj:
        # iterate over env, farm, farm_role and server objects
        count += len(data_obj[key])
    return count

def get_archives(folder):
    """ Get list of archives in folder. """
    archives = []
    try:
        archives = os.listdir(folder)
    except OSError as err:
        # No such directory
        if err.errno != 2:
            raise err
    return [os.path.join(folder, x) for x in archives]

@threadsafe_generator
def generate_chunks(csv_path, chunksize=100):
    """ Generates chunks from .csv file.
        Take a CSV `reader` and yield `chunksize` sized slices.
    """
    chunk = []
    with open(csv_path, "rb") as csvfile:
        reader = csv.reader(csvfile)
        for i, line in enumerate(reader):
            if i % chunksize == 0 and i > 0:
                yield chunk
                del chunk[:]
            chunk.append(line)
        yield chunk
