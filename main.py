"""This module implements functionality to parse billing detailed data."""

import ConfigParser
import csv
import logging
import logging.config
import os
import threading
import zipfile

from Queue import Queue

from utils import threadsafe_generator

SCALR_TAG = 'user:scalr-meta'
COST_TAG = 'Cost'

logging.config.fileConfig('log.conf')


class BillingParser(object):
    """ Parse billing data from zip archives. """

    def __init__(self):
        """ Initialize logger and configurations. """
        self.__log = logging.getLogger('billing')
        self.__config = ConfigParser.ConfigParser()
        self.__config.read('configs.conf')
        self.__num_threads = int(self.__config.get('app', 'num_threads'))


    def get_data_folder(self):
        """ Get folder with archives. """
        current_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(current_dir, self.__config.get('app', 'billing_data_folder'))

    def prepare_data(self, data_obj, row, cost):
        """ Format data to object with calculated sum into the following format:
            {'object1_name': {'object1_id': sum, 'object2_id': sum, ...}
            object_name - values from list ['env', 'farm', 'farm_role', 'server']
        """
        object_list = ['env', 'farm', 'farm_role', 'server']
        if len(row) != len(object_list):
            self.__log.info('Wrong data parsed from csv file: %s', row)
            return

        # calculate cost for each group of objects
        for ind, val in enumerate(object_list):
            if data_obj[val].has_key(row[ind]) and row[ind]:
                data_obj[val][row[ind]] += cost
            elif row[ind]:
                data_obj[val][row[ind]] = cost
        return data_obj

    # pylint: disable=no-self-use
    def get_tag_indexes(self, headers):
        """ Get indexes of scalr and cost tags. """
        scalr_ind = 0
        cost_ind = 0
        if SCALR_TAG in headers:
            for ind, value in enumerate(headers):
                if value == SCALR_TAG:
                    scalr_ind = ind
                elif value == COST_TAG:
                    cost_ind = ind
        return (scalr_ind, cost_ind)


    def parse_billing(self, archives):
        """ Parse billing reports in threads."""

        def __extract_zip(inputq, data, scalr_ind, cost_ind):
            """ Extract zip archive to list of files. """
            while True:
                try:
                    lines = inputq.get()
                    print lines
                    line = lines.pop()
                except StopIteration:
                    break
                # split scalr tag data into env, farm, farm_role and server ids
                scalr_data = line[scalr_ind].split(':')

                cost = line[cost_ind]
                if len(scalr_data) == 5:
                    # avoid v1:
                    self.prepare_data(data, scalr_data[1:], float(cost))
                    #print calculate_objects(data)
                inputq.task_done()


        outputq = {'env': {}, 'farm': {}, 'farm_role': {}, 'server': {}}
        lookup_threads = []
        for archive in archives:
            self.__log.info('Working with archive: %s', archive)
            with zipfile.ZipFile(archive, "r") as zipped:
                for name in zipped.namelist():
                    outpath = self.__config.get('app', 'unzip_folder')

                    # unzip package
                    zipped.extract(name, outpath)

                    # read .csv file
                    csv_path = os.path.join(outpath, name)
                    self.__log.info('Processing: %s', csv_path)
                    with open(csv_path, "rb") as csvfile:
                        datareader = csv.reader(csvfile)
                        headers = datareader.next()
                        scalr_ind, cost_ind = self.get_tag_indexes(headers)
                        chunks = generate_chunks(csv_path)

                    q = Queue()
                    for line in chunks:
                        q.put(line)
                    print q.qsize()
                    for thread_id in range(1, self.__num_threads + 1):
                        thread_name = 'parse_billing_%s' % thread_id
                        thr = threading.Thread(target=__extract_zip,
                                               name=thread_name,
                                               args=(q, outputq, scalr_ind, cost_ind))
                        thr.daemon = True
                        thr.start()
                        lookup_threads.append(thr)
                    q.join()
                    #for thr in lookup_threads:
                    #    thr.join()

        return outputq


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


if __name__ == '__main__':
    parser = BillingParser()
    data_folder = parser.get_data_folder()
    list_of_archives = get_archives(data_folder)
    output = parser.parse_billing(list_of_archives)
    print calculate_objects(output)
