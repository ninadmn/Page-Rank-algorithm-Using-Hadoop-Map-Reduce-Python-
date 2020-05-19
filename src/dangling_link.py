from gzip import GzipFile
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol, JSONProtocol, TextValueProtocol
import requests
from warcio.archiveiterator import ArchiveIterator
from urllib.parse import urlparse
import ujson as json
import itertools

class DanglingNodeJob(MRJob):
    INPUT_PROTOCOL = JSONProtocol
    OUTPUT_PROTOCOL = TextValueProtocol

    def mapper(self, website, node):
        if len(node['outgoing']) == 0:
            yield '_', str(node['state'])
        else:
            yield '_', str(0)

    def reducer(self, _, states):
        yield '_', str(sum(list(map(float, states))))

if __name__ == '__main__':
    DanglingNodeJob().run()
