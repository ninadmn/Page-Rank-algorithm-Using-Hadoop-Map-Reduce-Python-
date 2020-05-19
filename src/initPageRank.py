from gzip import GzipFile
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol, JSONProtocol, TextValueProtocol
import requests
from warcio.archiveiterator import ArchiveIterator
from urllib.parse import urlparse
import ujson as json
import itertools
from optparse import OptionParser

class DistributeInitialPageRankJob(MRJob):
    INPUT_PROTOCOL = JSONProtocol

    def configure_args(self):
        super(DistributeInitialPageRankJob, self).configure_args()
        self.add_passthru_arg('--graphsize', type=int, default=0 )

    def mapper(self, website, node):
        node['state'] = 1.0 / self.options.graphsize
        yield website, node


if __name__ == '__main__':
    DistributeInitialPageRankJob().run()
