from gzip import GzipFile
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol, JSONProtocol, TextValueProtocol
import requests
from warcio.archiveiterator import ArchiveIterator
from urllib.parse import urlparse
import ujson as json
import itertools

class PageRankJob(MRJob):
    INPUT_PROTOCOL = JSONProtocol


    def configure_args(self):
        super(PageRankJob, self).configure_args()
        self.add_passthru_arg('--graphsize', dest='size_of_web', default=0 )
        self.add_passthru_arg('--dangling-node-pr', dest='dangling_node_pr', default=0.0)
        self.add_passthru_arg('--damping-factor', dest='damping_factor', default=0.85)

    def mapper(self, website, node):
        yield website, ('node', node)

        for outgoing in node['outgoing']:
            msg = node['state'] / len(node['outgoing'])
            yield outgoing, ('msg', msg)

    def reducer(self, website, data):
        node = None
        msgs = []

        for msg_type, msg_val in data:
            if msg_type == 'node':
                node = msg_val
            elif msg_type == 'msg':
                msgs.append(msg_val)

        if node != None:
            node['state'] = self.options.damping_factor * sum(msgs) \
                    + self.options.damping_factor * self.options.dangling_node_pr / float(self.options.size_of_web) \
                    + (1 - self.options.damping_factor) // float(self.options.size_of_web)
            yield website, node


if __name__ == '__main__':
    PageRankJob().run()
