from gzip import GzipFile
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol, JSONProtocol, TextValueProtocol
import requests
from warcio.archiveiterator import ArchiveIterator
from urllib.parse import urlparse
import ujson as json
import itertools


class MRCalculateLinkGraph(MRJob):

    @staticmethod
    def map_linkgraph(self,test):
        url = "https://commoncrawl.s3.amazonaws.com/"+test
        resp = requests.get(url, stream=True)

        c=0
        for record in ArchiveIterator(resp.raw, arc2warc=True):
            c+=1
            if record.content_type != 'application/json':
                continue
            try:
                page_info = json.loads(record.content_stream().read())
                page_url = page_info['Envelope']['WARC-Header-Metadata']['WARC-Target-URI']
                page_domain = urlparse(page_url).netloc
                links = page_info['Envelope']['Payload-Metadata']['HTTP-Response-Metadata']['HTML-Metadata']['Links']
                domains = set(filter(None, [urlparse(url['url']).netloc for url in links]))



                if len(domains) > 0:
                        yield page_domain, list(domains)
            except(KeyError, UnicodeDecodeError):
                pass
            if c == 100:
                break

    def reduce_linkgraph(self, domain, links):
        links = list(set(itertools.chain(*links)))
        yield domain, {'id': domain, 'state': 0, 'outgoing': links}



    def map_normalize(self, domain, data):
        yield data['id'], data



        for outgoing in data['outgoing']:
            yield outgoing, None



    def reduce_normalize(self, domain, data):
        self.increment_counter('linkgraph', 'size', 1)
        try:
            node = list(filter(lambda x: x is not None, data))[0]
        except IndexError:
            node = {'id': domain, 'state': 0, 'outgoing': []}

        yield domain, node



    def steps(self):
        return [MRStep(mapper=self.map_linkgraph, reducer=self.reduce_linkgraph),MRStep(mapper=self.map_normalize, reducer=self.reduce_normalize)]



if __name__ == '__main__':
    MRCalculateLinkGraph().run()
