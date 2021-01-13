
from rdflib.plugins.parsers.ntriples import NTriplesParser, Sink, ParseError
from urllib.parse import urlparse
import hashlib, logging, coloredlogs, click, codecs, sys

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

class NTParser(NTriplesParser):

    def skipparse(self, dataset, max_size):
        """Parse dataset as an N-Triples/N3 file."""
        if not hasattr(dataset, 'read'):
            raise ParseError("Item to parse must be a file-like object.")

        self.file = codecs.getreader('utf-8')(dataset) # since N-Triples 1.1 files can and should be utf-8 encoded
        self.buffer = ''
        size = 0
        
        while size < max_size:
            self.line = self.readline()
            if self.line is None:
                break
            try:
                self.parseline()
                size += 1
            except ParseError:
                logger.warning(f"parse error: dropping {self.line}. Reason {sys.exc_info()[0]}")
                continue
        return self.sink


class NTCleaner(Sink):

    def __hash_term(self, term):
        if term.startswith('<'):
            url = urlparse(term[1:-1])
            path = hashlib.sha256(f'{url.path}{url.fragment}'.encode('utf-8')).hexdigest()
            return f'<{url.scheme}://{url.netloc}/{path}>'
        else:
            literal = hashlib.sha256(term.encode('utf-8')).hexdigest()
            return f'"{literal}"'

    def __parse_triple(self, triple):
        (s, p, o) = triple
        return f"{self.__hash_term(s.n3())} {p.n3()} {self.__hash_term(o.n3())} ."

    def triple(self, s, p, o):
        print(self.__parse_triple((s, p, o)))

@click.command()
@click.argument("dataset", type=click.Path(exists=True))
@click.argument("size", type=click.INT)
def create_sample(dataset, size):
    n = NTParser(NTCleaner())
    with open(dataset, "rb") as input_file:
        n.skipparse(input_file, size)


if __name__ == "__main__":
    create_sample()