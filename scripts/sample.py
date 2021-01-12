
from rdflib.plugins.parsers.ntriples import NTriplesParser, Sink, ParseError
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

    def __init__(self):
        self._memory = dict()

    def __parse_triple(self, triple):
        (s, p, o) = triple
        return f"{s.n3()} {p.n3()} {o.n3()} ."

    def __has_encoding_issue(self, triple, encoding):
        (s, p, o) = triple
        try:
            f"{s.n3()} {p.n3()} {o.n3()}".encode(encoding)
            return False
        except:
            return True

    def __mark_as_visited(self, triple):
        key = hashlib.sha256(triple.encode('utf-8')).hexdigest()
        self._memory[key] = None

    def __visited(self, triple):
        key = hashlib.sha256(triple.encode('utf-8')).hexdigest()
        return key in self._memory
        
    def triple(self, s, p, o):
        if not self.__has_encoding_issue((s, p, o), "utf-8"):
            triple = self.__parse_triple((s, p, o))
            if not self.__visited(triple):
                self.__mark_as_visited(triple)
                print(triple)
        else:
            logger.warning(f"print error. ({s}, {p}, {o}) Dropped.  Reason {sys.exc_info()[0]}")


@click.command()
@click.argument("dataset", type=click.Path(exists=True))
@click.argument("size", type=click.INT)
def create_sample(dataset, size):
    n = NTParser(NTCleaner())
    with open(dataset, "rb") as input_file:
        n.skipparse(input_file, size)


if __name__ == "__main__":
    create_sample()