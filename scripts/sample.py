
from rdflib.plugins.parsers.ntriples import NTriplesParser, Sink, ParseError
from urllib.parse import urlparse
import hashlib, logging, coloredlogs, click, codecs, sys

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

class NTParser(NTriplesParser):

    def skipparse(self, dataset, size):
        """Parse dataset as an N-Triples/N3 file."""
        if not hasattr(dataset, 'read'):
            raise ParseError("Item to parse must be a file-like object.")

        self.file = codecs.getreader('utf-8')(dataset) # since N-Triples 1.1 files can and should be utf-8 encoded
        self.buffer = ''
        line = ''
        nb_lines = 0
        logger.info(f'Start parsing the RDF file... {nb_lines} line written') 
        while size == -1 or nb_lines < size:
            line = self.readline()
            self.line = line
            if self.line is None:
                break
            try:
                self.parseline()
                nb_lines += 1
                if nb_lines % 10000 == 0:
                    logger.info(f'Parsing the RDF file... {nb_lines} lines written') 
            except (ParseError, UnicodeEncodeError) as exception:
                logger.warning(f"parse error: dropping {line}. Reason {exception}")
                continue
        logger.info(f'Parsing complete... {nb_lines} lines written')
        return self.sink


class NTTransformer(Sink):

    def __init__(self, format, hash, encoding):
        super(NTTransformer, self).__init__()
        self._format = format
        self._hash = hash
        self._encoding = encoding

    def __hash_term(self, term):
        if term.startswith('<http'):
            url = urlparse(term[1:-1])
            path = hashlib.sha256(f'{url.path}{url.fragment}'.encode('utf-8')).hexdigest()
            return f'{url.scheme}://{url.netloc}/{path}'
        else:
            return f'"{hashlib.sha256(term.encode("utf-8")).hexdigest()}"'

    def __check_encoding(self, term):
        term.encode(self._encoding)

    def __parse_triple(self, triple):
        (s, p, o) = triple
        subject = self.__hash_term(s.n3()) if self._hash else s.n3()
        predicate = p.n3()
        obj = self.__hash_term(o.n3()) if self._hash else o.n3()
        if self._format == 'nt':
            line = f'{subject} {predicate} {obj} .'
        elif self._format == 'csv':
            subject = subject[1:-1]
            predicate = predicate[1:-1]
            obj = obj[1:-1] if obj.startswith('<http') and obj.endswith('>') else obj
            line = f'{subject}|{predicate}|{obj}'
        self.__check_encoding(line)
        return line

    def triple(self, s, p, o):
        print(self.__parse_triple((s, p, o)))

@click.command()
@click.argument('dataset', type=click.Path(exists=True))
@click.option('--size', type=click.INT, default=-1)
@click.option('--format', type=click.Choice(['nt', 'csv']), default='nt')
@click.option('--hash', type=click.BOOL, default=False, is_flag=True)
@click.option('--encoding', type=click.Choice(['utf-8', 'ascii']), default='utf-8')
def create_sample(dataset, size, format, hash, encoding):
    n = NTParser(NTTransformer(format, hash, encoding))
    with open(dataset, "rb") as input_file:
        n.skipparse(input_file, size)


if __name__ == "__main__":
    create_sample()