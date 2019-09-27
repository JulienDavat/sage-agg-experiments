# utils.py
# Author: Thomas MINIER - MIT License 2017-2019
from re import compile
from math import ceil

regex = compile('"(.*)"\\^\\^(.+)')


def to_numeric(literal):
    """Convert a RDF Literal into its numeric value, or 0 if no cast is possible"""
    if not literal.startswith('"'):
        return 0
    else:
        matcher = regex.search(literal)
        # not a typed literal
        if matcher is None:
            return 0
        try:
            # try to cast float -> int when possible
            v = float(matcher.group(1))
            return int(v) if ceil(v) == v else v
        except Exception:
            return 0
