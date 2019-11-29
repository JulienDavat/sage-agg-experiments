from python_hll.hll import HLL, HLLType
import xxhash
from math import log2, ceil, pow
import sys
regWidth = 6
error_rate = 0.05

def from_rate(rate):
    return ( 1.04 / rate ) * ( 1.04 / rate )

log2m = ceil(log2(from_rate(error_rate)))
bits = regWidth * pow(2, log2m)
print('Creating hll({}, {}) = {} bits, ({} bytes)'.format(log2m, regWidth, bits, bits/8))

hll = HLL(log2m, regWidth, type=HLLType.SPARSE)

max = 6000

error = []

step = 1
i = 1
cpt = 0
for x in range(0, max):
    fake_binding = {
        "s": "http://sage.univ-nantes.fr/s" + str(x),
        "p": "http://sage.univ-nantes.fr/p" + str(x),
        "o": "\"3544556767\"^^<http://sage.univ-nantes.fr/xsd:integer>"
    }
    try:
        hash = xxhash.xxh32(fake_binding["p"]).intdigest()
        hll.add_raw(hash)
        # cpt += 1
        # if cpt == step*i:
        #     min = cpt - cpt * error_rate
        #     max = cpt + cpt * error_rate
        #     card = hll.cardinality()
        #     if max >= card >= min:
        #         i += 1
        #     else:
        #         error .append(card)
        #         print("STOP broken {} >= {} >= {} for {}".format(max, card, min, cpt))
        #         i += 1
    except Exception as e:
        print('Inserted: ', cpt)
        raise e
        exit(1)

def getsizeof(obj, seen=None):
    """Recursively finds size of objects in bytes"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([sys.getsizeof(v, seen) for v in obj.values()])
        size += sum([sys.getsizeof(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += sys.getsizeof(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([sys.getsizeof(i, seen) for i in obj])
    return size

print(f'{hll.cardinality()} +/- {max * error_rate}')
print(f'hll size in bytes: {getsizeof(hll)}')
print(f'hll serialized size in bytes: {getsizeof(hll.to_bytes())}')
print(hll.to_bytes())

fake_binding = {
    "s": "http://sage.univ-nantes.fr/s0",
    "p": "http://sage.univ-nantes.fr/p0",
    "o": "\"3544556767\"^^<http://sage.univ-nantes.fr/xsd:integer>"
}
print(getsizeof({
    "binding": fake_binding,
    "set": [xxhash.xxh32_intdigest("http://sage.univ-nantes.fr/p0"), xxhash.xxh32_intdigest("http://sage.univ-nantes.fr/p0")]
}))


