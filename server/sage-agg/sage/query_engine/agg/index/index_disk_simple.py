import os
import json
import xxhash
import shutil

class IndexSimple:
    def __init__(self, aggregator):
        self._aggregator = aggregator
        self._id = self._aggregator.get_id()
        self._location = "/tmp/sage-distinct/{}/".format(self._id)
        self._seed = 0

    def update(self, group_key, binding, write=True):
        ghash = xxhash.xxh64_hexdigest(group_key, self._seed)
        bhash = xxhash.xxh64_hexdigest(json.dumps(binding), self._seed)
        group_key_location = self._location + "{}/".format(ghash)
        if not os.path.exists(group_key_location):
            os.makedirs(group_key_location)

        location = group_key_location + "{}".format(bhash)
        if not os.path.exists(location):
            with open(location, 'w') as f:
                if write:
                    f.write(json.dumps(binding))
                f.close()

    def list(self, group_key, read=True):
        ghash = xxhash.xxh64_hexdigest(group_key, self._seed)
        group = []
        for file in os.listdir(self._location + "{}/".format(ghash)):
            filename = self._location + "{}/{}".format(ghash, file)
            with open(filename, 'r') as f:
                content = ''
                if read:
                    content = f.read()
                    content = json.loads(content)
                group.append(content)
                f.close()
        print('group length: ', len(group))
        return group

    def close(self):
        # rm everything in self._location
        shutil.rmtree(self._location, ignore_errors=True)
