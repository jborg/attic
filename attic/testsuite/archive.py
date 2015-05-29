import msgpack
from attic.testsuite import AtticTestCase
from attic.archive import CacheChunkBuffer, RobustUnpacker
from attic.key import PlaintextKey


class MockCache:

    def __init__(self):
        self.objects = {}

    def add_chunk(self, id, data, stats=None):
        self.objects[id] = data
        return id, len(data), len(data)


class ChunkBufferTestCase(AtticTestCase):

    def test(self):
        data = [{b'path': 1}, {b'path': 2}]
        cache = MockCache()
        key = PlaintextKey()
        chunks = CacheChunkBuffer(cache, key, None)
        for d in data:
            chunks.add(d)
            chunks.flush()
        chunks.flush(flush=True)
        self.assert_equal(len(chunks.chunks), 2)
        unpacker = msgpack.Unpacker()
        for id in chunks.chunks:
            unpacker.feed(cache.objects[id])
        self.assert_equal(data, list(unpacker))


class RobustUnpackerTestCase(AtticTestCase):

    def make_chunks(self, items):
        return b''.join(msgpack.packb({'path': item}) for item in items)

    def _validator(self, value):
        return isinstance(value, dict) and value.get(b'path') in (b'foo', b'bar', b'boo', b'baz')

    def process(self, input):
        unpacker = RobustUnpacker(validator=self._validator)
        result = []
        for should_sync, chunks in input:
            if should_sync:
                unpacker.resync()
            for data in chunks:
                unpacker.feed(data)
                for item in unpacker:
                    result.append(item)
        return result

    def test_extra_garbage_no_sync(self):
        chunks = [(False, [self.make_chunks([b'foo', b'bar'])]),
                  (False, [b'garbage'] + [self.make_chunks([b'boo', b'baz'])])]
        result = self.process(chunks)
        self.assert_equal(result, [
            {b'path': b'foo'}, {b'path': b'bar'},
            103, 97, 114, 98, 97, 103, 101,
            {b'path': b'boo'},
            {b'path': b'baz'}])

    def split(self, left, length):
        parts = []
        while left:
            parts.append(left[:length])
            left = left[length:]
        return parts

    def test_correct_stream(self):
        chunks = self.split(self.make_chunks([b'foo', b'bar', b'boo', b'baz']), 2)
        input = [(False, chunks)]
        result = self.process(input)
        self.assert_equal(result, [{b'path': b'foo'}, {b'path': b'bar'}, {b'path': b'boo'}, {b'path': b'baz'}])

    def test_missing_chunk(self):
        chunks = self.split(self.make_chunks([b'foo', b'bar', b'boo', b'baz']), 4)
        input = [(False, chunks[:3]), (True, chunks[4:])]
        result = self.process(input)
        self.assert_equal(result, [{b'path': b'foo'}, {b'path': b'boo'}, {b'path': b'baz'}])

    def test_corrupt_chunk(self):
        chunks = self.split(self.make_chunks([b'foo', b'bar', b'boo', b'baz']), 4)
        input = [(False, chunks[:3]), (True, [b'gar', b'bage'] + chunks[3:])]
        result = self.process(input)
        self.assert_equal(result, [{b'path': b'foo'}, {b'path': b'boo'}, {b'path': b'baz'}])
