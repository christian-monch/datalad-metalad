import unittest
from typing import List, Optional, Tuple

from datalad_metalad.metadata_store.exceptions import PathAlreadyExists
from datalad_metalad.metadata_store.storage_backend import StorageBackend
from datalad_metalad.metadata_store.simplefile_index import SimpleFileIndex


class DummyStorageBackend(StorageBackend):
    def __init__(self, file_name: str):
        super(DummyStorageBackend, self).__init__(file_name)
        self.offset = 0
        self.content = bytearray()

    def append_content(self, content: bytearray) -> Tuple[int, int]:
        offset = self.offset
        size = len(content)
        self.offset += size
        self.content += content
        return offset, size

    def read_content(self, offset: int, size: int) -> bytes:
        return bytes(self.content[offset:offset + size])

    def flush(self):
        pass


class TestSimpleFileIndex(unittest.TestCase):

    def get_content_objects(self, number: int, length: Optional[int] = 3) -> List[bytearray]:
        return [bytearray(range(i, i + length)) for i in range(number)]

    def test_append(self):
        """ Check adding and retrieving """
        sfi = SimpleFileIndex("dir", DummyStorageBackend)
        content = self.get_content_objects(3)
        sfi.add_content("a0", content[0])
        sfi.add_content("a1", content[1])
        sfi.add_content("a2", content[2])

        self.assertRaises(PathAlreadyExists, sfi.add_content, "a0", content[0])
        self.assertRaises(PathAlreadyExists, sfi.add_content, "a1", content[1])
        self.assertRaises(PathAlreadyExists, sfi.add_content, "a2", content[2])

        self.assertEqual(sfi.get_metadata("a0"), content[0])
        self.assertEqual(sfi.get_metadata("a1"), content[1])
        self.assertEqual(sfi.get_metadata("a2"), content[2])

    def test_delete(self):
        sfi = SimpleFileIndex("dir", DummyStorageBackend)
        content = self.get_content_objects(2)

        sfi.add_content("a0", content[0])
        self.assertEqual(len(sfi), 1)
        sfi.delete_content("a0")
        self.assertEqual(len(sfi), 0)
        self.assertEqual(len(sfi.deleted_regions), 1)
        self.assertEqual(sfi.deleted_regions[0].content_offset, 0)
        self.assertEqual(sfi.deleted_regions[0].size, len(content[0]))

        self.assertRaises(KeyError, sfi.delete_content, "a0")

        sfi.add_content("a0", content[1])
        self.assertEqual(sfi.get_metadata("a0"), content[1])

    def test_replace(self):
        sfi = SimpleFileIndex("dir", DummyStorageBackend)
        content = self.get_content_objects(2)
        sfi.add_content("a0", content[0])
        self.assertEqual(sfi.get_metadata("a0"), content[0])
        sfi.replace_content("a0", content[1])
        self.assertEqual(sfi.get_metadata("a0"), content[1])

        self.assertRaises(KeyError, sfi.replace_content, "xx", content[0])

    def test_key_access(self):
        content = bytearray(range(3))
        sfi = SimpleFileIndex("dir", DummyStorageBackend)
        sfi.add_content("a1", content)
        self.assertEqual(tuple(sfi.get_paths()), ("a1",))
        sfi.add_content("a2", content)
        self.assertEqual(tuple(sfi.get_paths()), ("a1", "a2"))
        sfi.add_content("a3", content)
        self.assertEqual(tuple(sfi.get_paths()), ("a1", "a2", "a3"))
        self.assertEqual(tuple(sfi.get_paths("a[12]")), ("a1", "a2"))


if __name__ == '__main__':
    unittest.main()
