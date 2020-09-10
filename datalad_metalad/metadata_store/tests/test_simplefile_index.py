import unittest
from typing import Tuple

from datalad_metalad.metadata_store.storage_backend import StorageBackend
from datalad_metalad.metadata_store.simplefile_index import SimpleFileIndex


class DummyStorageBackend(StorageBackend):
    def __init__(self, file_name: str):
        super(DummyStorageBackend, self).__init__(file_name)
        self.offset = 0

    def append_content(self, content: bytearray) -> Tuple[int, int]:
        offset = self.offset
        size = len(content)
        self.offset += size
        return offset, size

    def read_content(self, offset: int, size: int) -> bytes:
        return bytes(range(offset, offset + size))

    def flush(self):
        pass


class TestSimpleFileIndex(unittest.TestCase):
    def test_append(self):
        sfi = SimpleFileIndex("dir", DummyStorageBackend)
        content = bytearray(range(3))
        sfi.add_content("a1", content)
        sfi.add_content("a2", content)
        sfi.add_content("a3", content)
        self.assertEqual(sfi.paths["a3"].content_offset, 2 * len(content))
        self.assertEqual(sfi.paths["a3"].size, len(content))

    def test_delete(self):
        sfi = SimpleFileIndex("dir", DummyStorageBackend)
        content = bytearray(range(3))
        sfi.add_content("a1", content)
        self.assertEqual(len(sfi), 1)
        sfi.delete_content("a1")
        self.assertEqual(len(sfi), 0)
        sfi.add_content("a1", content)
        self.assertEqual(len(sfi), 1)
        self.assertEqual(sfi.paths["a1"].content_offset, len(content))
        self.assertEqual(sfi.paths["a1"].size, len(content))
        self.assertEqual(len(sfi.deleted_regions), 1)
        self.assertEqual(sfi.deleted_regions[0].content_offset, 0)
        self.assertEqual(sfi.deleted_regions[0].size, len(content))

    def test_key_access(self):
        content = bytearray(range(3))
        sfi = SimpleFileIndex("dir", DummyStorageBackend)
        sfi.add_content("a1", content)
        self.assertEqual(tuple(sfi.get_keys()), ("a1",))
        sfi.add_content("a2", content)
        self.assertEqual(tuple(sfi.get_keys()), ("a1", "a2"))
        sfi.add_content("a3", content)
        self.assertEqual(tuple(sfi.get_keys()), ("a1", "a2", "a3"))
        self.assertEqual(tuple(sfi.get_keys("a[12]")), ("a1", "a2"))


if __name__ == '__main__':
    unittest.main()
