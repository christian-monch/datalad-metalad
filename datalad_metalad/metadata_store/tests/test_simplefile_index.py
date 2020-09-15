import unittest
from typing import Iterator, List, Optional, Tuple

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

    def byte_iterator(self, offset: int, size: int) -> Iterator:
        return iter(self.content[offset:offset + size])

    def flush(self):
        pass


class TestSimpleFileIndex(unittest.TestCase):

    Format = "metadata_test"

    def get_content_objects(self, number: int, length: Optional[int] = 3) -> List[bytearray]:
        return [bytearray(range(i, i + length)) for i in range(number)]

    def add_file_metadata(self, index: SimpleFileIndex, path: str, metadata_format: str, content: bytearray):
        index.add_path(path)
        index.add_metadata_to_path(path, metadata_format, content)

    def test_append(self):
        """ Check adding and retrieving """
        sfi = SimpleFileIndex("dir", DummyStorageBackend)
        content = self.get_content_objects(3)
        for i in range(3):
            self.add_file_metadata(sfi, f"a{i}", self.Format, content[i])
        for i in range(3):
            self.assertRaises(PathAlreadyExists, sfi.add_metadata_to_path, f"a{i}", self.Format, content[i])
        for i in range(3):
            self.assertEqual(sfi.get_metadata(f"a{i}", self.Format), content[i])

    def test_delete(self):
        sfi = SimpleFileIndex("dir", DummyStorageBackend)
        content = self.get_content_objects(2)

        self.add_file_metadata(sfi, "a0", self.Format, content[0])
        self.assertEqual(len(sfi), 1)
        sfi.delete_path("a0")
        self.assertEqual(len(sfi), 0)
        self.assertEqual(len(sfi.deleted_regions), 1)
        self.assertEqual(sfi.deleted_regions[0].content_offset, 0)
        self.assertEqual(sfi.deleted_regions[0].content_size, len(content[0]))

        self.assertRaises(KeyError, sfi.delete_path, "a0")

        self.add_file_metadata(sfi, "a0", self.Format, content[1])
        self.assertEqual(sfi.get_metadata("a0", self.Format), content[1])

    def test_replace(self):
        sfi = SimpleFileIndex("dir", DummyStorageBackend)
        content = self.get_content_objects(2)
        self.add_file_metadata(sfi, "a0", self.Format, content[0])
        self.assertEqual(sfi.get_metadata("a0", self.Format), content[0])
        sfi.replace_metadata_at_path("a0", self.Format, content[1])
        self.assertEqual(sfi.get_metadata("a0", self.Format), content[1])

        self.assertRaises(KeyError, sfi.replace_metadata_at_path, "xx", self.Format, content[0])

    def test_key_access(self):
        content = bytearray(range(3))
        sfi = SimpleFileIndex("dir", DummyStorageBackend)
        self.add_file_metadata(sfi, "a1", self.Format, content)
        self.assertEqual(tuple(sfi.get_paths()), ("a1",))
        self.add_file_metadata(sfi, "a2", self.Format, content)
        self.assertEqual(tuple(sfi.get_paths()), ("a1", "a2"))
        self.add_file_metadata(sfi, "a3", self.Format, content)
        self.assertEqual(tuple(sfi.get_paths()), ("a1", "a2", "a3"))
        self.assertEqual(tuple(sfi.get_paths("a[12]")), ("a1", "a2"))


if __name__ == '__main__':
    unittest.main()
