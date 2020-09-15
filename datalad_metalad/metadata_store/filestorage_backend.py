import subprocess
from typing import Any, Tuple

from datalad_metalad.metadata_store.storage_backend import StorageBackend


class FileStorageBackendIterator:
    def __init__(self, file_name: str, offset: int, size: int):
        self.file = open(file_name, "rb")
        self.file.seek(offset)
        self.bytes_left = size

    def __iter__(self):
        while self.bytes_left > 0:
            value = self.file.read()
            if len(value) > self.bytes_left:
                value = value[:self.bytes_left]
            self.bytes_left -= len(value)
            yield value


class FileStorageBackend(StorageBackend):
    def __init__(self, file_name: str):
        super(FileStorageBackend, self).__init__(file_name)
        self.file_name = file_name
        self.file = open(self.file_name, "ab+")
        self.current_offset = self.file.tell()
        self.write_cache = []
        self.join = join

    def __del__(self):
        self.file.close()

    def __len__(self):
        return self.current_offset + max([
            cache_entry[0] + cache_entry[1] for cache_entry in self.write_cache] or [0])

    def append_content(self, content: bytearray) -> Tuple[int, int]:
        self.write_cache.append((self.current_offset, content))
        size = len(content)
        self.current_offset += size
        return self.current_offset - size, size

    def read_content(self, offset: int, size: int) -> bytes:
        self.flush()
        self.file.seek(offset)
        return self.file.read(size)

    def byte_iterator(self, offset: int, size: int) -> FileStorageBackendIterator:
        return FileStorageBackendIterator(self.file_name, offset, size)

    def flush(self):
        self.write_cache.sort(key=lambda offset_content_pair: offset_content_pair[0])
        current_position = self.file.tell()
        for (offset, content) in self.write_cache:
            if current_position != offset:
                self.file.seek(offset)
            self.file.write(content)
            current_position += (offset + len(content))
        self.file.flush()
        self.write_cache = []


def join(joined_file_name, left_backend, right_backend) -> Tuple[Any, int, int]:
    assert left_backend != right_backend
    assert isinstance(left_backend, FileStorageBackend)
    assert isinstance(right_backend, FileStorageBackend)
    assert joined_file_name != left_backend.file_name
    assert joined_file_name != right_backend.file_name

    left_backend.flush()
    right_backend.flush()
    subprocess.run(
        f"cat '{left_backend.file_name}' '{right_backend.file_name}' > '{joined_file_name}'",
        shell=True)

    return FileStorageBackend(joined_file_name), 0, len(left_backend)
