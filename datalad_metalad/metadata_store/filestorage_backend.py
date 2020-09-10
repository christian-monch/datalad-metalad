from typing import Tuple

from .storage_backend import StorageBackend


class FileStorageBackend(StorageBackend):
    def __init__(self, file_name: str):
        super(FileStorageBackend, self).__init__(file_name)
        self.file_name = file_name
        self.file = open(self.file_name, "ab+")
        self.current_offset = self.file.tell()
        self.write_cache = []

    def __len__(self):
        return self.current_offset

    def append_content(self, content: bytearray) -> Tuple[int, int]:
        self.write_cache.append((self.current_offset, content))
        size = len(content)
        self.current_offset += size
        return self.current_offset, self.current_offset - size

    def read_content(self, offset: int, size: int) -> bytes:
        self.flush()
        self.file.seek(offset)
        return self.file.read(size)

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
