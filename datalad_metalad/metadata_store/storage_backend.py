from abc import ABCMeta, abstractmethod
from typing import Tuple


class StorageBackend(object): #  metaclass=ABCMeta):
    def __init__(self, file_name: str):
        self.file_name = file_name

    @abstractmethod
    def append_content(self, content: bytearray) -> Tuple[int, int]:
        """ Append content and return the offset at which the content was stored and the size of the stored content """
        pass

    @abstractmethod
    def read_content(self, offset: int, size: int) -> bytes:
        """ read content of size "size" at offset "offset". Notice that len(bytes) might be larger
            than size, if the storage is compressed """
        pass

    @abstractmethod
    def flush(self):
        pass
