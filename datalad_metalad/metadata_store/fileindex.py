import abc
from typing import Iterator, Optional


class FileIndex(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def __contains__(self, path: str) -> bool:
        """ Returns True if the path is in the index """
        pass

    @abc.abstractmethod
    def __len__(self) -> int:
        """ Returns the number of path entries in the index """
        pass

    @abc.abstractmethod
    def add_content(self, key: str, content: bytearray):
        pass

    @abc.abstractmethod
    def replace_content(self, path: str, content: bytearray):
        pass

    def get_content(self, path: str) -> bytes:
        pass

    @abc.abstractmethod
    def delete_content(self, key: str):
        pass

    @abc.abstractmethod
    def get_keys(self, pattern: Optional[str] = None) -> Iterator[str]:
        """ Get all keys matching pattern, if pattern is provided, else all keys """
        pass
