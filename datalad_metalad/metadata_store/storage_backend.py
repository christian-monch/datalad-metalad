from abc import ABC, abstractmethod


class StorageBackend(ABC):
    def __init__(self, max_file_size):
        self.max_file_size = max_file_size

    @abstractmethod
    def add(self, path: str, content: bytes):
        raise NotImplemented

    @abstractmethod
    def get(self, path: str):
        raise NotImplemented

    @abstractmethod
    def delete(self, path: str):
        raise NotImplemented

    @abstractmethod
    def replace(self, path: str, content: bytes):
        raise NotImplemented
