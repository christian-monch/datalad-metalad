import abc
from typing import Any, Dict, Iterator, List, Union


JSONObject = Union[Dict, List]


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
    def add_file_entry(self, path: str):
        pass

    @abc.abstractmethod
    def add_dataset_entry(self, path: str, meta_metadata: JSONObject):
        pass

    @abc.abstractmethod
    def modify_dataset_entry(self, path: str, meta_metadata: str):
        pass

    @abc.abstractmethod
    def add_metadata_to_path(self, path: str, metadata_format: str, content: bytearray):
        pass

    @abc.abstractmethod
    def replace_metadata_at_path(self, path: str, metadata_format: str, content: bytearray):
        pass

    def get_content(self, path: str, metadata_format: str) -> bytes:
        pass

    def get_all_content(self, path: str) -> Dict[str, bytes]:
        pass

    #@abc.abstractmethod
    #def delete_content(self, path: str, metadata_format: str):
    #    pass

    #@abc.abstractmethod
    #def get_paths(self, pattern: Optional[str] = None) -> Iterator[str]:
    #    """ Get all paths matching pattern, if pattern is provided, else all keys """
    #    pass

    @abc.abstractmethod
    def content_iterator(self, path: str) -> Iterator:
        pass

    @staticmethod
    def join(joined_base_dir_name: str,
             left_prefix: str,
             left_index,
             right_prefix: str,
             right_index) -> Any:
        """
        Creates a new file index, located in the given directory
        and returns the file index object
        """
        raise NotImplementedError

