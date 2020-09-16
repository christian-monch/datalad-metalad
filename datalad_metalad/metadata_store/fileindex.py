import abc
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union


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
    def add_path(self, path: str):
        pass

    @abc.abstractmethod
    def add_metadata_to_path(self, path: str, metadata_format: str, content: bytearray):
        pass

    @abc.abstractmethod
    def replace_metadata_at_path(self, path: str, metadata_format: str, content: bytearray):
        pass

    @abc.abstractmethod
    def get_metadata(self, path: str, metadata_format: str) -> bytes:
        pass

    @abc.abstractmethod
    def get_metadata_formats(self, path: str) -> List[str]:
        pass

    @abc.abstractmethod
    def set_dataset_entry(self, path: str, meta_metadata: JSONObject):
        """ mark an existing path as dataset root with the given meta_metadata """
        pass

    @abc.abstractmethod
    def modify_dataset_entry(self, path: str, meta_metadata: str):
        pass

    @abc.abstractmethod
    def delete_path(self, path: str):
        pass

    @abc.abstractmethod
    def delete_metadata_from_path(self, path: str, metadata_format: str, auto_delete_path: Optional[bool] = False):
        """
        Delete the content in metadata_format from the path. If no more metadata is present
        and autodelete is True and if the path is not a dataset, delete the path entry.
        """
        pass

    @abc.abstractmethod
    def get_paths(self, pattern: Optional[str] = None) -> List[Tuple[str, bool]]:
        pass

    @abc.abstractmethod
    def metadata_iterator(self, path: str, metadata_format: str) -> Iterator:
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

