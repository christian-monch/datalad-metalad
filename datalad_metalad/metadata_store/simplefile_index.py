import json
import logging
import os
import re
from typing import Iterator, Optional, Type

from datalad_metalad.metadata_store.exceptions import PathAlreadyExists
from datalad_metalad.metadata_store.fileindex import FileIndex
from datalad_metalad.metadata_store.storage_backend import StorageBackend


LOGGER = logging.getLogger("metadata_store")


class RegionEntry(object):
    def __init__(self, content_offset, size):
        self.content_offset = content_offset
        self.size = size


class SimpleFileIndex(FileIndex):

    IndexVersion = "SimpleFileIndex-0.1"

    def __init__(self, base_dir_name: str, storage_backend_class: Type[StorageBackend], empty: Optional[bool] = False):
        self.base_dir_name = base_dir_name
        self.index_file_name = os.path.join(self.base_dir_name, "index.json")
        self.storage_file_name = os.path.join(self.base_dir_name, "content")

        self.paths = {}
        self.deleted_regions = []
        if not empty:
            try:
                self.read()
            except FileNotFoundError:
                LOGGER.warning(f"no index found at {self.index_file_name}")

        self.storage_backend = storage_backend_class(self.storage_file_name)
        self.dirty = False

    def __contains__(self, path: str):
        return path in self.paths

    def __len__(self):
        return len(self.paths)

    def add_region_entry(self, path: str, offset: int, size: int):
        self.paths[path] = RegionEntry(offset, size)
        self.dirty = True

    def get_region_entry(self, path: str) -> RegionEntry:
        if path not in self.paths:
            raise KeyError(f"{path} not in index")
        return self.paths[path]

    def write(self):
        if self.dirty is True:
            with open(self.index_file_name, "tw") as file:
                file.write(json.dumps({
                    "version": self.IndexVersion,
                    "paths": self.paths,
                    "deleted_regions": self.deleted_regions
                }))
            self.dirty = False

    def read(self):
        with open(self.index_file_name, "tr") as file:
            index_info = json.load(file)
        index_version = index_info.get("version", "0.0")
        if index_version != self.IndexVersion:
            raise ValueError(
                f"index file version {index_version} does not "
                f"match code version {self.IndexVersion}")

        self.paths = {
            path: RegionEntry(entry_list[0], entry_list[1])
            for path, entry_list in index_info["paths"].items()
        }
        self.deleted_regions = [
            RegionEntry(entry_list[0], entry_list[1])
            for entry_list in index_info["deleted_regions"]
        ]
        self.dirty = False

    def delete_content(self, path: str):
        if path not in self.paths:
            raise KeyError(f"{path} not in index")
        self.deleted_regions.append(self.paths[path])
        del self.paths[path]
        self.dirty = True

    def add_content(self, path: str, content: bytearray):
        if path in self.paths:
            raise PathAlreadyExists(f"{path} already in index")
        offset, size = self.storage_backend.append_content(content)
        self.add_region_entry(path, offset, size)

    def replace_content(self, path: str, content: bytearray):
        self.delete_content(path)
        self.add_content(path, content)

    def get_content(self, path: str) -> bytes:
        region_entry = self.get_region_entry(path)
        return self.storage_backend.read_content(region_entry.content_offset, region_entry.size)

    def get_keys(self, pattern: Optional[str] = None) -> Iterator[str]:
        if pattern:
            matcher = re.compile(pattern)
            return filter(lambda key: matcher.match(key) is not None, self.paths.keys())
        return iter(self.paths.keys())

    def flush(self):
        self.storage_backend.flush()
        self.write()


def join(joined_base_dir_name: str,
         left_prefix: str,
         left_index: SimpleFileIndex,
         right_prefix: str,
         right_index: SimpleFileIndex):
    """ merge two indices """

    assert type(left_index) == type(right_index)
    assert type(left_index.storage_backend) == type(right_index.storage_backend)

    joined_index = SimpleFileIndex(
        joined_base_dir_name,
        type(left_index.storage_backend),
        True)

    entries_from_left = [

    ]
