import json
import logging
import os
import re
from typing import Iterator, Optional

from .exceptions import PathAlreadyExists


LOGGER = logging.getLogger("metadata_store")


class RegionEntry(object):
    def __init__(self, file_index, content_offset, size):
        self.file_index = file_index
        self.content_offset = content_offset
        self.size = size


class MultiFileIndex(object):

    IndexVersion = "MultiFileIndex-0.1"

    def __init__(self, base_dir_name: str, maximum_content_size: int, storage_backend_class):
        self.base_dir_name = base_dir_name
        self.maximum_content_size = maximum_content_size
        self.storage_backend_class = storage_backend_class
        self.index_file_name = os.path.join(self.base_dir_name, "index.json")
        self.storage_backend = None

        try:
            self.read()
        except FileNotFoundError:
            LOGGER.warning(f"no index found at {self.index_file_name}")
            self.file_index = 0
            self.content_offset = 0
            self.paths = {}
            self.deleted_regions = []

        self.update_storage_backend()
        self.dirty = False

    def __contains__(self, path: str):
        return path in self.paths

    def __len__(self):
        return len(self.paths)

    def update_storage_backend(self):
        if self.storage_backend:
            self.storage_backend.flush()
            del self.storage_backend
        self.storage_backend = self.storage_backend_class(
            os.path.join(
                self.base_dir_name,
                self.get_content_file_name("content_")))

    def next_content_file(self):
        self.file_index += 1
        self.content_offset = 0
        self.dirty = True

    def add_region_entry(self, path: str, offset: int, size: int):
        self.paths[path] = RegionEntry(self.file_index, offset, size)
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
                    "file_index": self.file_index,
                    "content_offset": self.content_offset,
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

        self.file_index = index_info["file_index"]
        self.content_offset = index_info["content_offset"]
        self.paths = {
            path: RegionEntry(entry_list[0], entry_list[1], entry_list[2])
            for path, entry_list in index_info["paths"].items()
        }
        self.deleted_regions = [
            RegionEntry(entry_list[0], entry_list[1], entry_list[2])
            for entry_list in index_info["deleted_regions"]
        ]
        self.dirty = False

    def get_content_file_name(self, basename: str) -> str:
        return f"{basename}{self.file_index:03}"

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

        if len(self.storage_backend) >= self.maximum_content_size:
            self.next_content_file()
            self.update_storage_backend()

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
