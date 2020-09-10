import json
import logging
import os
import re
from collections import namedtuple
from typing import Iterator, Optional, Type

from datalad_metalad.metadata_store.exceptions import PathAlreadyExists
from datalad_metalad.metadata_store.fileindex import FileIndex
from datalad_metalad.metadata_store.storage_backend import StorageBackend


LOGGER = logging.getLogger("metadata_store")


RegionEntry = namedtuple("RegionEntry", ["content_offset", "size"])


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
        self.dirty = True

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
         right_index: SimpleFileIndex) -> SimpleFileIndex:

    assert left_index != right_index
    assert left_prefix != right_prefix
    assert isinstance(left_index, SimpleFileIndex)
    assert isinstance(right_index, SimpleFileIndex)
    assert type(left_index.storage_backend) == type(right_index.storage_backend)
    assert left_index.base_dir_name != joined_base_dir_name
    assert right_index.base_dir_name != left_index.base_dir_name

    joined_backend, left_offset, right_offset = left_index.storage_backend.join(
        os.path.join(joined_base_dir_name, "content"),
        left_index.storage_backend,
        right_index.storage_backend)

    joined_index = SimpleFileIndex(
        joined_base_dir_name,
        type(left_index.storage_backend),
        True)

    # Fix paths
    fixed_paths_from_left = {
        left_prefix + "/" + path: RegionEntry(region.content_offset + left_offset, region.size)
        for path, region in left_index.paths.items()}

    fixed_paths_from_right = {
        right_prefix + "/" + path: RegionEntry(region.content_offset + right_offset, region.size)
        for path, region in right_index.paths.items()}

    joined_index.paths = {
        **fixed_paths_from_left,
        **fixed_paths_from_right
    }

    # Fix deleted regions
    fixed_deleted_regions_from_left = [
        RegionEntry(region.content_offset + left_offset, region.size)
        for region in left_index.deleted_regions]

    fixed_deleted_regions_from_right = [
        RegionEntry(region.content_offset + right_offset, region.size)
        for region in right_index.deleted_regions]

    joined_index.deleted_regions = fixed_deleted_regions_from_left + fixed_deleted_regions_from_right

    joined_index.dirty = True
    return joined_index


SimpleFileIndex.join = join



if __name__ == "__main__":
    import time
    from datalad_metalad.metadata_store.filestorage_backend import FileStorageBackend

    entries = 100

    lios = SimpleFileIndex("/home/cristian/tmp/index_store_test/left", FileStorageBackend)
    try:
        for i in range(entries):
            lios.add_content(f"e{i}", bytearray(f"#{i}", encoding="utf-8"))
        lios.flush()
    except PathAlreadyExists:
        print("sl seems to be set, skipping its creation")

    rios = SimpleFileIndex("/home/cristian/tmp/index_store_test/right", FileStorageBackend)
    try:
        for i in range(entries):
            rios.add_content(f"e{i}", bytearray(f"#{i}", encoding="utf-8"))
        rios.flush()
    except PathAlreadyExists:
        print("sr seems to be set, skipping its creation")

    start_time = time.time()

    combined_ios = SimpleFileIndex.join(
        "/home/cristian/tmp/index_store_test/joined",
        "left", lios,
        "right", rios)

    combine_time = time.time()
    print(f"duration of combine: {int(combine_time - start_time)}")

    combined_ios.flush()

    flush_time = time.time()
    print(f"duration of flush: {int(flush_time - combine_time)}")

    print("----------------")
    print(lios.paths)
    print(lios.deleted_regions)

    print("----------------")
    print(rios.paths)
    print(rios.deleted_regions)

    print("----------------")
    print(combined_ios.paths)
    print(combined_ios.deleted_regions)

    combined_ios.flush()

    print(f"combined_ios('left/e10'): {combined_ios.get_content('left/e10')}")
    print(f"combined_ios('right/e20'): {combined_ios.get_content('right/e20')}")
