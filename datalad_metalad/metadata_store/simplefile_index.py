import json
import logging
import os
import re
from typing import Generator, List, Iterator, Optional, Tuple, Type

from datalad_metalad.metadata_store.exceptions import MetadataAlreadyExists, PathAlreadyExists
from datalad_metalad.metadata_store.fileindex import FileIndex, JSONObject
from datalad_metalad.metadata_store.storage_backend import StorageBackend


LOGGER = logging.getLogger("metadata_store")

OFFSET_INDEX = 0
SIZE_INDEX = 1



class SimpleFileIndex(FileIndex):

    IndexVersion = "SimpleFileIndex-0.1"

    def __init__(self, base_dir_name: str, storage_backend_class: Type[StorageBackend], empty: Optional[bool] = False):
        self.base_dir_name = base_dir_name
        self.index_file_name = os.path.join(self.base_dir_name, "index.json")
        self.storage_file_name = os.path.join(self.base_dir_name, "content")

        self.paths = {}
        self.dataset_paths = {}
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

    def __iter__(self) -> Generator[Tuple[str, bool, str, Iterator], None, None]:
        for path, metadata_dict in self.paths.items():
            for metadata_format, region_info in metadata_dict.items():
                yield path, path in self.dataset_paths, metadata_format, self.storage_backend.byte_iterator(
                    region_info[OFFSET_INDEX],
                    region_info[SIZE_INDEX])

    def _ensure_path_exists(self, path: str):
        if path not in self.paths:
            raise KeyError(f"{path} not in index")

    def _ensure_path_does_not_exist(self, path: str):
        if path in self.paths:
            raise PathAlreadyExists(f"{path} already in index")

    def _ensure_format_exists(self, path: str, metadata_format: str):
        if path not in self.paths or metadata_format not in self.paths[path]:
            raise KeyError(f"{path}#{metadata_format} not in index")

    def _ensure_format_does_not_exist(self, path: str, metadata_format: str):
        if path in self.paths and metadata_format in self.paths[path]:
            raise MetadataAlreadyExists(f"{path}#{metadata_format} already in index")

    def _add_region_entry(self, path: str, metadata_format: str, offset: int, size: int):
        format_dict = self.paths.get(path, None)
        if format_dict is None:
            self.paths[path] = {metadata_format: (offset, size)}
        else:
            self.paths[path][metadata_format] = (offset, size)

    def _get_region_entry(self, path: str, metadata_format: str) -> Tuple[int, int]:
        return self.paths[path][metadata_format]

    def write(self):
        if self.dirty is True:
            index_object = {
                "version": self.IndexVersion,
                "paths": self.paths,
                "dataset_paths": self.dataset_paths,
                "deleted_regions": self.deleted_regions}
            with open(self.index_file_name, "tw") as file:
                file.write(json.dumps(index_object))
            self.dirty = False

    def read(self):
        with open(self.index_file_name, "tr") as file:
            index_info = json.load(file)
        index_version = index_info.get("version", "0.0") if index_info else "0.0"
        if index_version != self.IndexVersion:
            raise ValueError(
                f"index file version {index_version} does not "
                f"match code version {self.IndexVersion}")
        self.dirty = False

    def add_path(self, path: str):
        self._ensure_path_does_not_exist(path)
        self.paths[path] = {}
        self.dirty = True

    def set_dataset_entry(self, path: str, meta_metadata: JSONObject):
        self._ensure_path_exists(path)
        self.dataset_paths[path] = meta_metadata
        self.dirty = True

    def modify_dataset_entry(self, path: str, meta_metadata: str):
        self.dataset_paths[path] = meta_metadata
        self.dirty = True

    def delete_metadata_from_path(self, path: str, metadata_format: str, auto_delete_path: Optional[bool] = False):
        #self._ensure_format_exists(path, metadata_format)
        self.deleted_regions.append(self.paths[path][metadata_format])
        del self.paths[path][metadata_format]
        if not self.paths[path] and auto_delete_path is True:
            del self.paths[path]
        self.dirty = True

    def delete_path(self, path: str):
        #self._ensure_path_exists(path)
        for region_entry in self.paths[path].values():
            self.deleted_regions.append(region_entry)
        del self.paths[path]
        self.dirty = True

    def add_metadata_to_path(self, path: str, metadata_format: str, content: bytearray):
        self._ensure_format_does_not_exist(path, metadata_format)
        offset, size = self.storage_backend.append_content(content)
        self._add_region_entry(path, metadata_format, offset, size)
        self.dirty = True

    def replace_metadata_at_path(self, path: str, metadata_format: str, content: bytearray):
        #self._ensure_format_exists(path, metadata_format)
        self.delete_metadata_from_path(path, metadata_format, auto_delete_path=False)
        self.add_metadata_to_path(path, metadata_format, content)

    def get_metadata_formats(self, path: str) -> List[str]:
        #self._ensure_path_exists(path)
        return list(self.paths[path].keys())

    def get_metadata(self, path: str, metadata_format: str) -> bytes:
        #self._ensure_format_exists(path, metadata_format)
        region_entry = self._get_region_entry(path, metadata_format)
        return self.storage_backend.read_content(region_entry[OFFSET_INDEX], region_entry[SIZE_INDEX])

    def get_paths(self, pattern: Optional[str] = None) -> List[Tuple[str, bool]]:
        if pattern:
            matcher = re.compile(pattern)
            key_source = filter(lambda key: matcher.match(key) is not None, self.paths.keys())
        else:
            key_source = self.paths.keys()
        return [(path, path in self.dataset_paths) for path in key_source]

    def metadata_iterator(self, path: str, metadata_format: str) -> Iterator:
        #self._ensure_format_exists(path, metadata_format)
        return self.storage_backend.byte_iterator(
            self.paths[path][metadata_format][OFFSET_INDEX],
            self.paths[path][metadata_format][SIZE_INDEX])

    def flush(self):
        self.storage_backend.flush()
        self.write()


def join(joined_base_dir_name: str,
         left_prefix: str,
         left_index: SimpleFileIndex,
         right_prefix: str,
         right_index: SimpleFileIndex) -> SimpleFileIndex:

    def join_paths(left: str, right: str) -> str:
        return (
            left.rstrip("/") + "/" + right.lstrip("/")
            if left
            else right)

    assert left_index != right_index
    assert left_prefix != right_prefix
    assert isinstance(left_index, SimpleFileIndex)
    assert isinstance(right_index, SimpleFileIndex)
    assert isinstance(left_index.storage_backend, type(right_index.storage_backend))
    assert left_index.base_dir_name != joined_base_dir_name
    assert right_index.base_dir_name != left_index.base_dir_name

    joined_backend, left_offset, right_offset = left_index.storage_backend.join(
        os.path.join(joined_base_dir_name, "content"),
        left_index.storage_backend,
        right_index.storage_backend)

    joined_index = SimpleFileIndex(
        joined_base_dir_name,
        type(left_index.storage_backend),
        empty=True)

    # Join paths
    fixed_paths_from_left = {
        join_paths(left_prefix, path): {
            metadata_format: (region_entry[OFFSET_INDEX] + left_offset, region_entry[SIZE_INDEX])
            for metadata_format, region_entry in index_entry.items()
        }
        for path, index_entry in left_index.paths.items()
    }

    fixed_paths_from_right = {
        join_paths(right_prefix, path): {
            metadata_format: (region_entry[OFFSET_INDEX] + right_offset, region_entry[SIZE_INDEX])
            for metadata_format, region_entry in index_entry.items()
        }
        for path, index_entry in right_index.paths.items()
    }

    joined_index.paths = {
        **fixed_paths_from_left,
        **fixed_paths_from_right
    }

    # Join dataset paths
    fixed_dataset_paths_from_left = {
        join_paths(left_prefix, path): dataset_metadata
        for path, dataset_metadata in left_index.dataset_paths.items()
    }

    fixed_dataset_paths_from_right = {
        join_paths(right_prefix, path): dataset_metadata
        for path, dataset_metadata in right_index.dataset_paths.items()
    }

    joined_index.dataset_paths = {
        **fixed_dataset_paths_from_left,
        **fixed_dataset_paths_from_right
    }

    # Join deleted regions
    fixed_deleted_regions_from_left = [
        (region_entry[OFFSET_INDEX] + left_offset, region_entry[SIZE_INDEX])
        for region_entry in left_index.deleted_regions
    ]

    fixed_deleted_regions_from_right = [
        (region_entry[OFFSET_INDEX] + right_offset, region_entry[SIZE_INDEX])
        for region_entry in right_index.deleted_regions
    ]

    joined_index.deleted_regions = fixed_deleted_regions_from_left + fixed_deleted_regions_from_right

    joined_index.dirty = True
    return joined_index


SimpleFileIndex.join = join


if __name__ == "__main__":
    import time
    from itertools import count
    from datalad_metalad.metadata_store.filestorage_backend import FileStorageBackend

    entries = 10000000 #1000000

    start_time = time.time()
    lios = SimpleFileIndex("/home/cristian/tmp/index_store_test/left", FileStorageBackend)

    read_time = time.time()
    print(f"duration of init left: {(read_time - start_time)}")

    try:
        lios.add_metadata_to_path(
            "/",
            'ng_dataset',
            bytearray('{content: "left side ng dataset info"}', encoding='utf-8'))
        lios.set_dataset_entry("/", {"left side dataset meta-metadata": "true"})

        for i in range(entries):
            lios.add_metadata_to_path(f"/e{i}", "ng_file", bytearray(f"left #{i}", encoding="utf-8"))
        lios.flush()
    except (PathAlreadyExists, MetadataAlreadyExists):
        print("sl seems to be set, skipping its creation")

    add_time_left = time.time()
    print(f"add time left: {(add_time_left - read_time)}")


    rios = SimpleFileIndex("/home/cristian/tmp/index_store_test/right", FileStorageBackend)

    read_time_right = time.time()
    print(f"duration of init right: {(read_time_right - add_time_left)}")

    try:
        rios.add_metadata_to_path(
            "/",
            'ng_dataset',
            bytearray('{content: "right side dataset info"}', encoding='utf-8'))
        rios.set_dataset_entry("/", {"right side dataset meta-metadata": "true"})

        for i in range(entries):
            rios.add_metadata_to_path(f"/e{i}", "ng_file", bytearray(f"right #{i}", encoding="utf-8"))
        rios.flush()
    except (PathAlreadyExists, MetadataAlreadyExists):
        print("sr seems to be set, skipping its creation")

    fill_time_right = time.time()
    print(f"add time right: {(fill_time_right - read_time_right)}")


    start_time = time.time()

    combined_ios = SimpleFileIndex.join(
        "/home/cristian/tmp/index_store_test/joined",
        "/left", lios,
        "/right", rios)

    combine_time = time.time()
    print(f"duration of combine: {(combine_time - start_time)}")

    combined_ios.flush()

    flush_time = time.time()
    print(f"duration of flush: {(flush_time - combine_time)}")

    print(f"combined_ios('/left/e10'): {combined_ios.get_metadata('/left/e10', 'ng_file')}")
    print(f"combined_ios('/right/e20'): {combined_ios.get_metadata('/right/e20', 'ng_file')}")

    print(f"lios('/', 'ng_dataset'): {lios.get_metadata('/', 'ng_dataset')}")
    print(f"lios('/e10', 'ng_file'): {lios.get_metadata('/e10', 'ng_file')}")
    print(f"lios('/e20', 'ng_file'): {lios.get_metadata('/e20', 'ng_file')}")

    exit(0)
    c = count()
    for path, is_dataset, metadata, reader in combined_ios:
        print("+" * 20)
        print(f"{is_dataset}[{metadata}]: {path}")
        for b in reader:
            print(b)
        if c.__next__() == 40:
            break

    print("XXXXX" * 20)
    for b in combined_ios.metadata_iterator("/left/e19", "ng_file"):
        print(b)
