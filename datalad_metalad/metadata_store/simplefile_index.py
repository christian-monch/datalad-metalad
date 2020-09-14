import json
import logging
import os
import re
from typing import Dict, List, Iterator, Optional, Type, Union

from datalad_metalad.metadata_store.exceptions import MetadataAlreadyExists, PathAlreadyExists
from datalad_metalad.metadata_store.fileindex import FileIndex, JSONObject
from datalad_metalad.metadata_store.storage_backend import StorageBackend


LOGGER = logging.getLogger("metadata_store")


class MetadataRegionEntry(object):
    def __init__(self, content_offset: int, content_size: int):
        self.content_offset = content_offset
        self.content_size = content_size


class FileIndexEntry(object):
    def __init__(self):
        self.format_entries = {}


class DatasetIndexEntry(FileIndexEntry):
    def __init__(self, meta_metadata: JSONObject):
        super(DatasetIndexEntry, self).__init__()
        self.meta_metadata = meta_metadata


class SimpleFileIndexJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, DatasetIndexEntry):
            return {
                "meta_metadata": self.default(obj.meta_metadata),
                "format_entries": self.default(obj.format_entries)
            }
        if isinstance(obj, FileIndexEntry):
            return {
                "format_entries": self.default(obj.format_entries)
            }
        if isinstance(obj, MetadataRegionEntry):
            return {
                "offset": obj.content_offset,
                "size": obj.content_size
            }
        return obj


def simple_file_index_json_decoder(dct):
    if "meta_metadata" in dct:
        dataset_index_entry = DatasetIndexEntry(dct["meta_metadata"])
        dataset_index_entry.format_entries = dct["format_entries"]
        return dataset_index_entry
    if "format_entries" in dct:
        file_index_entry = FileIndexEntry()
        file_index_entry.format_entries = dct["format_entries"]
        return file_index_entry
    if "offset" in dct:
        return MetadataRegionEntry(dct["offset"], dct["size"])
    return dct


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

    def __iter__(self):
        for path, entry in self.paths.items():
            yield path, self.storage_backend.byte_iterator(entry.content_offset, entry.size)

    def _ensure_path_exists(self, path: str):
        if path not in self.paths:
            raise KeyError(f"{path} not in index")

    def _ensure_path_does_not_exist(self, path: str):
        if path in self.paths:
            raise PathAlreadyExists(f"{path} already in index")

    def _ensure_format_exists(self, path: str, metadata_format: str):
        if path not in self.paths or metadata_format not in self.paths[path].format_entries:
            raise KeyError(f"{path}#{metadata_format} not in index")

    def _ensure_format_does_not_exist(self, path: str, metadata_format: str):
        if path in self.paths and metadata_format in self.paths[path].format_entries:
            raise MetadataAlreadyExists(f"{path}#{metadata_format} already in index")

    def _add_region_entry(self, path: str, metadata_format: str, offset: int, size: int):
        self.paths[path].format_entries[metadata_format] = MetadataRegionEntry(offset, size)
        self.dirty = True

    def _get_region_entry(self, path: str, metadata_format: str) -> MetadataRegionEntry:
        return self.paths[path].format_entries[metadata_format]

    def write(self):
        if self.dirty is True:
            index_object = {
                "version": self.IndexVersion,
                "paths": self.paths,
                "deleted_regions": self.deleted_regions}
            with open(self.index_file_name, "tw") as file:
                file.write(json.dumps(index_object, cls=SimpleFileIndexJSONEncoder))
            self.dirty = False

    def read(self):
        with open(self.index_file_name, "tr") as file:
            index_info = json.load(file, object_hook=simple_file_index_json_decoder)
        index_version = index_info.get("version", "0.0") if index_info else "0.0"
        if index_version != self.IndexVersion:
            raise ValueError(
                f"index file version {index_version} does not "
                f"match code version {self.IndexVersion}")
        self.dirty = False

    def add_file_entry(self, path: str):
        self._ensure_path_does_not_exist(path)
        self.paths[path] = FileIndexEntry()

    def add_dataset_entry(self, path: str, meta_metadata: JSONObject):
        self._ensure_path_does_not_exist(path)
        self.paths[path] = DatasetIndexEntry(meta_metadata)

    def modify_dataset_entry(self, path: str, meta_metadata: str):
        self._ensure_path_exists(path)
        assert isinstance(self.paths[path], DatasetIndexEntry)
        self.paths[path].meta_metadata = meta_metadata

    def delete_metadata_from_path(self, path: str, metadata_format: str, auto_delete_path: Optional[bool] = False):
        self._ensure_format_exists(path, metadata_format)
        self.deleted_regions.append(self.paths[path].format_entries[metadata_format])
        del self.paths[path].format_entries[metadata_format]
        if not self.paths[path].format_entries and auto_delete_path is True:
            del self.paths[path]
        self.dirty = True

    def delete_path(self, path: str):
        self._ensure_path_exists(path)
        for region_entry in self.paths[path].format_entries.values():
            self.deleted_regions.append(region_entry)
        del self.paths[path]

    def add_metadata_to_path(self, path: str, metadata_format: str, content: bytearray):
        self._ensure_path_exists(path)
        self._ensure_format_does_not_exist(path, metadata_format)
        offset, size = self.storage_backend.append_content(content)
        self._add_region_entry(path, metadata_format, offset, size)

    def replace_metadata_at_path(self, path: str, metadata_format: str, content: bytearray):
        self._ensure_format_exists(path, metadata_format)
        self.delete_metadata_from_path(path, metadata_format, auto_delete_path=False)
        self.add_metadata_to_path(path, metadata_format, content)

    def get_metadata_formats(self, path: str) -> List[str]:
        self._ensure_path_exists(path)
        return list(self.paths[path].format_entries.keys())

    def get_metadata(self, path: str, metadata: str) -> bytes:
        self._ensure_format_exists(path, metadata)
        region_entry = self._get_region_entry(path, metadata)
        return self.storage_backend.read_content(region_entry.content_offset, region_entry.content_size)

    def get_paths(self, pattern: Optional[str] = None) -> Iterator[str]:
        if pattern:
            matcher = re.compile(pattern)
            return filter(lambda key: matcher.match(key) is not None, self.paths.keys())
        return iter(self.paths.keys())

    def metadata_iterator(self, path: str, metadata_format: str) -> Iterator:
        self._ensure_path_exists(path)
        return self.storage_backend.byte_iterator(self.paths[path].content_offset, self.paths[path].size)

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
        join_paths(left_prefix, path): FileIndexEntry(region.content_offset + left_offset, region.size)
        for path, region in left_index.paths.items()}

    fixed_paths_from_right = {
        join_paths(right_prefix, path): FileIndexEntry(region.content_offset + right_offset, region.size)
        for path, region in right_index.paths.items()}

    joined_index.paths = {
        **fixed_paths_from_left,
        **fixed_paths_from_right
    }

    # Fix deleted regions
    fixed_deleted_regions_from_left = [
        FileIndexEntry(region.content_offset + left_offset, region.size)
        for region in left_index.deleted_regions]

    fixed_deleted_regions_from_right = [
        FileIndexEntry(region.content_offset + right_offset, region.size)
        for region in right_index.deleted_regions]

    joined_index.deleted_regions = fixed_deleted_regions_from_left + fixed_deleted_regions_from_right

    joined_index.dirty = True
    return joined_index


SimpleFileIndex.join = join


if __name__ == "__main__":
    import time
    from datalad_metalad.metadata_store.filestorage_backend import FileStorageBackend

    entries = 100 #1000000

    lios = SimpleFileIndex("/home/cristian/tmp/index_store_test/left", FileStorageBackend)
    try:
        lios.add_dataset_entry("/", {"dataset_info": "true"})
        lios.add_metadata_to_path(
            "/",
            'ng_dataset',
            bytearray('{content: "some ng dataset info"}', encoding='utf-8'))

        for i in range(entries):
            lios.add_file_entry(f"/e{i}")
            lios.add_metadata_to_path(f"/e{i}", "ng_file", bytearray(f"#{i}", encoding="utf-8"))
        lios.flush()
    except (PathAlreadyExists, MetadataAlreadyExists):
        print("sl seems to be set, skipping its creation")

    print(f"lios('/', 'ng_dataset'): {lios.get_metadata('/', 'ng_dataset')}")
    print(f"lios('/e10', 'ng_file'): {lios.get_metadata('/e10', 'ng_file')}")
    print(f"lios('/e20', 'ng_file'): {lios.get_metadata('/e20', 'ng_file')}")

    exit(0)
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

    print(f"combined_ios('left/e10'): {combined_ios.get_metadata('left/e10')}")
    print(f"combined_ios('right/e20'): {combined_ios.get_metadata('right/e20')}")

    for path, reader in combined_ios:
        print("+" * 20)
        print(path)
        for b in reader:
            print(b)

    print("XXXXX" * 20)
    for b in combined_ios.metadata_iterator("left/e19"):
        print(b)
