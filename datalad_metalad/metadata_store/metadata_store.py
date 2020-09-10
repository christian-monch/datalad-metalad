"""

This defines the Metadata Store ADT.
A Metadata Store stores metadata indexed by
metadata format-name and a path.

The implementation is meant to be:

1. Space efficient (compressed storage)
2. I-node efficient (small number of files)
3. Fast, i. e. random access to metadata should be fast
4. Quickly aggregatable, i. e. joining two Metadata Stores should be efficient


create(directory) -> MDS
add(MDS, path, metadata-format, metadata) -> None
remove(MDS, path, metadata-format, metadata) -> None
join(directory, LeftMDS, leftprefix, rightMDS, right_prefix) -> MDS



a: /data/image1.png
a md1 index:
/ -> 1000
/data/image1.png -> 2000


b: /data/image2.png
b md1 index:
/ -> 1500
/data/image1.png -> 3500



composed in c:
c/sub1/a/
c/sub2/b/

c md1 index:
/ -> 100
/c/sub1/a + /  ->  1000 + sizeof(index(c))
/c/sub1/a + /data/image1.png -> 2000 + sizeof(index(c))
/c/sub2/b + / -> 1500 + sizeof(index(c)) + sizeof(index(a))
/c/sub2/b + /data/image1.png -> 3500 + sizeof(index(c)) + sizeof(index(a))





"""
import json
import logging
import os
import subprocess
from collections import namedtuple
from itertools import chain
from pathlib import PosixPath
from typing import Dict, List, Union

from exceptions import PathAlreadyExists


LOGGER = logging.getLogger("metadata_store")


ContentEntry = namedtuple("ContentEntry", ("file_number", "offset", "size"))


class MetadataStore(object):
    """
    Implementation of the Metadata Store ADT.

    We assume that path names are longer than format names and
    that metadata format names are in the hundreds, while path
    names are in the millions (for optimization considerations)
    """

    IndexFileName = "index.json"
    IndexFileVersion = "0.9"
    MaximumContentSize = 8 * 1024 * 1024 * 1024

    def __init__(self, storage_dir: str):
        self.storage_dir = PosixPath(storage_dir)

        self.current_file_index = 0
        self.current_offset = 0
        self.paths: Dict[str, ContentEntry] = {}
        self.deleted_paths: List[ContentEntry, ...] = []
        self.content = bytearray()
        self.content_modified = False
        self.index_modified = False

        try:
            self._read_index()
            self.content = self._read_content(self.current_file_index)
        except FileNotFoundError:
            print("could not load")
            pass

    def __contains__(self, path: str):
        return path in self.paths

    def __len__(self):
        return len(self.paths)

    @classmethod
    def set_maximum_content_size(cls, size: int):
        cls.MaximumContentSize = size

    def _current_content_file_name(self) -> PosixPath:
        return self._content_file_name(self.current_file_index)

    def _content_file_name(self, content_file_index) -> PosixPath:
        return self.storage_dir / f"content_{content_file_index:03}"

    def _index_file_name(self) -> PosixPath:
        return self.storage_dir / f"{self.IndexFileName}"

    def _add_content_to_current_file(self, path: str, content: bytearray):
        self.paths[path] = ContentEntry(self.current_file_index, self.current_offset, len(content))
        self.current_offset += len(content)
        self.content += content
        self.content_modified = True
        self.index_modified = True

    def _persist_index(self):
        with open(str(self._index_file_name()), "tw") as file:
            file.write(json.dumps({
                "version": self.IndexFileVersion,
                "file_number": self.current_file_index,
                "offset": self.current_offset,
                "paths": self.paths,
                "deleted_paths": self.deleted_paths
            }))

    def _persist_content(self):
        with open(str(self._current_content_file_name()), "bw") as file:
            file.write(self.content)

    def _read_index(self):
        with open(str(self._index_file_name()), "tr") as file:
            index_info = json.load(file)
        index_file_version = index_info.get("version", "0.0")
        if index_file_version != self.IndexFileVersion:
            raise ValueError(
                f"index file version {index_file_version} does not "
                f"match code version {self.IndexFileVersion}")

        self.current_file_index = index_info["file_number"]
        self.current_offset = index_info["offset"]
        self.paths = {
            path: ContentEntry(entry_list[0], entry_list[1], entry_list[2])
            for path, entry_list in index_info["paths"].items()
        }
        self.deleted_paths = [
            ContentEntry(entry_list[0], entry_list[1], entry_list[2])
            for entry_list in index_info["deleted_paths"]
        ]

    def _read_content(self, content_file_index) -> bytearray:
        with open(str(self._content_file_name(content_file_index)), "br") as file:
            return bytearray(file.read())

    def _next_content_file(self):
        self.current_file_index += 1
        self.current_offset = 0
        self.content = bytearray()
        self.index_modified = True

    def add_content(self, path: str, content: bytearray):
        if path in self.paths:
            raise PathAlreadyExists(f"{path} already in indexed object store")

        if self.current_offset > 0 and self.current_offset + len(content) > self.MaximumContentSize:
            self._persist_content()
            self._next_content_file()
        self._add_content_to_current_file(path, content)

    def delete_content(self, path: str):
        if path not in self.paths:
            raise KeyError(f"{path} not in indexed object store")
        self.deleted_paths.append(self.paths[path])
        del self.paths[path]
        self.index_modified = True

    def replace_content(self, path: str, content: bytearray):
        self.delete_content(path)
        self.add_content(path, content)

    def get_content(self, path: str) -> Union[None, bytearray]:
        if path not in self.paths:
            return None

        path_entry = self.paths[path]
        content = self._read_content(path_entry.file_number)
        return content[path_entry.offset:path_entry.offset + path_entry.size]

    def flush(self):
        if self.content_modified:
            self._persist_content()
        if self.index_modified:
            self._persist_index()


def file_size(path: str) -> int:
    return os.stat(path).st_size


def join_files(first: str, second: str, result: str):
    command = f"cat {first} {second} > {result}"
    print(f"executing command: {command}")
    subprocess.run(command, shell=True)


def copy_file(source: str, destination: str):
    command = f"cp {source} {destination}"
    print(f"executing command: {command}")
    subprocess.run(command, shell=True)


def join(result_storage_dir: str,
         left_store: MetadataStore,
         left_prefix: str,
         right_store: MetadataStore,
         right_prefix: str) -> MetadataStore:

    """
    Join two MetadataStore objects.
    All but the last indexed file get copied to the new
    location, since they have reached their maximum size.

    The last two files are combined, and probably split
    up, if the combined content file exceeds the file size
    limit.

    Parameters
    ----------
    result_storage_dir
    left_store
    left_prefix
    right_store
    right_prefix

    Returns
    -------

    """

    assert left_prefix != right_prefix

    left_file_number = left_store.current_file_index + 1
    right_file_number = right_store.current_file_index + 1

    left_current_content_size = file_size(str(left_store._current_content_file_name()))
    right_current_content_size = file_size(str(right_store._current_content_file_name()))

    new_store = MetadataStore(result_storage_dir)

    # Join the content
    combined_file_size = (
        file_size(str(left_store._current_content_file_name()))
        +
        file_size(str(right_store._current_content_file_name())))

    if combined_file_size < MetadataStore.MaximumContentSize:

        combined_current_content_index = left_file_number + right_file_number - 2
        left_content_index_mapping = tuple(chain(
            range(left_file_number - 1),
            [combined_current_content_index]
        ))
        right_content_index_mapping = tuple(range(left_file_number - 1, left_file_number - 1 + right_file_number))

        # join the two current content files.
        join_files(
            str(left_store._current_content_file_name()),
            str(right_store._current_content_file_name()),
            str(new_store._content_file_name(combined_current_content_index)))

        # copy content files from left store
        for index in range(left_file_number - 1):
            copy_file(
                str(left_store._content_file_name(index)),
                str(new_store._content_file_name(index))
            )

        # copy content files from right store
        for index in range(right_file_number - 1):
            copy_file(
                str(right_store._content_file_name(index)),
                str(new_store._content_file_name(index + left_file_number - 1))
            )

        # remap content file and byte indices of the left store
        updated_left_paths = {
            f"{left_prefix}/{path}": ContentEntry(
                left_content_index_mapping[path_entry.file_number], path_entry.offset, path_entry.size)
            for path, path_entry in left_store.paths.items()
        }

        # remap content file and byte indices of the right store
        updated_right_paths = {
            f"{right_prefix}/{path}": (
                ContentEntry(right_content_index_mapping[path_entry.file_number], path_entry.offset, path_entry.size)
                if path_entry.file_number < right_store.current_file_index
                else ContentEntry(
                    right_content_index_mapping[path_entry.file_number],
                    path_entry.offset + left_store.current_offset,
                    path_entry.size))
            for path, path_entry in right_store.paths.items()
        }

        # Remap deleted paths of left store
        updated_left_deleted_paths = [
            ContentEntry(left_content_index_mapping[path_entry.file_number], path_entry.offset, path_entry.size)
            for path_entry in left_store.deleted_paths
        ]

        # Remap deleted paths of right store
        updated_right_deleted_paths = [
            (
                ContentEntry(right_content_index_mapping[path_entry.file_number], path_entry.offset, path_entry.size)
                if path_entry.file_number < right_store.current_file_index
                else ContentEntry(
                    right_content_index_mapping[path_entry.file_number],
                    path_entry.offset + left_store.current_offset,
                    path_entry.size)
            )
            for path_entry in right_store.deleted_paths]

        new_store.paths = {
            **updated_left_paths,
            **updated_right_paths
        }
        new_store.current_file_index = left_file_number + right_file_number - 2
        new_store.current_offset = left_store.current_offset + right_store.current_offset
        new_store.deleted_paths = updated_left_deleted_paths + updated_right_deleted_paths
        new_store.modified = False
        new_store.content_modified = False
        new_store.index_modified = True

    else:
        # Keep all content file separated
        raise NotImplemented()

    new_store.flush()
    return new_store


if __name__ == "__main__":
    import time

    lios = MetadataStore("/home/cristian/tmp/index_store_test/sl")
    try:
        for i in range(10000000):
            lios.add_content(f"e{i}", bytearray(f"#{i}", encoding="utf-8"))
        lios.flush()
    except PathAlreadyExists:
        print("sl seems to be set, skipping its creation")

    rios = MetadataStore("/home/cristian/tmp/index_store_test/sr")
    try:
        for i in range(10000000):
            rios.add_content(f"e{i}", bytearray(f"#{i}", encoding="utf-8"))
        rios.flush()
    except PathAlreadyExists:
        print("sr seems to be set, skipping its creation")

    start_time = time.time()

    combined_ios = join(
        "/home/cristian/tmp/index_store_test/scombined",
        lios, "left",
        rios, "right"
    )

    combine_time = time.time()
    print(f"duration of combine: {int(combine_time - start_time)}")

    combined_ios.flush()

    flush_time = time.time()
    print(f"duration of flush: {int(flush_time - combine_time)}")

    exit(0)
    # Keep all content file separated

    rios = MetadataStore("/home/cristian/tmp/index_store_test/sr")
    test_content = bytearray(f"Zeit: {time.time()}", encoding="utf-8")
    if "a" not in rios:
        rios.add_content("a", test_content)
        rios.flush()
    else:
        rios.replace_content("a", test_content)
        rios.flush()
    test_content = rios.get_content("a")
    print(f"rios: {test_content}")


    print("----------------")
    print(lios.paths)
    print(lios.deleted_paths)
    print(lios.current_file_index)
    print(lios.current_offset)

    print("----------------")
    print(rios.paths)
    print(rios.deleted_paths)
    print(rios.current_file_index)
    print(rios.current_offset)

    print("----------------")
    print(combined_ios.paths)
    print(combined_ios.deleted_paths)
    print(combined_ios.current_file_index)
    print(combined_ios.current_offset)

    combined_ios.flush()

    print(f"combined_ios('left/a'): {combined_ios.get_content('left/a')}")
    print(f"combined_ios('right/a'): {combined_ios.get_content('right/a')}")
