"""
Converter from old style metadata to new generation metadata
"""
import json
import lzma
from argparse import ArgumentParser
from pathlib import PosixPath
from typing import Dict, List, Union

from datalad_metalad.metadata_store.fileindex import FileIndex
from datalad_metalad.metadata_store.filestorage_backend import FileStorageBackend
from datalad_metalad.metadata_store.simplefile_index import SimpleFileIndex


JSONObject = Union[Dict, List]


PARSER = ArgumentParser()
PARSER.add_argument("dataset_root", type=str)


def open_file(file_name: PosixPath):
    if file_name.suffix == ".xz":
        return lzma.open(str(file_name), mode="rt")
    return file_name.open("rt")


def read_json_object_from_file(file_name: PosixPath) -> JSONObject:
    f = open_file(file_name)
    json_object = json.load(f)
    f.close()
    return json_object


def read_json_object_from_line_file(file_name: PosixPath) -> JSONObject:
    json_object = []
    f = open_file(file_name)
    for line in f.readlines():
        json_object.append(json.loads(line))
    f.close()
    return json_object


def add_dataset_path(metadata_store: FileIndex, path: str, metadata_file_name: PosixPath):
    metadata_store.add_path(path)
    metadata_elements = read_json_object_from_file(metadata_file_name)
    for metadata_format, metadata_json_object in metadata_elements.items():
        metadata_store.add_metadata_to_path(
            path,
            metadata_format,
            bytearray(json.dumps(metadata_json_object), encoding="utf-8")
        )


def join_paths(left: str, right: str):
    if left:
        return left.rstrip("/") + "/" + right.lstrip("/")
    return right


def add_content_path(metadata_store: FileIndex, metadata_file_name: PosixPath, path_prefix: str):
    file_entries = read_json_object_from_line_file(metadata_file_name)
    print(file_entries)
    for file_entry in file_entries:
        path = join_paths(path_prefix, file_entry["path"])
        metadata_store.add_path(path)
        for metadata_format, metadata in file_entry.items():
            if metadata_format == "path":
                continue
            metadata_store.add_metadata_to_path(
                path,
                metadata_format,
                bytearray(json.dumps(metadata), encoding="utf-8")
            )


def add_dataset(metadata_store: FileIndex, aggregate: JSONObject, metadata_dir: PosixPath):
    for path, meta_metadata in aggregate.items():
        print(path, meta_metadata)
        add_dataset_path(metadata_store, path, metadata_dir / meta_metadata["dataset_info"])
        metadata_store.set_dataset_entry(
            path,
            {
                key: value
                for key, value in meta_metadata.items()
                if key not in ("content_info", "dataset_info")
            }
        )
        if "content_info" in meta_metadata:
            add_content_path(metadata_store, metadata_dir / meta_metadata["content_info"], path)


def create_metadata_store(metadata_dir: PosixPath):
    return SimpleFileIndex(str(metadata_dir / "ng"), FileStorageBackend)


def convert_os2ng(root_dir: PosixPath):
    metadata_dir = root_dir / ".datalad/metadata"
    metadata_store = create_metadata_store(metadata_dir)
    aggregate = json.load((metadata_dir / "aggregate_v1.json").open("rt"))
    add_dataset(metadata_store, aggregate, metadata_dir)
    metadata_store.flush()


def main():
    arguments = PARSER.parse_args()
    convert_os2ng(PosixPath(arguments.dataset_root))


if __name__ == "__main__":
    main()
