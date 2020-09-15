import enum
import os
import time
import unittest
from tempfile import TemporaryDirectory
from typing import List

from datalad_metalad.metadata_store.simplefile_index import SimpleFileIndex
from datalad_metalad.metadata_store.filestorage_backend import FileStorageBackend


class StoreCommand(enum.Enum):
    ADD_FILE = 1
    ADD_DATASET = 1
    DELETE = 2


class InitializationCommand:
    def __init__(self, command: StoreCommand, path: str, metadata_format: str, value: bytearray = None):
        self.command = command
        self.path = path
        self.metadata_format = metadata_format
        self.value = value


class JoinTests(unittest.TestCase):

    TestFormat = "meta_test_format"

    # Create a store, but do not try to persist it.
    def _create_ephemeral_store(self, base_dir, initial_content=None):
        os.makedirs(base_dir, exist_ok=True)
        metadata_store = SimpleFileIndex(base_dir, FileStorageBackend)
        if initial_content:
            for path, value in initial_content.items():
                metadata_store.add_file_entry(path)
                metadata_store.add_metadata_to_path(path, self.TestFormat, value)
        return metadata_store

    # Create a persisted store with a given sequence of operations
    def _create_persisted_store(self, base_dir, initialization_commands: List[InitializationCommand]):
        os.makedirs(base_dir, exist_ok=True)
        metadata_store = SimpleFileIndex(base_dir, FileStorageBackend)
        for command in initialization_commands:
            if command.command == StoreCommand.ADD_FILE:
                metadata_store.add_file_entry(command.path)
                metadata_store.add_metadata_to_path(command.path, command.metadata_format, command.value)
            if command.command == StoreCommand.ADD_DATASET:
                metadata_store.add_dataset_entry(command.path)
                metadata_store.add_metadata_to_path(command.path, command.metadata_format, command.value)
            elif command.command == StoreCommand.DELETE:
                metadata_store.delete_metadata_from_path(command.path, command.metadata_format)
            else:
                raise ValueError("Unknown initializer command")

        metadata_store.flush()
        return metadata_store

    # Create a store on disk
    def _write_test_store(self, base_dir, initial_content=None):
        metadata_store = self._create_ephemeral_store(base_dir, initial_content)
        metadata_store.flush()
        return metadata_store

    def test_join(self):
        with TemporaryDirectory() as temp_dir:
            first_dir = os.path.join(temp_dir, "first")
            second_dir = os.path.join(temp_dir, "second")
            first_stamp = bytearray(f"first store time stamp: {time.time()}", encoding="utf-8")
            second_stamp = bytearray(f"second store time stamp: {time.time()}", encoding="utf-8")

            first_store = self._write_test_store(
                first_dir,
                {
                    "timestamp": first_stamp,
                    "p1": bytearray("a:1", encoding="utf-8"),
                    "p2": bytearray("a:22", encoding="utf-8"),
                    "p3": bytearray("a:333", encoding="utf-8")
                }
            )

            second_store = self._write_test_store(
                second_dir,
                {
                    "timestamp": second_stamp,
                    "p1": bytearray("b:1", encoding="utf-8"),
                    "p2": bytearray("b:22", encoding="utf-8"),
                    "p3": bytearray("b:333", encoding="utf-8")
                }
            )

            aggregate_dir = os.path.join(temp_dir, "aggregate")
            os.makedirs(aggregate_dir, exist_ok=True)
            aggregate_store: SimpleFileIndex = SimpleFileIndex.join(
                aggregate_dir,
                "first", first_store,
                "second", second_store)

            self.assertEqual(first_stamp, aggregate_store.get_metadata("first/timestamp", self.TestFormat))
            self.assertEqual(second_stamp, aggregate_store.get_metadata("second/timestamp", self.TestFormat))


if __name__ == '__main__':
    unittest.main()
