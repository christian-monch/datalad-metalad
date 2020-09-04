import enum
import os
import time
import unittest
from tempfile import TemporaryDirectory
from typing import List

from datalad_metalad.metadata_store.metadata_store import MetadataStore, join


class StoreCommand(enum.Enum):
    ADD = 1
    DELETE = 2


class InitializationCommand:
    def __init__(self, command: StoreCommand, path: str, value: bytes = None):
        self.command = command
        self.path = path
        self.value = value


class JoinTests(unittest.TestCase):

    # Create a store, but do not try to persist it.
    def _create_test_store(self, base_dir, initial_content=None):
        os.makedirs(base_dir, exist_ok=True)
        metadata_store = MetadataStore(base_dir)
        if initial_content:
            for key, value in initial_content.items():
                metadata_store.add_content(key, value)
        return metadata_store

    # Create a persisted store with a given sequence of operations
    def _create_used_store(self, base_dir, initialization_commands: List[InitializationCommand]):
        os.makedirs(base_dir, exist_ok=True)
        metadata_store = MetadataStore(base_dir)
        for command in initialization_commands:
            if command.command == StoreCommand.ADD:
                metadata_store.add_content(command.path, command.value)
            elif command.command == StoreCommand.DELETE:
                metadata_store.delete_content(command.path)
            else:
                raise ValueError("Unknown initializer command")

        metadata_store.flush()
        return metadata_store

    # Create a store on disk
    def _write_test_store(self, base_dir, initial_content=None):
        metadata_store = self._create_test_store(base_dir, initial_content)
        metadata_store.flush()
        return metadata_store

    def test_join(self):
        with TemporaryDirectory() as temp_dir:
            first_dir = os.path.join(temp_dir, "first")
            second_dir = os.path.join(temp_dir, "second")
            first_stamp = bytes(f"first store time stamp: {time.time()}", encoding="utf-8")
            second_stamp = bytes(f"second store time stamp: {time.time()}", encoding="utf-8")

            first_store = self._write_test_store(
                first_dir,
                {
                    "timestamp": first_stamp,
                    "p1": bytes("a:1", encoding="utf-8"),
                    "p2": bytes("a:22", encoding="utf-8"),
                    "p3": bytes("a:333", encoding="utf-8")
                }
            )
            second_store = self._write_test_store(
                second_dir,
                {
                    "timestamp": second_stamp,
                    "p1": bytes("b:1", encoding="utf-8"),
                    "p2": bytes("b:22", encoding="utf-8"),
                    "p3": bytes("b:333", encoding="utf-8")
                }
            )

            aggregate_dir = os.path.join(temp_dir, "aggregate")
            os.makedirs(aggregate_dir, exist_ok=True)
            aggregate_store = join(
                aggregate_dir,
                first_store, "first",
                second_store, "second"
            )

            self.assertEqual(first_stamp, aggregate_store.get_content("first/timestamp"))
            self.assertEqual(second_stamp, aggregate_store.get_content("second/timestamp"))


if __name__ == '__main__':
    unittest.main()
