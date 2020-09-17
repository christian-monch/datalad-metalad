"""
Read basic data from a given metadata index
"""
from argparse import ArgumentParser
from itertools import islice

from datalad_metalad.metadata_store.filestorage_backend import FileStorageBackend
from datalad_metalad.metadata_store.simplefile_index import SimpleFileIndex


PARSER = ArgumentParser()
PARSER.add_argument("index_dir", type=str)
PARSER.add_argument("--show-metadata-content", action="store_true", default=False)


def create_metadata_store(index_dir: str):
    return SimpleFileIndex(index_dir, FileStorageBackend)


def main():
    arguments = PARSER.parse_args()

    metadata_store = create_metadata_store(arguments.index_dir)
    print(f"# path entries: {len(metadata_store)}")

    current_path = None
    for path, is_dataset, metadata_format, reader in metadata_store:
        if path != current_path:
            print(f"\"{path}\"{': (dataset root)' if is_dataset else ':'}")
            current_path = path
        if arguments.show_metadata_content is True:
            metadata = "".join([chr(content_part[0]) for content_part in zip(reader, [0])])
            print(f"  metadata: {metadata_format}: {repr(metadata)}")
        else:
            print(f"  metadata: {metadata_format}")


if __name__ == "__main__":
    main()
