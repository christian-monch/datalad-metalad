

from datalad_metalad.metadata_store.simplefile_index import SimpleFileIndex
from datalad_metalad.metadata_store.exceptions import PathAlreadyExists, MetadataAlreadyExists


if __name__ == "__main__":
    import time
    from datalad_metalad.metadata_store.filestorage_backend import FileStorageBackend

    entries = 10000000

    lios = SimpleFileIndex("/home/cristian/tmp/index_store_test/left", FileStorageBackend)
    try:
        lios.set_dataset_entry("/", {"left side dataset meta-metadata": "true"})
        lios.add_metadata_to_path(
            "/",
            'ng_dataset',
            bytearray('{content: "left side ng dataset info"}', encoding='utf-8'))

        for i in range(entries):
            lios.add_path(f"/e{i}")
            lios.add_metadata_to_path(f"/e{i}", "ng_file", bytearray(f"left #{i}", encoding="utf-8"))
        lios.flush()
    except (PathAlreadyExists, MetadataAlreadyExists):
        print("sl seems to be set, skipping its creation")

    rios = SimpleFileIndex("/home/cristian/tmp/index_store_test/right", FileStorageBackend)
    try:
        rios.set_dataset_entry("/", {"right side dataset meta-metadata": "true"})
        rios.add_metadata_to_path(
            "/",
            'ng_dataset',
            bytearray('{content: "right side dataset info"}', encoding='utf-8'))

        for i in range(entries):
            rios.add_path(f"/e{i}")
            rios.add_metadata_to_path(f"/e{i}", "ng_file", bytearray(f"right #{i}", encoding="utf-8"))
        rios.flush()
    except (PathAlreadyExists, MetadataAlreadyExists):
        print("sr seems to be set, skipping its creation")

    start_time = time.time()

    combined_ios = SimpleFileIndex.join(
        "/home/cristian/tmp/index_store_test/joined",
        "/left", lios,
        "/right", rios)

    combine_time = time.time()
    print(f"duration of combine: {int(combine_time - start_time)}")

    combined_ios.flush()

    flush_time = time.time()
    print(f"duration of flush: {int(flush_time - combine_time)}")

    print(f"combined_ios('/left/e10'): {combined_ios.get_metadata('/left/e10', 'ng_file')}")
    print(f"combined_ios('/right/e20'): {combined_ios.get_metadata('/right/e20', 'ng_file')}")

    print(f"lios('/', 'ng_dataset'): {lios.get_metadata('/', 'ng_dataset')}")
    print(f"lios('/e10', 'ng_file'): {lios.get_metadata('/e10', 'ng_file')}")
    print(f"lios('/e20', 'ng_file'): {lios.get_metadata('/e20', 'ng_file')}")

    for path, metadata, reader in combined_ios:
        print("+" * 20)
        print(f"[{metadata}]: {path}")
        for b in reader:
            print(b)
        if path == "/left/e100":
            break

    print("XXXXX" * 20)
    for b in combined_ios.metadata_iterator("/left/e19", "ng_file"):
        print(b)
