

from metadata_store import MetadataStore
from exceptions import PathAlreadyExists


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

    print(f"combined_ios('left/a'): {combined_ios.get_metadata('left/a')}")
    print(f"combined_ios('right/a'): {combined_ios.get_metadata('right/a')}")
