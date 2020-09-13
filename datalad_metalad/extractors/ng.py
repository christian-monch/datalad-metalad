# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##


from .base import MetadataExtractor

from datalad_metalad.metadata_store.exceptions import PathAlreadyExists
from datalad_metalad.metadata_store.simplefile_index import SimpleFileIndex
from datalad_metalad.metadata_store.filestorage_backend import FileStorageBackend


class DataladNGExtractor(MetadataExtractor):

    def _get_metadata_dir(self):
        """
        Until we have modified the extractor, we hard-code an extractor for every extractor here.
        """
        return "/tmp/ng-test"

    def __call__(self, dataset, refcommit, process_type, status):

        metadata_store = SimpleFileIndex(self._get_metadata_dir(), FileStorageBackend)

        if process_type in ('all', 'content'):
            for entry in status:
                metadata_path = (
                    entry["path"][len(dataset.path):]
                    if entry["path"].startswith(dataset.path)
                    else entry["path"]
                )

                try:
                    metadata_store.add_content(
                        metadata_path,
                        bytearray(f'{{"type": "all+content#{entry["type"]}}}', encoding="utf-8"))
                except PathAlreadyExists:
                    print(f"NG: path: {entry['path']} already in metadata")

                yield {
                    "path": entry["path"],
                    "type": f"file",
                    "status": "ok",
                    "metadata": {
                        "entry_type": entry["type"],
                        "ng_was_here": True
                    }
                }

        if process_type in ('all', 'dataset'):
            try:
                metadata_store.add_content(
                    "/",
                    bytearray('{"type": "all+dataset#dataset"}', encoding="utf-8"))
            except PathAlreadyExists:
                print("NG: path: / already in metadata")

            yield {
                "type": f"dataset",
                "status": 'ok',
                "metadata": {
                    '@id': f"http://datalad.org/{repr(dataset)}"
                }
            }

        metadata_store.flush()

    def get_state(self, dataset):
        return {
            'version': "0.1",
            "info": "Test extractor for work on the NG-Interface",
        }
