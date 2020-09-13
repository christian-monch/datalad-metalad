# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Metadata extractor for Datalad's own core storage"""

# TODO dataset metadata
# - known annex UUIDs
# - avoid anything that is specific to a local clone
#   (repo mode, etc.) limit to description of dataset(-network)

from .base import MetadataExtractor


class DataladNGExtractor(MetadataExtractor):

    def _get_metadata_dir(self):
        """
        Until we have modified the extractor, we hard-code an extractor for every extractor here.
        """
        pass

    def __call__(self, dataset, refcommit, process_type, status):
        if process_type in ('all', 'content'):
            for entry in status:
                yield {
                    "path": entry["path"],
                    "type": f"{process_type} + content",
                    "status": "ok",
                    "metadata": {
                        "entry_type": entry["type"],
                        "ng_was_here": True
                    }
                }

        if process_type in ('all', 'dataset'):
            yield {
                "type": f"{process_type} + dataset",
                "status": 'ok',
                "metadata": {
                    '@id': f"http://datalad.org/{repr(dataset)}"
                }
            }

    def get_state(self, dataset):
        return {
            'version': "0.1",
            "info": "Test extractor for work on the NG-Interface",
        }
