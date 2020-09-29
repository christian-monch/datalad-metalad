# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Metadata extractor for studyminimeta metadata contained in a dataset

The metadata source file can be specified via the
'datalad.metadata.studyminimeta-source' configuration variable.
The content of the file must be a JSON object that conforms to the
studyminimeta metadata schema. It should be serialized in YAML.

By default the following file is read: '.studyminimeta.yaml'
"""

import logging

import yaml

from datalad.log import log_progress
from ..base import MetadataExtractor
from .ld_creator import LDCreator


lgr = logging.getLogger('datalad.metadata.extractors.studyminimeta')


class StudyMiniMetaExtractor(MetadataExtractor):
    def get_required_content(self, dataset, process_type, status):
        for processed_status in status:
            if processed_status['path'].endswith('.studyminimeta.yaml'):
                yield processed_status

    @staticmethod
    def _get_relative_studyminimeta_file_name(dataset) -> str:
        return dataset.config.obtain('datalad.metadata.studyminimeta-source', '.studyminimeta.yaml')

    @staticmethod
    def _get_absolute_studyminimeta_file_name(dataset) -> str:
        return str(dataset.pathobj / StudyMiniMetaExtractor._get_relative_studyminimeta_file_name(dataset))

    def __call__(self, dataset, refcommit, process_type, status):
        if process_type not in ('all', 'dataset'):
            return None
        ds = dataset
        log_progress(
            lgr.info,
            'extractorstudyminimeta',
            'Start studyminimeta metadata extraction from {path}'.format(path=ds.path),
            total=len(tuple(status)) + 1,
            label='Studyminimeta metadata extraction',
            unit=' Files',
        )

        source_file = self._get_absolute_studyminimeta_file_name(dataset)
        try:
            with open(source_file, "rt") as input_stream:
                metadata_object = yaml.safe_load(input_stream)
        except FileNotFoundError:
            yield {
                "status": "ok",
                "metadata": {},
                "type": "dataset"
            }
            return

        if not isinstance(metadata_object, dict):
            yield {
                "status": "error",
                "metadata": None,
                "type": "dataset",
                'message': "Content of metadata-file is not valid"
            }
            return

        ld_creator_result = LDCreator(
            dataset.id,
            refcommit,
            self._get_relative_studyminimeta_file_name(dataset)).create_ld_from_spec(metadata_object)

        if ld_creator_result.success:
            log_progress(
                lgr.info,
                'extractorstudyminimeta',
                'Finished studyminimeta metadata extraction from {path}'.format(path=ds.path)
            )
            yield {
                "status": "ok",
                "metadata": ld_creator_result.json_ld_object,
                "type": "dataset"
            }

        else:
            log_progress(
                lgr.error,
                'extractorstudyminimeta',
                'Error in studyminimeta metadata extraction from {path}'.format(path=ds.path)
            )
            yield {
                "status": "failed",
                "metadata": {},
                "type": "dataset"
            }

    def get_state(self, dataset):
        return {"version": "0.1"}
