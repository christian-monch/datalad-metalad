# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Metadata extractor for dataset information stored in Datalad's own core storage"""
import logging
from uuid import UUID

from .base import DataOutputCategory, ExtractorResult, DatasetMetadataExtractor
from datalad.log import log_progress


lgr = logging.getLogger('datalad.metadata.extractors.metalad_core_dataset')


class DataladCoreDatasetExtractor(DatasetMetadataExtractor):

    def get_id(self) -> UUID:
        return UUID("b3c487ea-e670-4801-bcdc-29639bf1269b")

    def get_version(self) -> str:
        return "0.0.1"

    def get_data_output_category(self) -> DataOutputCategory:
        return DataOutputCategory.IMMEDIATE

    def get_required_content(self) -> bool:
        return True

    def extract(self, _=None) -> ExtractorResult:
        log_progress(
            lgr.info,
            "datalad_core_dataset_extractor",
            "Running core dataset extraction for uuid:%s, tree:%s",
            self.dataset.id,
            self.dataset.path,
            label="Core dataset metadata extraction")

        return ExtractorResult(
            extractor_version=self.get_version(),
            extraction_parameter=self.parameter or {},
            extraction_success=True,
            datalad_result_dict={
                "type": "dataset",
                "status": "ok"
            },
            immediate_data={
                "id": self.dataset.id,
                "refcommit": self.dataset.repo.get_hexsha(),
                "path": self.dataset.path,
                "comment": "test-implementation"
            })
