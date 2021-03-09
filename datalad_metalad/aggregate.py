# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""
Interface for aggregating metadata from (sub)dataset into (super)datasets


Aggregating a subdataset (sds) into the UUID set of the root dataset (rds)
is relatively simple. (There is a possible error condition, where different UUIDS would
be added to the same rds-pd-version at the same path).


Assumption:

The sds are real sub-datasets of the rds



Issue 1: path determination
----------------------------

For UUID-set aggregation and for tree-version-list aggregation, the new path
of the sds metadata has to be determined.

UUIDSet:

     UUID    ---connected to--> [{sds-pd-version, path, metadata-root-record}, ...]

Since the sds UUID-set metadata has no path yet in the aggregated rds metadata,
the path has to be determined. For a given uuid and a given rds, with metadata
md(rds) all possible paths would be:

    relevant-rds-versions = for all versions(rds) where sds-version in [sds-pd-version of uuid]

    for rds-version in relevant-rds-versions:
        for sds-pd-version, _, metadata-root-record in version-list(uuid):
            if sds-pd-version is sub-dataset of rds-version:
                sds-path = path of sds-pd-version in rds-version
                add metadata_root_record to uuid-set(rds).sds-pd-version, sds-path
            else:
                Error("Cannot find path of sds-uuid@sds-pd-version in any rds@version)
                Error("What can you do? Not much besides re-aggregating")
                Error("What can we do? Add a structure that allows for 'detached' metadata")

"""
import logging
import time
from itertools import islice
from pathlib import PosixPath
from typing import List, Tuple

import dataclasses

from datalad.interface.base import Interface
from datalad.interface.utils import (
    eval_results
)
from datalad.interface.base import build_doc
from datalad.interface.common_opts import (
    recursion_limit as default_recursion_limit,
    recursion_flag
)
from datalad.distribution.dataset import (
    datasetmethod
)
from datalad.support.param import Parameter
from datalad.support.constraints import (
    EnsureStr,
    EnsureNone
)
from datalad.support.constraints import EnsureChoice
from dataladmetadatamodel.datasettree import DatasetTree
from dataladmetadatamodel.uuidset import UUIDSet
from dataladmetadatamodel.versionlist import TreeVersionList
from dataladmetadatamodel.mapper.gitmapper.objectreference import flush_object_references
from dataladmetadatamodel.mapper.gitmapper.utils import lock_backend, unlock_backend
from .metadata import get_top_level_metadata_objects


__docformat__ = 'restructuredtext'

lgr = logging.getLogger('datalad.metadata.aggregate')


@dataclasses.dataclass
class AggregateItem:
    source_tree_version_list: TreeVersionList
    source_uuid_set: UUIDSet
    destination_path: str


@build_doc
class Aggregate(Interface):
    """Aggregate metadata of one or more sub-datasets for later reporting.

    <REMARK>
    Metadata storage is not forced to reside inside the dataset repository.
    Metadata might be stored within the repository that is used by a
    dataset, but it might as well be stored in another repository (or
    a non-git backend, once those exist). To distinguish the metadata storage
    from the dataset storage, we refer to the metadata storage as realm
    (which for now is only a git-repository). For now, the realm for metadata
    is usually the git-repository that holds the dataset.

    The distinction is the reason for the "double"-path arguments below.
    for each metadata realm that should be integrated into the root realm,
    we have to give the metadata realm itself and the intra-dataset-path
    with regard to the root-repository.
    </REMARK>

    Metadata aggregation refers to a procedure that combines metadata from
    different sub-datasets into a root dataset, i.e. a dataset that contains
    all the sub-datasets. Aggregated metadata is "prefixed" with the
    intra-dataset-paths of the sub-datasets. The intra-dataset-path for a
    sub-dataset is the path from the top-level directory of the root dataset,
    i.e. the directory that contains the ".datalad"-entry, to the top-level
    directory of the respective sub-dataset.

    Aggregate assumes that metadata exists. To create metadata, use the
    meta-extract command.

    As a result of the aggregation, the metadata of all specified sub-datasets
    will be available in the root metadata realm. A datalad meta-dump command
    on the root-realm will therefore be able to consider all metadata from
    the root dataset and all aggregated sub-datasets.


    NB! recursive aggregation is not implemented yet! Therefore the following
    text might NOT be true.

    Metadata aggregation can be performed recursively, in order to aggregate
    all metadata from all subdatasets. By default, re-aggregation of metadata
    inspects modifications of datasets and metadata extractor parameterization
    with respect to the last aggregated state. For performance reasons,
    re-aggregation will be automatically skipped, if no relevant change is
    detected. This default behavior can be altered via the ``--force``
    argument.
    """
    _params_ = dict(
        backend=Parameter(
            args=("--backend",),
            metavar="BACKEND",
            doc="""metadata storage backend to be used. Currently only
            "git" is supported.""",
            constraints=EnsureChoice("git")),
        rootrealm=Parameter(
            args=("rootrealm",),
            metavar="ROOT_REALM",
            doc="""Realm where metadata will be aggregated into, this
                   is also interpreted as root dataset."""),
        path=Parameter(
            args=("path",),
            metavar="PATH",
            doc="""
            If -s, --separatepaths is defined (currently it IS
            defined by default and cannot be change), the realm
            that store metadata and the intra-dataset path for the
            sub-dataset, whose metadata is stored in realm, are given
            separately. Consequently path is interpreted a list of
            pairs of:
            
            1. Intra dataset path of sub-dataset whose metadata is stored
               in the METADATA_REALM.
            2. Location of METADATA_REALM, i.e. metadata repository-path

            For example, if the root dataset  path is "root_ds", and the
            metadata of the root is located in "/metadata/root_ds", and
            the sub-dataset we want to aggregate into the root dataset
            is located in "root_ds/sub_ds1/sub_ds2", with its metadata
            stored in the realm "/metadata/sub_ds2", we have to 
            give the following command to aggregate the metadata of
            sub_ds2 into the root dataset::
            
               datalad meta-extract /metadata/root_ds sub_ds1/sub_ds2 /metadata/sub_ds2
                        
            """,
            nargs="*",
            constraints=EnsureStr() | EnsureNone()),
        recursive=recursion_flag,
        recursion_limit=default_recursion_limit)

    @staticmethod
    @datasetmethod(name='meta_aggregate')
    @eval_results
    def __call__(
            backend="git",
            rootrealm=None,     # TODO: rename to root_realm
            path=None,
            recursive=False,
            recursion_limit=None):

        separate_paths = True
        if separate_paths is True:
            path_realm_associations = process_separated_path_spec(path)
        else:
            path_realm_associations = process_congruent_path_spec(path)

        root_realm = rootrealm or "."

        # TODO: we should read-lock all ag_realms
        # Collect aggregate information
        aggregate_items = []
        for ag_path, ag_realm in path_realm_associations:

            ag_tree_version_list, ag_uuid_set = get_top_level_metadata_objects(
                backend,
                ag_realm)

            if ag_tree_version_list is None or ag_uuid_set is None:
                message = f"No {backend}-mapped datalad metadata model found in: {ag_realm}, " \
                          f"ignoring metadata location {ag_realm} (and sub-dataset {ag_path})."
                lgr.warning(message)
                yield dict(
                    backend=backend,
                    realm=root_realm,
                    status='error',
                    message=message)
                continue

            aggregate_items.append(
                AggregateItem(
                    ag_tree_version_list,
                    ag_uuid_set,
                    ag_path))

        lock_backend(rootrealm)

        tree_version_list, uuid_set = get_top_level_metadata_objects(backend, root_realm)
        if tree_version_list is None:
            lgr.warning(
                f"no tree version list found in {root_realm}, "
                f"creating an empty tree version list")
            tree_version_list = TreeVersionList(backend, root_realm)
        if uuid_set is None:
            lgr.warning(
                f"no uuid set found in {root_realm}, "
                f"creating an empty set")
            uuid_set = UUIDSet(backend, root_realm)

        perform_aggregation(
            root_realm,
            tree_version_list,
            uuid_set,
            aggregate_items)

        tree_version_list.save()
        uuid_set.save()
        flush_object_references(root_realm)

        unlock_backend(root_realm)

        yield dict(
            action="meta_aggregate",
            backend=backend,
            realm=root_realm,
            status='ok',
            message="aggregation performed")

        return


def process_separated_path_spec(paths: List[str]
                                ) -> Tuple[Tuple[str, str], ...]:
    if len(paths) % 2 != 0:
        raise ValueError(
            "You must provide the same number of "
            "intra-dataset-paths and realms")
    return tuple(
        zip(
            islice(paths, 0, len(paths), 2),
            islice(paths, 1, len(paths), 2)))


def process_congruent_path_spec(paths: List[str]
                                ) -> Tuple[Tuple[str, str], ...]:
    raise NotImplementedError


def perform_aggregation(destination_realm: str,
                        tree_version_list: TreeVersionList,
                        destination_uuid_set: UUIDSet,
                        aggregate_items: List[AggregateItem]
                        ):

    for aggregate_item in aggregate_items:
        copy_uuid_set(
            destination_realm,
            destination_uuid_set,
            aggregate_item.source_uuid_set,
            aggregate_item.destination_path)

        copy_tree_version_list(
            destination_realm,
            tree_version_list,
            aggregate_item.source_tree_version_list,
            aggregate_item.destination_path)


def copy_uuid_set(destination_realm: str,
                  destination_uuid_set: UUIDSet,
                  source_uuid_set: UUIDSet,
                  destination_path: str):

    """
    For each uuid in the source uuid set, create a version
    list in the destination uuid set, if it does not yet exist
    and copy the metadata for all versions into the version list.

    :param destination_realm: realm of the uuid set
    :param destination_uuid_set: uuid set that should be updated
    :param source_uuid_set: uuid set the should be copied into the new set
    :param destination_path: the path under which the source uuid set should appear
    """

    # For every uuid in the source uuid set get the source version list
    for uuid in source_uuid_set.uuids():

        lgr.debug(f"aggregating metadata of dataset UUID: {uuid}")

        src_version_list = source_uuid_set.get_version_list(uuid)

        # If the destination does not contain a version list for the
        # source UUID, we add a copy of the source version list with
        # a the specified path prefix
        if uuid not in destination_uuid_set.uuids():

            lgr.debug(f"no version list for UUID: {uuid} in dest, creating it, by copying the source version list")
            destination_uuid_set.set_version_list(
                uuid,
                src_version_list.deepcopy(
                    new_realm=destination_realm,
                    path_prefix=destination_path))

        else:

            # Get the destination version list
            lgr.debug(f"updating destination version list for UUID: {uuid}")
            dest_version_list = destination_uuid_set.get_version_list(uuid)

            # Copy the individual version elements from source to destination.
            for pd_version in src_version_list.versions():

                lgr.debug(f"reading metadata element for pd version {pd_version} of UUID: {uuid}")
                time_stamp, old_path, element = src_version_list.get_versioned_element(pd_version)

                new_path = destination_path + (
                    "/"
                    if (destination_path != "" and old_path != "")
                    else "") + old_path

                lgr.debug(f"adding version {pd_version} with path {new_path} to UUID: {uuid}")
                dest_version_list.set_versioned_element(
                    pd_version,
                    time_stamp,
                    new_path,
                    element.deepcopy(new_realm=destination_realm)
                )

                # Unget the versioned element
                lgr.debug(f"persisting copied metadata element for pd version {pd_version} of UUID: {uuid}")
                dest_version_list.unget_versioned_element(pd_version)

                # Remove the source versioned element from memory
                lgr.debug(f"purging source metadata element for pd version {pd_version} of UUID: {uuid}")
                src_version_list.unget_versioned_element(pd_version)

            # Unget the version list in the destination, that should persist it.
            lgr.debug(f"persisting copied version list for UUID: {uuid}")
            destination_uuid_set.unget_version_list(uuid)

        # Remove the version list from memory
        lgr.debug(f"purging source version list for UUID: {uuid}")
        source_uuid_set.unget_version_list(uuid)


def copy_tree_version_list(destination_realm: str,
                           destination_tree_version_list: TreeVersionList,
                           source_tree_version_list: TreeVersionList,
                           destination_path: str):
    """
    Determine the root-dataset version, that is, the root
    version the contains the source version.
    Copy the dataset tree to the root dataset tree in the
    given destination path.
    """
    for source_pd_version in source_tree_version_list.versions():

        for root_pd_version in get_root_version_for_subset_version(
                destination_realm,
                source_pd_version,
                destination_path):

            if root_pd_version in destination_tree_version_list.versions():
                lgr.debug(
                    f"reading root dataset tree for version "
                    f"{root_pd_version}")

                _, root_dataset_tree = \
                    destination_tree_version_list.get_dataset_tree(
                        root_pd_version)
            else:
                lgr.debug(
                    f"creating new root dataset tree for version "
                    f"{root_pd_version}")
                root_dataset_tree = DatasetTree("git", destination_realm)

            time_stamp, source_dataset_tree = \
                source_tree_version_list.get_dataset_tree(source_pd_version)

            if destination_path in root_dataset_tree:
                lgr.warning(
                    f"replacing subtree {destination_path} for root dataset "
                    f" version {root_pd_version}")
                root_dataset_tree.delete_subtree(destination_path)

            root_dataset_tree.add_subtree(
                source_dataset_tree.deepcopy("git", destination_realm),
                destination_path)

            destination_tree_version_list.set_dataset_tree(
                root_pd_version,
                str(time.time()),
                root_dataset_tree)

            # Remove the trees from memory
            destination_tree_version_list.unget_dataset_tree(
                root_pd_version)
            source_tree_version_list.unget_dataset_tree(
                source_pd_version)

    return


# TODO: this function should check all branches of all
#  parent repositories, because more than one version of
#  the root repository might contain the given path.
def get_root_version_for_subset_version(root_dataset_path: str,
                                        sub_dataset_version: str,
                                        sub_dataset_path: str
                                        ) -> List[str]:
    """
    Get the versions of the root that contains the
    given sub_dataset_version at the given sub_dataset_path,
    if any exists. If the configuration does not exist
    return an empty iterable.
    """
    root_path = PosixPath(root_dataset_path).resolve()
    current_path = (root_path / sub_dataset_path).resolve()

    # Ensure that the sub-dataset path is under the root-dataset path
    current_path.relative_to(root_path)

    current_version = sub_dataset_version
    current_path = current_path.parent
    while len(current_path.parts) >= len(root_path.parts):

        # Skip intermediate directories, i.e. check only on git
        # repository roots.
        if len(tuple(current_path.glob(".git"))) == 0:
            current_path = current_path.parent
            continue

        current_version = find_version_containing(current_path, current_version)
        if current_version == "":
            return []
        current_path = current_path.parent

    return [current_version]


def find_version_containing(path: PosixPath, current_version):
    import subprocess

    result = subprocess.run([
        f"git", "--git-dir", str(path / ".git"), "log",
        f"--find-object={current_version}",
        f"--pretty=tformat:%h", "--no-abbrev"],
        stdout=subprocess.PIPE
    )

    return result.stdout.decode().strip()
