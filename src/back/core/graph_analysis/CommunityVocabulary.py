"""CommunityVocabulary -- URI helpers for cluster detection materialisation.

One active partition at a time: membership uses a single ``inCluster``
predicate. Re-materialising deletes every triple under the cluster URI
prefix, then writes a fresh set.
"""

from __future__ import annotations

from typing import Final


class CommunityVocabulary:
    """Domain-aware URI builder for community / cluster triples.

    Pure helpers: ``base_uri`` in, URI string out. No I/O.
    """

    CLUSTER_CLASS_FRAGMENT: Final[str] = "Cluster"
    IN_CLUSTER_FRAGMENT: Final[str] = "inCluster"
    FROM_ALGORITHM_FRAGMENT: Final[str] = "fromAlgorithm"
    CLUSTER_SIZE_FRAGMENT: Final[str] = "clusterSize"
    RESOLUTION_FRAGMENT: Final[str] = "resolution"
    MODULARITY_FRAGMENT: Final[str] = "modularity"
    CLUSTER_PATH_FRAGMENT: Final[str] = "cluster"

    @staticmethod
    def join(base: str, fragment: str) -> str:
        if not base:
            return fragment
        if base.endswith("/") or base.endswith("#"):
            return base + fragment
        return base + "/" + fragment

    @classmethod
    def cluster_class(cls, base_uri: str) -> str:
        return cls.join(base_uri, cls.CLUSTER_CLASS_FRAGMENT)

    @classmethod
    def in_cluster(cls, base_uri: str) -> str:
        return cls.join(base_uri, cls.IN_CLUSTER_FRAGMENT)

    @classmethod
    def from_algorithm(cls, base_uri: str) -> str:
        return cls.join(base_uri, cls.FROM_ALGORITHM_FRAGMENT)

    @classmethod
    def cluster_size(cls, base_uri: str) -> str:
        return cls.join(base_uri, cls.CLUSTER_SIZE_FRAGMENT)

    @classmethod
    def resolution(cls, base_uri: str) -> str:
        return cls.join(base_uri, cls.RESOLUTION_FRAGMENT)

    @classmethod
    def modularity(cls, base_uri: str) -> str:
        return cls.join(base_uri, cls.MODULARITY_FRAGMENT)

    @classmethod
    def cluster_prefix(cls, base_uri: str) -> str:
        return cls.join(base_uri, cls.CLUSTER_PATH_FRAGMENT) + "/"

    @classmethod
    def cluster(cls, base_uri: str, cluster_id: int) -> str:
        return f"{cls.cluster_prefix(base_uri)}c-{int(cluster_id)}"

    @classmethod
    def parse_cluster_id(cls, cluster_uri: str) -> int:
        """Return the integer id encoded in ``…/cluster/c-{id}``, or -1."""
        if not cluster_uri:
            return -1
        tail = cluster_uri.rstrip("/").rsplit("/", 1)[-1]
        if not tail.startswith("c-"):
            return -1
        try:
            return int(tail[2:])
        except ValueError:
            return -1
