"""Supported local embedded Store boundary.

The package is intentionally small: :class:`EmbeddedStore` owns the typed
Store operation surface while filesystem calls remain a platform realization.
Consumers do not import Store tests or the historical Phase-1/Phase-2 drivers.
"""

from .embedded import BoundObjectInput, EmbeddedStore, StoredObject
from .errors import (
    BatchCommitResult,
    CommitResult,
    StoreConflict,
    StoreError,
    StoreIntegrityError,
    StoreResultCode,
)
from .session import StoreSession

__all__ = [
    "BatchCommitResult",
    "BoundObjectInput",
    "CommitResult",
    "EmbeddedStore",
    "StoreConflict",
    "StoreError",
    "StoreIntegrityError",
    "StoreResultCode",
    "StoreSession",
    "StoredObject",
]
