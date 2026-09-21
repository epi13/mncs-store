"""Typed outcomes at the supported Store consumer boundary."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class StoreResultCode(StrEnum):
    COMMITTED = "COMMITTED"
    DUPLICATE = "DUPLICATE"
    STALE_GENERATION = "STALE_GENERATION"
    CONFLICT = "CONFLICT"
    IDENTITY_CONFLICT = "IDENTITY_CONFLICT"
    INTEGRITY_FAILURE = "INTEGRITY_FAILURE"
    RECOVERED_OLD = "RECOVERED_OLD"
    RECOVERED_NEW = "RECOVERED_NEW"
    RECOVERY_REQUIRED = "RECOVERY_REQUIRED"
    DENIED = "DENIED"
    PLATFORM_UNSUPPORTED = "PLATFORM_UNSUPPORTED"


class StoreError(RuntimeError):
    """A transport-visible Store failure with a stable typed code."""

    def __init__(self, code: StoreResultCode, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class StoreConflict(StoreError):
    def __init__(self, *, expected: int, observed: int, token: bytes) -> None:
        self.expected = expected
        self.observed = observed
        self.token = token
        super().__init__(
            StoreResultCode.STALE_GENERATION,
            f"expected generation {expected}, observed {observed}",
        )


class StoreIntegrityError(StoreError):
    def __init__(self, message: str) -> None:
        super().__init__(StoreResultCode.INTEGRITY_FAILURE, message)


@dataclass(frozen=True, slots=True)
class CommitResult:
    """Bounded result; callers choose whether and how to retry staleness."""

    code: StoreResultCode
    generation: int
    logical_id: bytes
    content_id: bytes
    representation_root: bytes
    binding_id: bytes
    expected_generation: int
    observed_generation: int
    conflict_token: bytes | None = None

    @property
    def committed(self) -> bool:
        return self.code in {StoreResultCode.COMMITTED, StoreResultCode.DUPLICATE}
