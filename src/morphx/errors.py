"""Exception hierarchy shared by the device agent and the central server."""

from __future__ import annotations


class MorphXError(Exception):
    """Base class for every error raised deliberately by this package."""


class ConfigurationError(MorphXError, ValueError):
    """Runtime configuration is unsafe or invalid."""


class StorageError(MorphXError):
    """Local or central persistence violated an expected invariant."""


class StoreNotInitializedError(StorageError):
    """A store was used before its schema and metadata were created."""


class DeviceIdentityError(StorageError):
    """A local database was opened with a different device identity."""


class OutboxStateError(StorageError):
    """An outbox row changed state underneath the synchronizer."""


class EventIntegrityError(MorphXError, ValueError):
    """An event factory produced a record that does not match its allocation."""


class AcknowledgementError(MorphXError, ValueError):
    """The server acknowledged a different or malformed event."""


class IngestConflictError(MorphXError):
    """An idempotency key or device sequence was reused for different data."""

    code = "MEASUREMENT_CONFLICT"


class EventIdConflictError(IngestConflictError):
    """The same event_id arrived with a different payload."""

    code = "EVENT_ID_CONFLICT"


class SequenceConflictError(IngestConflictError):
    """A device sequence number already belongs to a different event_id."""

    code = "SEQUENCE_CONFLICT"


class AgentTaskFailedError(MorphXError):
    """A supervised agent task stopped unexpectedly; the process must restart."""
