class PsqlUtilsError(Exception):
    """Base exception for psql_utils domain errors."""


class ValidationError(PsqlUtilsError):
    """Raised when caller input or query shape is invalid."""


class QueryResultError(PsqlUtilsError):
    """Raised when query result constraints are not satisfied."""


class RecordNotFoundError(PsqlUtilsError):
    """Raised when an expected record does not exist."""


class UniqueConflictError(PsqlUtilsError):
    """Raised when a unique-key conflict is detected."""

