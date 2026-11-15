"""Domain errors mapped to HTTP status codes by the API layer."""
from __future__ import annotations


class RegistryError(Exception):
    """Base class; default HTTP status is 400."""

    status_code = 400

    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class NotFoundError(RegistryError):
    status_code = 404


class ConflictError(RegistryError):
    status_code = 409
