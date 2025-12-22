import logging

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

DEFAULT_MAX_CONCURRENT_REQUESTS = (
    10  # Default maximum number of concurrent API requests
)
DEFAULT_MAX_CONCURRENT_ENDPOINTS = (
    3  # Default maximum number of endpoints to extract concurrently
)
DEFAULT_BATCH_SIZE = (
    1000  # Default batch size for processing records before writing to CSV
)


class Connection(BaseModel):
    """Connection configuration."""

    url: str
    username: str
    password: str = Field(alias="#password")
    verify_ssl: bool = True


class RowDestination(BaseModel):
    """Row-level destination configuration."""

    incremental: bool = False
    primary_key: list[str] | None = None


class RowConfiguration(BaseModel):
    """Row configuration for a single endpoint extraction."""

    endpoint: str
    date_from: str
    date_to: str
    fields: list[str] | None = None
    destination: RowDestination = Field(default_factory=RowDestination)

    @classmethod
    def from_dict(cls, data: dict) -> "RowConfiguration":
        """Create RowConfiguration from dict with user-friendly error messages."""
        try:
            return cls(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Row validation error: {', '.join(error_messages)}")


class Advanced(BaseModel):
    """Advanced performance configuration."""

    batch_size: int = DEFAULT_BATCH_SIZE
    max_concurrent_requests: int = DEFAULT_MAX_CONCURRENT_REQUESTS
    max_concurrent_endpoints: int = DEFAULT_MAX_CONCURRENT_ENDPOINTS

    @field_validator("batch_size")
    @classmethod
    def validate_batch_size(cls, v: int) -> int:
        """Validate batch size is positive."""
        if v <= 0:
            raise ValueError("Batch size must be a positive integer.")
        return v


class Configuration(BaseModel):
    """Global configuration (from configSchema.json)."""

    connection: Connection
    advanced: Advanced = Field(default_factory=Advanced)
    debug: bool = False

    @model_validator(mode="after")
    def log_debug_mode(self) -> "Configuration":
        """Log if debug mode is enabled."""
        if self.debug:
            logging.debug("Component will run in Debug mode")
        return self

    @classmethod
    def from_dict(cls, data: dict) -> "Configuration":
        """Create Configuration from dict with user-friendly error messages."""
        try:
            return cls(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")
