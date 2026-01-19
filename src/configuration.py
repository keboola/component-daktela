import logging

from keboola.component.exceptions import UserException
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

DEFAULT_MAX_CONCURRENT_REQUESTS = (
    10  # Default maximum number of concurrent API requests
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


class Destination(BaseModel):
    """Destination configuration for load type and primary key."""

    load_type: str = "full_load"  # "full_load" or "incremental_load"
    primary_key: list[str] | None = None

    @property
    def incremental(self) -> bool:
        """Convert load_type to boolean for backward compatibility."""
        return self.load_type == "incremental_load"


class RowConfiguration(BaseModel):
    """Row configuration for a single endpoint extraction."""

    endpoint: str
    date_from: str
    date_to: str
    fields: list[str] | None = None
    destination: Destination = Field(default_factory=Destination)

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

    @field_validator("batch_size")
    @classmethod
    def validate_batch_size(cls, v: int) -> int:
        """Validate batch size is positive."""
        if v <= 0:
            raise ValueError("Batch size must be a positive integer.")
        return v


class Configuration(BaseModel):
    """
    Merged configuration combining root config and row config.

    Platform merges root config (from configSchema.json) with row config
    (from configRowSchema.json) into self.configuration.parameters.

    This class reads from the merged parameters and includes both:
    - Root config fields: connection, advanced, debug
    - Row config fields: endpoint, date_from, date_to, fields, destination
    """

    model_config = ConfigDict(extra="ignore")  # Ignore unknown fields

    # Root config fields (from configSchema.json)
    connection: Connection
    advanced: Advanced = Field(default_factory=Advanced)
    debug: bool = False

    # Row config fields (from configRowSchema.json)
    # Optional with defaults for testing and partial configs
    endpoint: str | None = None
    date_from: str = "7 days ago"
    date_to: str = "today"
    fields: list[str] | None = None
    destination: Destination = Field(default_factory=Destination)

    @model_validator(mode="after")
    def log_debug_mode(self) -> "Configuration":
        """Log if debug mode is enabled."""
        if self.debug:
            logging.debug("Component will run in Debug mode")
        return self

    def validate_for_extraction(self) -> None:
        """
        Validate that required fields for extraction are present.

        Call this before running extraction to ensure endpoint is set.
        """
        if not self.endpoint:
            raise ValueError("endpoint is required for extraction")

    @classmethod
    def from_dict(cls, data: dict) -> "Configuration":
        """Create Configuration from dict with user-friendly error messages."""
        try:
            return cls(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")
