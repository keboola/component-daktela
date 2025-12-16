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


class EndpointConfig(BaseModel):
    """Configuration for a single endpoint extraction."""

    endpoint: str
    fields: list[str] | None = None


class DataSelection(BaseModel):
    """Data selection configuration."""

    date_from: str
    date_to: str
    endpoints: list[EndpointConfig]

    def get_endpoint_names(self) -> list[str]:
        """Get list of endpoint names."""
        return [ep.endpoint for ep in self.endpoints]

    def get_fields_dict(self) -> dict[str, list[str]]:
        """Get fields configuration as a dictionary (for backward compatibility)."""
        result = {}
        for ep in self.endpoints:
            if ep.fields:
                result[ep.endpoint] = ep.fields
        return result if result else None


class Destination(BaseModel):
    """Destination configuration."""

    incremental: bool = False


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
    connection: Connection
    data_selection: DataSelection
    destination: Destination = Field(default_factory=Destination)
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
