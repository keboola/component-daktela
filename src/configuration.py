import logging

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError

DEFAULT_MAX_CONCURRENT_REQUESTS = 10  # Default maximum number of concurrent API requests
DEFAULT_MAX_CONCURRENT_ENDPOINTS = 3  # Default maximum number of endpoints to extract concurrently
DEFAULT_BATCH_SIZE = 1000  # Default batch size for processing records before writing to CSV


class Connection(BaseModel):
    """Connection configuration."""

    url: str
    username: str
    password: str = Field(alias="#password")
    verify_ssl: bool = True


class DataSelection(BaseModel):
    """Data selection configuration."""

    date_from: str
    date_to: str
    endpoints: list[str]


class Destination(BaseModel):
    """Destination configuration."""

    incremental: bool = False
    batch_size: int = DEFAULT_BATCH_SIZE
    max_concurrent_requests: int = DEFAULT_MAX_CONCURRENT_REQUESTS
    max_concurrent_endpoints: int = DEFAULT_MAX_CONCURRENT_ENDPOINTS


class Configuration(BaseModel):
    connection: Connection
    data_selection: DataSelection
    destination: Destination = Field(default_factory=Destination)
    debug: bool = False

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.destination.batch_size <= 0:
            raise UserException("Batch size must be a positive integer.")

        if self.debug:
            logging.debug("Component will run in Debug mode")
