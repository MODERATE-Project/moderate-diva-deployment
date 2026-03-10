from typing import Dict, Optional

from core.stats import topic_stats
from fastapi import APIRouter, Response, status
from pydantic import BaseModel, Field

router = APIRouter()


class ReportEntry(BaseModel):
    validator: str = Field(..., description="Validator identifier.")
    rule: str = Field(..., description="Validation rule type.")
    feature: str = Field(..., description="Feature path evaluated by the rule.")
    VALID: int = Field(..., description="Number of successful validations.")
    FAIL: int = Field(..., description="Number of failed validations.")


class ReportClearResponse(BaseModel):
    message: str = Field(..., description="Human-readable status message.")
    deleted_records: int = Field(..., description="Number of deleted aggregate rows.")


class ReportStatus(BaseModel):
    state: str = Field(
        ...,
        description="Derived reporter health state: starting, healthy, catching_up, stale, or error.",
    )
    kafka_connected: bool = Field(
        ..., description="Whether the reporter currently has a live Kafka consumer."
    )
    consumer_group: str = Field(
        ..., description="Kafka consumer group identifier used by the reporter."
    )
    last_poll_at: Optional[float] = Field(
        None, description="Unix timestamp in seconds for the last Kafka poll attempt."
    )
    last_message_at: Optional[float] = Field(
        None,
        description="Unix timestamp in seconds for the newest Kafka message applied to SQLite.",
    )
    last_flush_at: Optional[float] = Field(
        None,
        description="Unix timestamp in seconds for the last successful batch persistence.",
    )
    seconds_since_last_flush: Optional[float] = Field(
        None, description="Seconds elapsed since the last successful batch persistence."
    )
    messages_processed_total: int = Field(
        ...,
        description="Total number of Kafka messages applied to aggregates since startup state was initialized.",
    )
    current_batch_size: int = Field(
        ..., description="Number of Kafka messages currently buffered in memory."
    )
    validation_pending_messages: Optional[int] = Field(
        None,
        description="Estimated number of validation-topic messages still pending for the Quality Reporter to persist.",
    )
    validation_pending_by_partition: Dict[str, int] = Field(
        ...,
        description="Estimated validation-topic pending messages grouped by Kafka partition.",
    )
    reconnect_count: int = Field(
        ..., description="Number of reconnect attempts recorded by the reporter."
    )
    last_error: Optional[str] = Field(
        None, description="Most recent Kafka or persistence error seen by the reporter."
    )


@router.get(
    "/report",
    status_code=status.HTTP_200_OK,
    response_model=list[ReportEntry],
    summary="Get aggregate validation counts",
    response_description="Aggregate counts grouped by validator, rule, and feature.",
)
async def get_list(response: Response, validator: str | None = None):
    """Return aggregate validation counts, optionally filtered by validator ID."""
    response.headers["Access-Control-Allow-Origin"] = "*"
    return topic_stats.get_report(validator=validator)


@router.get(
    "/report/status",
    status_code=status.HTTP_200_OK,
    response_model=ReportStatus,
    summary="Get reporter runtime status",
    response_description="Reporter freshness, Kafka connectivity, and recent processing telemetry.",
)
async def get_status(response: Response):
    """Return freshness and connectivity metadata for clients that need to detect lag without inferring it from /report."""
    response.headers["Access-Control-Allow-Origin"] = "*"
    return topic_stats.get_status()


@router.delete(
    "/report",
    status_code=status.HTTP_200_OK,
    response_model=ReportClearResponse,
    summary="Clear aggregate validation counts",
    response_description="Confirmation that aggregate report rows were removed.",
)
async def clear_report_data(response: Response):
    """Delete all aggregate statistics without resetting Kafka offsets."""
    response.headers["Access-Control-Allow-Origin"] = "*"
    deleted_count = topic_stats.clear_report()
    return {
        "message": "Report data cleared successfully.",
        "deleted_records": deleted_count,
    }
