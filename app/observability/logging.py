import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict

from app.observability.tracking import get_request_id


class JSONFormatter(logging.Formatter):
    """
    Custom logging formatter that serializes log records into structured JSON objects.
    Enables instant parsing, indexing, and filtering in cloud log management platforms.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_payload: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # 1. Attach trace ID from async context if available
        req_id = get_request_id()
        if req_id:
            log_payload["request_id"] = req_id

        # 2. Attach any extra structured properties passed to logger (e.g. latency_ms, status_code)
        standard_attrs = {
            "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
            "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
            "created", "msecs", "relativeCreated", "thread", "threadName",
            "processName", "process", "message",
        }
        for key, val in record.__dict__.items():
            if key not in standard_attrs and not key.startswith("_"):
                log_payload[key] = val

        # 3. Attach exception traceback if logged with exc_info=True
        if record.exc_info:
            log_payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_payload, default=str)


def setup_logging(log_level: str = "INFO") -> None:
    """
    Configures the root application logger with the JSONFormatter.
    Standardizes stdout logging across FastAPI, Uvicorn, and background workers.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove default handlers to avoid duplicate log entries
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    # Attach stdout stream handler with JSON formatting
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    root_logger.addHandler(handler)

    # Standardize uvicorn access logging
    logging.getLogger("uvicorn.access").handlers = [handler]
    logging.getLogger("uvicorn.access").propagate = False
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
