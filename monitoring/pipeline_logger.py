import logging
import json
import time
from datetime import datetime
from functools import wraps

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("DisneyPipelineMonitor")


class PipelineLogger:
    """
    Monitors and logs ETL pipeline execution.
    In production, integrates with Azure Monitor / Application Insights.
    """

    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.run_id = datetime.utcnow().strftime("RUN_%Y%m%d_%H%M%S")
        self.events = []
        self.start_time = None

    def start(self):
        self.start_time = time.time()
        self._log("PIPELINE_START", f"Pipeline '{self.pipeline_name}' started | run_id={self.run_id}")

    def end(self, rows_processed: int = 0):
        duration = round(time.time() - self.start_time, 2)
        self._log("PIPELINE_END",
                  f"Pipeline '{self.pipeline_name}' completed | "
                  f"rows={rows_processed} | duration={duration}s | run_id={self.run_id}")

    def log_stage(self, stage: str, rows: int, status: str = "SUCCESS"):
        self._log(f"STAGE_{status}", f"[{stage}] rows={rows} | status={status}")

    def log_warning(self, message: str):
        self._log("WARNING", message)
        logger.warning(message)

    def log_error(self, message: str):
        self._log("ERROR", message)
        logger.error(message)

    def log_validation(self, passed: bool, report: dict):
        status = "PASSED" if passed else "FAILED"
        self._log(f"VALIDATION_{status}",
                  f"rows={report['total_rows']} | "
                  f"dupes={report['duplicate_count']} | "
                  f"missing_cols={report['missing_columns']}")

    def _log(self, event_type: str, message: str):
        entry = {
            "timestamp":  datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "run_id":     self.run_id,
            "pipeline":   self.pipeline_name,
            "event_type": event_type,
            "message":    message
        }
        self.events.append(entry)
        logger.info(f"[{event_type}] {message}")

    def get_summary(self) -> dict:
        return {
            "run_id":        self.run_id,
            "pipeline":      self.pipeline_name,
            "total_events":  len(self.events),
            "errors":        sum(1 for e in self.events if e["event_type"] == "ERROR"),
            "warnings":      sum(1 for e in self.events if e["event_type"] == "WARNING"),
            "events":        self.events
        }

    def print_summary(self):
        summary = self.get_summary()
        print("\n" + "=" * 50)
        print(f"  PIPELINE RUN SUMMARY: {self.pipeline_name}")
        print("=" * 50)
        print(f"  Run ID     : {summary['run_id']}")
        print(f"  Events     : {summary['total_events']}")
        print(f"  Errors     : {summary['errors']}")
        print(f"  Warnings   : {summary['warnings']}")
        print("=" * 50)


def monitor_stage(stage_name: str):
    """
    Decorator to automatically log stage execution time and status.
    Usage: @monitor_stage("Transform")
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            logger.info(f"▶ Stage '{stage_name}' started")
            try:
                result = func(*args, **kwargs)
                duration = round(time.time() - start, 2)
                logger.info(f"✅ Stage '{stage_name}' completed in {duration}s")
                return result
            except Exception as e:
                duration = round(time.time() - start, 2)
                logger.error(f"❌ Stage '{stage_name}' failed after {duration}s: {e}")
                raise
        return wrapper
    return decorator


if __name__ == "__main__":
    monitor = PipelineLogger("DisneyContentETL")
    monitor.start()

    monitor.log_stage("Extract",  rows=100, status="SUCCESS")
    monitor.log_stage("Validate", rows=98,  status="SUCCESS")
    monitor.log_warning("2 rows dropped due to null department")
    monitor.log_stage("Transform", rows=98, status="SUCCESS")
    monitor.log_stage("Load",      rows=98, status="SUCCESS")

    monitor.end(rows_processed=98)
    monitor.print_summary()
