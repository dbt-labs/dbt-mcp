import logging
from typing import Literal

import requests
from snowplow_tracker import Emitter, SelfDescribingJson, Tracker
from snowplow_tracker import logger as sp_logger
from snowplow_tracker.events import StructuredEvent

logger = logging.getLogger(__name__)
sp_logger.setLevel(100)

COLLECTOR_URL = "fishtownanalytics.sinter-collect.com"
COLLECTOR_PROTOCOL: Literal["http", "https"] = "https"
tracking_enabled = True


def handle_failure(num_ok, unsent):
    # num_ok will always be 0, unsent will always be 1 entry long, because
    # the buffer is length 1, so not much to talk about
    global tracking_enabled  # noqa:PLW0603
    tracking_enabled = False


class TimeoutEmitter(Emitter):
    def __init__(self) -> None:
        super().__init__(
            COLLECTOR_URL,
            protocol=COLLECTOR_PROTOCOL,
            on_failure=handle_failure,
            method="post",
            # don't set this.
            byte_limit=None,
            batch_size=30,
        )

    def _log_request(self, request, payload):
        sp_logger.info(f"Sending {request} request to {self.endpoint}...")
        sp_logger.debug(f"Payload: {payload}")

    def _log_result(self, request, status_code):
        msg = f"{request} request finished with status code: {status_code}"
        if self.is_good_status_code(status_code):
            sp_logger.info(msg)
        else:
            sp_logger.warning(msg)

    def http_post(self, payload) -> int:
        self._log_request("POST", payload)

        r = requests.post(
            self.endpoint,
            data=payload,
            headers={"content-type": "application/json; charset=utf-8"},
            timeout=5.0,
        )

        self._log_result("GET", r.status_code)
        return r.status_code

    def http_get(self, payload) -> int:
        self._log_request("GET", payload)

        r = requests.get(self.endpoint, params=payload, timeout=5.0)

        self._log_result("GET", r.status_code)
        return r.status_code


emitter = TimeoutEmitter()
tracker = Tracker(
    emitters=emitter,
    namespace="cf",  # TODO: what is cf?
    app_id="dbt-mcp",
)


def disable_tracking() -> None:
    tracker.set_subject(None)


def track(
    category: str,
    action: str,
    label: str,
    schema: str,
    data: dict,
) -> None:
    global tracking_enabled  # noqa:PLW0602
    if not tracking_enabled:
        return
    context = [SelfDescribingJson(schema, data)]
    try:
        tracker.track(
            StructuredEvent(
                category=category,
                action=action,
                label=label,
                context=context,
            )
        )
    except Exception:
        logger.error("Failed to track event", exc_info=True)
