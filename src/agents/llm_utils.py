"""
Shared LLM call utilities for agents.

Provides retry with exponential backoff for rate limit (429) and overload (503) errors.
"""

import time
from typing import Any, Dict

import requests

from back.core.logging import get_logger

logger = get_logger(__name__)

_RATE_LIMIT_RETRIES = 6
_RATE_LIMIT_BASE_DELAY = 5  # seconds
_RATE_LIMIT_MAX_DELAY = 60  # cap so we never sleep longer than this


def _get_retry_delay(attempt: int, response: requests.Response = None) -> float:
    """Compute backoff delay, honouring the Retry-After header when present."""
    backoff = min(_RATE_LIMIT_BASE_DELAY * (2 ** (attempt - 1)), _RATE_LIMIT_MAX_DELAY)
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                backoff = min(float(retry_after), _RATE_LIMIT_MAX_DELAY)
            except (ValueError, TypeError):
                pass
    return backoff


def call_llm_with_retry(
    url: str,
    headers: Dict[str, str],
    payload: Dict[str, Any],
    timeout: int = 180,
) -> requests.Response:
    """POST to LLM endpoint with retry on 429/503.

    Retries up to ``_RATE_LIMIT_RETRIES`` times with exponential backoff
    (5 s, 10 s, 20 s, 40 s, 60 s, 60 s) capped at ``_RATE_LIMIT_MAX_DELAY``.
    If the response includes a ``Retry-After`` header its value is used instead.
    """
    last_exc = None
    for attempt in range(1, _RATE_LIMIT_RETRIES + 1):
        try:
            t0 = time.time()
            resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
            elapsed_ms = int((time.time() - t0) * 1000)
            logger.info(
                "LLM: status=%d, %d bytes in %dms (attempt %d/%d)",
                resp.status_code,
                len(resp.content),
                elapsed_ms,
                attempt,
                _RATE_LIMIT_RETRIES,
            )
            if resp.status_code in (429, 503):
                last_exc = requests.exceptions.HTTPError(
                    f"Rate limit or overload (HTTP {resp.status_code})", response=resp
                )
                if attempt < _RATE_LIMIT_RETRIES:
                    delay = _get_retry_delay(attempt, resp)
                    logger.warning(
                        "LLM: HTTP %d — waiting %.0fs before retry %d/%d",
                        resp.status_code,
                        delay,
                        attempt + 1,
                        _RATE_LIMIT_RETRIES,
                    )
                    time.sleep(delay)
                    continue
                resp.raise_for_status()
            if not resp.ok:
                body_preview = (resp.text or "")[:800]
                logger.error(
                    "LLM: HTTP %d from %s — body: %s",
                    resp.status_code,
                    url,
                    body_preview,
                )
                raise requests.exceptions.HTTPError(
                    f"{resp.status_code} Client Error: {resp.reason} for url: {url} "
                    f"— body: {body_preview}",
                    response=resp,
                )
            return resp
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status in (429, 503) and attempt < _RATE_LIMIT_RETRIES:
                delay = _get_retry_delay(attempt, exc.response)
                logger.warning(
                    "LLM: HTTP %s — waiting %.0fs before retry %d/%d",
                    status,
                    delay,
                    attempt + 1,
                    _RATE_LIMIT_RETRIES,
                )
                time.sleep(delay)
                last_exc = exc
                continue
            raise
        except (
            requests.exceptions.ReadTimeout,
            requests.exceptions.RequestException,
        ) as exc:
            last_exc = exc
            if attempt < _RATE_LIMIT_RETRIES:
                delay = _get_retry_delay(attempt)
                logger.warning(
                    "LLM: %s — waiting %.0fs before retry %d/%d",
                    type(exc).__name__,
                    delay,
                    attempt + 1,
                    _RATE_LIMIT_RETRIES,
                )
                time.sleep(delay)
                continue
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError("call_llm_with_retry: unexpected exit")
