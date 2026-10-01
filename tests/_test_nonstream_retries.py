"""Deterministic coverage for retries after an upstream SSE read failure."""
from http.client import IncompleteRead
import os
import sys
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("ACCOUNTS_DIR",
                      os.path.join(os.path.dirname(os.path.abspath(__file__)), "_acc"))
os.environ.setdefault("USAGE_DIR",
                      os.path.join(os.path.dirname(os.path.abspath(__file__)), "_use"))

import wb_proxy as P

PASS = FAIL = 0


def check(label, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("  [PASS] " + label)
    else:
        FAIL += 1
        print("  [FAIL] " + label + ("  " + str(extra) if extra else ""))


class FakeAccount:
    uid = "acct-retry"


class FakeResponse:
    def __init__(self, lines=None, error=None):
        self.lines = lines or []
        self.error = error
        self.closed = False

    def __iter__(self):
        if self.error is not None:
            raise self.error
        return iter(self.lines)

    def close(self):
        self.closed = True


chunk = b'data: {"id":"cmpl-1","choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}\n'
done = b"data: [DONE]\n"
first = FakeResponse(error=IncompleteRead(b"", 10))
second = FakeResponse([chunk, done])
opened = []


def reopen(payload, session_key=None, target_realm=None):
    opened.append((payload, session_key, target_realm))
    return second, FakeAccount()


with mock.patch.object(P, "NONSTREAM_READ_RETRIES", 1):
    active, account, result, error = P.aggregate_nonstream_with_retry(
        first, "hy4-preview-f", {"model": "hy4-preview-f"},
        "group:195298023", "intl", FakeAccount(), opener=reopen)

check("a transient read failure is retried once", len(opened) == 1, opened)
check("the failed response is closed", first.closed)
check("the retry response is returned", active is second)
check("the retry result is aggregated", error is None and
      result["choices"][0]["message"]["content"] == "ok", result)

bad = FakeResponse(error=ValueError("malformed upstream"))
opened_bad = []
with mock.patch.object(P, "NONSTREAM_READ_RETRIES", 1):
    active_bad, _, result_bad, error_bad = P.aggregate_nonstream_with_retry(
        bad, "hy4-preview-f", {}, None, "intl", FakeAccount(),
        opener=lambda *a, **k: opened_bad.append(True))

check("a non-transient read failure is not retried", not opened_bad)
check("the original error is returned", active_bad is bad and result_bad is None and
      isinstance(error_bad, ValueError), error_bad)

print("\n  %d passed, %d failed" % (PASS, FAIL))
raise SystemExit(1 if FAIL else 0)
