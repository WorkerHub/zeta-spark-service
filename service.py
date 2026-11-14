#!/usr/bin/env python3
"""Thin HTTP service around a resident SparkSession (Spark local mode).

Endpoints:
  GET  /health  -> {"ok": true, "sparkVersion": "..."}
  POST /query   -> header X-Auth-Token required; body {"sql": "...", "maxRows": 1000, "session": "...", "execution": "..."}
                   returns {"columns": [...], "rows": [[...]], "rowCount": n, "truncated": bool}
  POST /cancel  -> header X-Auth-Token required; body {"session": "...", "execution": "..."}
                   cancels the running job(s) of that session's job group, or of
                   that execution's job group when "execution" is given;
                   returns {"ok": true} (also when nothing is running)

The optional "session" field (string, <=128 chars) selects a per-caller Spark
session derived from the resident session via newSession(): current database,
temporary views and session-level config are isolated per session, while
metastore databases and tables stay shared. Requests without "session" use
the default (resident) session, exactly as before this field existed.
Same-session queries are serialized with a per-session lock; different
sessions run in parallel. At most 32 named sessions are kept; sessions idle
for 6 hours are recycled, least recently used first when over the cap.
Session state lives in memory only: a service restart clears it (metastore
data is unaffected). Single statement per request. Values are serialized
JSON-safe (everything else -> str).

The optional "execution" field (string, <=128 chars) scopes a query to its
own job group "session:execution" (default session: "default:execution").
Without it a query runs in the session-level group, exactly as before this
field existed, and /cancel without "execution" still cancels the whole
session group. Cancelling one execution therefore never kills a later
execution in the same session, even when the cancel arrives late.

Configuration (environment variables):
  SPARK_API_TOKEN    Required. Requests without a matching X-Auth-Token
                     header are rejected; when unset, every /query is 401.
  PORT               HTTP listen port (default 8080).
  SPARK_DATA_DIR     Metastore + warehouse directory (default /data/spark).
                     Mount a volume here to keep databases and tables.
  SPARK_MASTER       Spark master (default local[*]).
  SPARK_DRIVER_MEMORY  Driver memory (default 2g).
  SPARK_APP_NAME     Spark application name (default spark-sql-service).
"""
import json
import os
import threading
import time
from decimal import Decimal
from datetime import date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hmac
import sys
import glob

sys.path += ["/opt/spark/python"] + glob.glob("/opt/spark/python/lib/py4j-*-src.zip")

TOKEN = os.environ.get("SPARK_API_TOKEN", "")
MAX_ROWS_HARD = 5000
MAX_SESSIONS = 32
SESSION_IDLE_SECONDS = 6 * 3600
SESSION_KEY_MAX = 128

DATA_DIR = os.environ.get("SPARK_DATA_DIR", "/data/spark")
MASTER = os.environ.get("SPARK_MASTER", "local[*]")
DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
APP_NAME = os.environ.get("SPARK_APP_NAME", "spark-sql-service")

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .master(MASTER)
    .config("spark.driver.memory", DRIVER_MEMORY)
    .config("spark.sql.warehouse.dir", DATA_DIR + "/warehouse")
    .config("spark.local.dir", DATA_DIR + "/tmp")
    .config("spark.driver.extraJavaOptions", "-Dderby.system.home=" + DATA_DIR)
    .config("spark.ui.enabled", "false")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


class _SessionEntry:
    __slots__ = ("session", "lock", "last_used")

    def __init__(self, session):
        self.session = session
        self.lock = threading.Lock()
        self.last_used = time.monotonic()


_DEFAULT_ENTRY = _SessionEntry(spark)
_SESSIONS = {}
_SESSIONS_LOCK = threading.Lock()


def _drop_entry(key):
    # Never call .stop() on a derived session: sessions from newSession()
    # share the resident SparkContext, and stop() would kill it for everyone.
    # Dropping our reference is enough; Spark/GC reclaim the session state.
    _SESSIONS.pop(key, None)


def _purge_idle(now):
    stale = [
        k
        for k, e in _SESSIONS.items()
        if now - e.last_used > SESSION_IDLE_SECONDS and not e.lock.locked()
    ]
    for k in stale:
        _drop_entry(k)


def _evict_lru():
    candidates = sorted(
        (e.last_used, k) for k, e in _SESSIONS.items() if not e.lock.locked()
    )
    if candidates:
        _drop_entry(candidates[0][1])
        return True
    return False


def get_session_entry(key):
    """key=None returns the default (resident) session entry."""
    if key is None:
        return _DEFAULT_ENTRY
    now = time.monotonic()
    with _SESSIONS_LOCK:
        _purge_idle(now)
        entry = _SESSIONS.get(key)
        if entry is None:
            while len(_SESSIONS) >= MAX_SESSIONS:
                if not _evict_lru():
                    break
            entry = _SessionEntry(spark.newSession())
            _SESSIONS[key] = entry
        entry.last_used = now
        return entry


def jsonable(v):
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        import base64
        return base64.b64encode(bytes(v)).decode()
    return str(v)


def _job_group_id(session_key, execution=None):
    """Job group for a session: the session key itself, or "default" for the
    default (resident) session. With an execution id the group is
    "session:execution", so POST /cancel for that execution kills only that
    execution's jobs and a late cancel can never kill a later execution in
    the same session. POST /cancel kills this group."""
    base = session_key if session_key else "default"
    if execution is None:
        return base
    return f"{base}:{execution}"


def _parse_session(payload):
    """Session key from a /query or /cancel body: None/blank means the default
    session. Raises ValueError on a malformed value (caller maps it to 400)."""
    raw_session = payload.get("session")
    if raw_session is None:
        return None
    if not isinstance(raw_session, str) or len(raw_session) > SESSION_KEY_MAX:
        raise ValueError("bad session")
    if raw_session.strip() == "":
        return None
    return raw_session


def _parse_execution(payload):
    """Execution id from a /query or /cancel body: None/blank means no
    execution scoping (session-level job group, as before). Raises
    ValueError on a malformed value (caller maps it to 400)."""
    raw = payload.get("execution")
    if raw is None:
        return None
    if not isinstance(raw, str) or len(raw) > SESSION_KEY_MAX:
        raise ValueError("bad execution")
    if raw.strip() == "":
        return None
    return raw


def _code_only(sql):
    """Return sql with comments and quoted literals blanked out to spaces, so
    separator checks only ever see real code. Skipped regions: line comments
    (-- to end of line), block comments (/* ... */), single-quoted strings,
    double-quoted strings/identifiers and backtick identifiers. A doubled
    quote ('' / "" / ``) is an escaped quote and stays inside the literal;
    a backslash is an ordinary character (Spark escapedStringLiterals is off
    by default). An unterminated literal swallows the rest, which is safe:
    such SQL fails in Spark on its own."""
    out = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if ch == "-" and nxt == "-":
            j = sql.find("\n", i)
            i = n if j < 0 else j
            out.append(" ")
        elif ch == "/" and nxt == "*":
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
            out.append(" ")
        elif ch in "'\"`":
            i += 1
            while i < n:
                if sql[i] == ch:
                    if i + 1 < n and sql[i + 1] == ch:
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            out.append(" ")
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def _has_multiple_statements(sql):
    """True if a code-region ';' is followed by more code. A trailing ';'
    (followed only by whitespace, comments or further empty separators)
    stays tolerated, as the old rstrip-based check did."""
    code = _code_only(sql)
    semi = code.find(";")
    while semi >= 0:
        if code[semi + 1:].strip(" \t\r\n;"):
            return True
        semi = code.find(";", semi + 1)
    return False


def cancel_session_jobs(session_key, execution=None):
    """Cancel the running job(s) of one session's job group (or one
    execution's job group when execution is given), without touching
    the session itself: the SparkSession, its temporary views and its current
    database survive; no job running is a no-op. Runs from the /cancel
    request thread while the query runs in its own thread."""
    spark.sparkContext.cancelJobGroup(_job_group_id(session_key, execution))


def run_query(session, sql, max_rows, job_group=None):
    # The job group lets POST /cancel kill this query from another thread:
    # setJobGroup is thread-local, so it only affects jobs this thread
    # submits; interruptOnCancel makes running tasks stop promptly instead of
    # waiting for the next task boundary. The group is cleared afterwards so
    # a later query on a reused thread never inherits it.
    sc = session.sparkContext
    if job_group:
        sc.setJobGroup(job_group, "zeta query", interruptOnCancel=True)
    try:
        df = session.sql(sql)
        cols = list(df.columns)
        limit = max(1, min(int(max_rows or 1000), MAX_ROWS_HARD))
        rows = df.limit(limit + 1).collect()
        truncated = len(rows) > limit
        rows = rows[:limit]
        out = [[jsonable(v) for v in r] for r in rows]
        return {"columns": cols, "rows": out, "rowCount": len(out), "truncated": truncated}
    finally:
        if job_group:
            # PySpark's SparkContext has no clearJobGroup(); clearing the
            # three local properties is exactly what Scala's clearJobGroup
            # does (None becomes Java null, which removes the property).
            for _key in (
                "spark.jobGroup.id",
                "spark.job.description",
                "spark.job.interruptOnCancel",
            ):
                sc.setLocalProperty(_key, None)


# --- Self-healing -----------------------------------------------------------
# A dead gateway cannot be repaired in-process; exiting lets Docker's
# restart policy relaunch the container with a fresh JVM. Metastore and
# warehouse data live on the data volume and are unaffected.

_EXIT_LOCK = threading.Lock()
_EXITING = False

_GATEWAY_DEAD_MARKERS = (
    "Connection refused",
    "Connection reset",
    "connect to the Java server",
    "stopped SparkContext",
    "Cannot call methods on a stopped",
)


def _is_gateway_dead(exc):
    if type(exc).__name__ in ("Py4JNetworkError", "ConnectionRefusedError"):
        return True
    msg = str(exc)
    return any(m in msg for m in _GATEWAY_DEAD_MARKERS)


def _exit_for_restart(reason):
    global _EXITING
    with _EXIT_LOCK:
        if _EXITING:
            return
        _EXITING = True
    print(
        f"FATAL: spark gateway dead ({reason}); exiting so docker restarts the service",
        flush=True,
    )

    def _go():
        time.sleep(0.5)
        os._exit(70)

    threading.Thread(target=_go, daemon=True).start()


def _probe_gateway():
    """One live round-trip through the default session; raises on failure."""
    _ = spark.version
    spark.sql("SELECT 1").collect()


def _gateway_confirmed_dead():
    # Require two failures in a row, a few seconds apart, so a single
    # transient py4j hiccup never restarts the service.
    for attempt in range(2):
        try:
            _probe_gateway()
            return False
        except Exception as e:
            print(f"watchdog: gateway probe failed ({attempt + 1}/2): {e!r:.200}", flush=True)
            if attempt == 0:
                time.sleep(3)
    return True


def _watchdog():
    while True:
        time.sleep(30)
        try:
            _probe_gateway()
        except Exception:
            if _gateway_confirmed_dead():
                _exit_for_restart("watchdog probe")
                return


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            try:
                self._send(200, {"ok": True, "sparkVersion": spark.version})
            except Exception as e:
                if _is_gateway_dead(e):
                    _exit_for_restart("health check")
                try:
                    self._send(503, {"ok": False, "error": str(e)[:200]})
                except Exception:
                    pass
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/query":
            self._handle_query()
        elif self.path == "/cancel":
            self._handle_cancel()
        else:
            self._send(404, {"error": "not found"})

    def _check_auth(self):
        # Empty TOKEN fails closed for every endpoint, exactly like /query.
        token = self.headers.get("X-Auth-Token", "")
        return bool(TOKEN) and hmac.compare_digest(token, TOKEN)

    def _read_json_body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0 or length > 65536:
            raise ValueError("bad request size")
        return json.loads(self.rfile.read(length))

    def _handle_query(self):
        if not self._check_auth():
            self._send(401, {"error": "unauthorized"})
            return
        try:
            payload = self._read_json_body()
            sql = (payload.get("sql") or "").strip().rstrip(";").strip()
            if not sql:
                self._send(400, {"error": "empty sql"})
                return
            if _has_multiple_statements(sql):
                self._send(400, {"error": "single statement only"})
                return
            session_key = _parse_session(payload)
            execution_key = _parse_execution(payload)
        except Exception as e:
            self._send(400, {"error": f"bad request: {e}"})
            return
        try:
            entry = get_session_entry(session_key)
        except Exception as e:
            if _is_gateway_dead(e):
                _exit_for_restart("session create")
            self._send(200, {"error": str(e)[:2000]})
            return
        with entry.lock:
            entry.last_used = time.monotonic()
            try:
                result = run_query(
                    entry.session, sql, payload.get("maxRows"),
                    _job_group_id(session_key, execution_key),
                )
                self._send(200, result)
            except Exception as e:
                if _is_gateway_dead(e):
                    _exit_for_restart("query")
                self._send(200, {"error": str(e)[:2000]})
            finally:
                entry.last_used = time.monotonic()

    def _handle_cancel(self):
        # Deliberately takes no session lock and creates no session entry:
        # it must run concurrently while the query holds the lock, and
        # cancelling a session with no job running is a 200 no-op.
        if not self._check_auth():
            self._send(401, {"error": "unauthorized"})
            return
        try:
            payload = self._read_json_body()
            session_key = _parse_session(payload)
            execution_key = _parse_execution(payload)
        except Exception as e:
            self._send(400, {"error": f"bad request: {e}"})
            return
        try:
            cancel_session_jobs(session_key, execution_key)
            self._send(200, {"ok": True})
        except Exception as e:
            if _is_gateway_dead(e):
                _exit_for_restart("cancel")
            self._send(200, {"error": str(e)[:2000]})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    print(f"spark {spark.version} ready on :{port}", flush=True)
    threading.Thread(target=_watchdog, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
