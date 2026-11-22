"""Shared triage worker.

The queue holds *inference* ids (one per distinct feature fingerprint), never
individual review slips.  A separate review slip submitted with an identical
input is attached to the same inference and receives the conclusion when the
tree finishes, so the tree is evaluated once per distinct input.

Crash safety:
* an inference claimed by a worker carries ``locked_by``/``locked_until``;
* if the process dies mid-run the sweeper requeues the inference once the lease
  expires, and a later attempt starts the same tree evaluation again;
* completion only updates slips still in ``queued``/``running``, so a retry can
  never re-send advice to a slip that already received it.
"""

import json
import os
import socket
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta

import pymysql
import redis
from algorithm import classify
from pymysql.err import OperationalError

DB = {
    "host": os.getenv("MYSQL_HOST", "db"), "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "lift_user"), "password": os.getenv("MYSQL_PASSWORD", "lift_pass"),
    "database": os.getenv("MYSQL_DATABASE", "lift_review"), "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": False,
}
QUEUE_KEY = "lift_analysis"
WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"
LEASE_SECONDS = int(os.getenv("WORKER_LEASE_SECONDS", "60"))
HEARTBEAT_FILE = os.getenv("WORKER_HEARTBEAT_FILE", "/shared/worker_healthy")
RECOVERY_INTERVAL = int(os.getenv("WORKER_RECOVERY_SECONDS", "5"))

rdb = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)


@contextmanager
def transaction():
    conn = pymysql.connect(**DB)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


RETRYABLE_DB_ERRORS = (1205, 1213)  # lock wait timeout, deadlock


def retry_db(operation, attempts=6):
    """Retry a short write transaction killed by a concurrent lock conflict.

    A submission for the same lift may hold the lift row while this worker
    publishes results; InnoDB can pick either side as the deadlock victim. A
    retry replays the idempotent statement instead of failing the inference.
    """
    delay = 0.02
    for attempt in range(attempts):
        try:
            return operation()
        except OperationalError as exc:
            if exc.args and exc.args[0] in RETRYABLE_DB_ERRORS and attempt + 1 < attempts:
                time.sleep(delay)
                delay = min(delay * 2, 0.4)
                continue
            raise


def ensure_queue():
    """Migrate a legacy list-typed queue key so the sorted-set worker can use it."""
    try:
        kind = rdb.type(QUEUE_KEY)
    except Exception:
        return
    if kind not in (b"zset", "zset", "none", b"none", None):
        try:
            rdb.rename(QUEUE_KEY, QUEUE_KEY + ":legacy")
        except Exception:
            rdb.delete(QUEUE_KEY)
        # migrate any job ids a previous deployment left on the list
        for legacy_id in rdb.lrange(QUEUE_KEY + ":legacy", 0, -1):
            try:
                with transaction() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT inference_id FROM analysis_jobs WHERE id=%s", (int(legacy_id),))
                        row = cur.fetchone()
                if row and row["inference_id"]:
                    rdb.zadd(QUEUE_KEY, {str(row["inference_id"]): time.time()}, nx=True)
            except Exception:
                continue


def _features(value):
    if isinstance(value, dict):
        return value
    return json.loads(value)


def claim(inference_id):
    """Take the inference if it still needs work; returns its features or None."""
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM analysis_inferences WHERE id=%s FOR UPDATE", (inference_id,))
            row = cur.fetchone()
            if not row or row["status"] == "completed":
                return None
            if row["status"] == "running" and row["locked_until"] and row["locked_until"] > datetime.utcnow():
                return None
            cur.execute(
                """UPDATE analysis_inferences
                   SET status='running',locked_by=%s,locked_until=%s,heartbeat_at=%s,attempts=attempts+1,error=NULL
                   WHERE id=%s""",
                (WORKER_ID, datetime.utcnow() + timedelta(seconds=LEASE_SECONDS), datetime.utcnow(), inference_id),
            )
            return _features(row["features_json"])


def heartbeat(inference_id):
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE analysis_inferences
                   SET heartbeat_at=%s,locked_until=%s
                   WHERE id=%s AND status='running' AND locked_by=%s""",
                (datetime.utcnow(), datetime.utcnow() + timedelta(seconds=LEASE_SECONDS), inference_id, WORKER_ID),
            )


def complete(inference_id, result, features):
    """Publish the conclusion to every slip still waiting on this inference.

    Slips already ``completed`` (advice delivered by an earlier attempt) or
    ``withdrawn`` are untouched, which makes retries safe.
    """
    payload = json.dumps(
        {"rules": result["path_rules"], "model": result["model"], "features": features},
        ensure_ascii=False,
    )
    finished = datetime.utcnow()
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM analysis_inferences WHERE id=%s FOR UPDATE", (inference_id,))
            row = cur.fetchone()
            if not row or row["status"] == "completed":
                return
            cur.execute(
                """UPDATE analysis_inferences
                   SET status='completed',prediction=%s,probability=%s,path_rules_json=%s,model=%s,
                       error=NULL,locked_by=NULL,locked_until=NULL,completed_at=%s
                   WHERE id=%s""",
                (result["prediction"], result["probability"], payload, result["model"], finished, inference_id),
            )
            cur.execute(
                """SELECT DISTINCT lift_id FROM analysis_jobs
                   WHERE inference_id=%s AND status IN ('queued','running')""",
                (inference_id,),
            )
            lift_ids = [item["lift_id"] for item in cur.fetchall()]
            cur.execute(
                """UPDATE analysis_jobs
                   SET status='completed',prediction=%s,probability=%s,path_rules_json=%s,error=NULL,completed_at=%s
                   WHERE inference_id=%s AND status IN ('queued','running')""",
                (result["prediction"], result["probability"], payload, finished, inference_id),
            )
            for lift_id in lift_ids:
                cur.execute(
                    "UPDATE lifts SET status=IF(status='under_review','rectification',status) WHERE id=%s",
                    (lift_id,),
                )


def fail(inference_id, message):
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE analysis_inferences
                   SET status='failed',error=%s,locked_by=NULL,locked_until=NULL,heartbeat_at=%s
                   WHERE id=%s AND status<>'completed'""",
                (message[:2000], datetime.utcnow(), inference_id),
            )
            cur.execute(
                "UPDATE analysis_jobs SET status='failed',error=%s WHERE inference_id=%s AND status IN ('queued','running')",
                (message[:2000], inference_id),
            )


def process(inference_id):
    features = retry_db(lambda: claim(inference_id))
    if features is None:
        return
    stop = threading.Event()

    def renew():
        while not stop.wait(LEASE_SECONDS / 3):
            try:
                retry_db(lambda: heartbeat(inference_id))
            except Exception as exc:  # the main loop will decide retry/fail
                print(f"heartbeat error for inference {inference_id}: {exc}", flush=True)

    renewer = threading.Thread(target=renew, daemon=True)
    renewer.start()
    try:
        result = classify(features)
        retry_db(lambda: complete(inference_id, result, features))
    except Exception as exc:  # leave the inference retryable
        retry_db(lambda: fail(inference_id, str(exc)))
    finally:
        stop.set()


def recover():
    """Re-enqueue work whose lease expired or that is still waiting to start."""
    horizon = datetime.utcnow()
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id FROM analysis_inferences
                   WHERE status='queued' OR (status='running' AND (locked_until IS NULL OR locked_until<%s))""",
                (horizon,),
            )
            ids = [item["id"] for item in cur.fetchall()]
    if ids:
        # A crashed claim may have been popped off the queue; put it back.
        rdb.zadd(QUEUE_KEY, {str(inference_id): time.time() for inference_id in ids}, nx=True)
    return ids


def touch_health():
    try:
        os.makedirs(os.path.dirname(HEARTBEAT_FILE) or ".", exist_ok=True)
        with open(HEARTBEAT_FILE, "w") as handle:
            handle.write(datetime.utcnow().isoformat())
    except OSError:
        pass


def main():
    ensure_queue()
    last_recovery = 0.0
    while True:
        try:
            touch_health()
            if time.time() - last_recovery > RECOVERY_INTERVAL:
                recover()
                last_recovery = time.time()
            message = rdb.bzpopmin(QUEUE_KEY, timeout=2)
            if message:
                process(int(message[1]))
        except Exception as exc:
            print(f"worker loop error: {exc}", flush=True)
            time.sleep(2)


if __name__ == "__main__":
    main()
