"""Fingerprint-keyed triage inference shared by every review request with the same input.

A review request ("复核单") never runs the tree itself. It references an
inference row keyed by a canonical fingerprint of the four triage features, so
lifts with identical inputs share one tree evaluation. Each slip keeps its own
lifecycle (waiting / completed / withdrawn): one operator withdrawing their slip
must not erase the advice already handed to the other slips on that inference.
"""

import json
import time
from datetime import datetime

from fastapi import HTTPException
from pymysql.err import OperationalError

from .triage import FEATURE_NAMES, canonical_features, fingerprint  # noqa: F401  (re-exported)
from .db import connect
from .cache import redis_client as _redis_client

QUEUE_KEY = "lift_analysis"
ACTIVE_JOB_STATUSES = ("queued", "running")
RETRYABLE_DB_ERRORS = (1205, 1213)  # lock wait timeout, deadlock


def _retry_transaction(operation, attempts=8):
    """Run one short write transaction, retrying lock conflicts in place.

    Several lifts with an identical fingerprint insert the same inference row at
    the same time; InnoDB may pick a deadlock victim under gap locks. Each
    attempt is independent and idempotent thanks to the UNIQUE fingerprint.
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


def _fetch_dicts(sql, params=()):
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())
    finally:
        conn.close()


def _json(value, default=None):
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def _loads_features(value):
    parsed = _json(value, {})
    return parsed if isinstance(parsed, dict) else {}


def job_state(row):
    if not row:
        return "missing"
    status = row["status"]
    if status == "withdrawn":
        return "withdrawn"
    if status == "completed":
        return "completed"
    if status in ACTIVE_JOB_STATUSES:
        return "waiting"
    return "failed"


def expose(row):
    """Public shape of a review slip; a withdrawn slip never exposes its conclusion."""
    if not row:
        return None
    state = job_state(row)
    visible = state == "completed"
    item = dict(row)
    item["state"] = state
    item["has_result"] = visible
    item["conclusion_visible"] = visible
    if not visible:
        item["prediction"] = None
        item["probability"] = None
        item["path_rules_json"] = None
        item["path_rules"] = None
    else:
        item["path_rules"] = _json(item.get("path_rules_json"))
    item.pop("withdrawn_by", None)
    item["features"] = _loads_features(item.pop("features_json", None))
    return item


JOB_SELECT = """
SELECT j.*, l.asset_code, l.building,
       (SELECT COUNT(*) FROM analysis_jobs s
          WHERE s.inference_id=j.inference_id AND s.status<>'withdrawn') AS shared_jobs,
       (SELECT COUNT(*) FROM analysis_jobs s
          WHERE s.inference_id=j.inference_id AND s.status IN ('queued','running')) AS waiting_jobs,
       i.status AS inference_status, i.attempts AS inference_attempts
FROM analysis_jobs j
JOIN lifts l ON l.id=j.lift_id
LEFT JOIN analysis_inferences i ON i.id=j.inference_id
"""


def get_job(job_id):
    rows = _fetch_dicts(JOB_SELECT + " WHERE j.id=%s", (job_id,))
    return expose(rows[0]) if rows else None


def latest_job(lift_id):
    rows = _fetch_dicts(JOB_SELECT + " WHERE j.lift_id=%s ORDER BY j.id DESC LIMIT 1", (lift_id,))
    return expose(rows[0]) if rows else None


def list_jobs(lift_id=None, state=None, limit=100):
    clauses, params = [], []
    if lift_id is not None:
        clauses.append("j.lift_id=%s")
        params.append(int(lift_id))
    if state == "waiting":
        clauses.append("j.status IN ('queued','running')")
    elif state:
        clauses.append("j.status=%s")
        params.append(state)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(max(1, min(int(limit), 500)))
    return [expose(row) for row in _fetch_dicts(JOB_SELECT + where + " ORDER BY j.id DESC LIMIT %s", tuple(params))]


def list_inferences(limit=100):
    sql = """
    SELECT i.*,
           (SELECT COUNT(*) FROM analysis_jobs j
              WHERE j.inference_id=i.id AND j.status<>'withdrawn') AS job_count,
           (SELECT COUNT(*) FROM analysis_jobs j
              WHERE j.inference_id=i.id AND j.status IN ('queued','running')) AS waiting_jobs,
           (SELECT COUNT(*) FROM analysis_jobs j
              WHERE j.inference_id=i.id AND j.status='completed') AS completed_jobs
    FROM analysis_inferences i ORDER BY i.id DESC LIMIT %s
    """
    limit = max(1, min(int(limit), 500))
    result = []
    for row in _fetch_dicts(sql, (limit,)):
        item = dict(row)
        item["features"] = _loads_features(item.pop("features_json", None))
        item["path_rules"] = _json(item.get("path_rules_json"))
        result.append(item)
    return result


def ensure_queue(client):
    """Keep the queue a sorted set even when an older deployment left a list behind."""
    try:
        kind = client.type(QUEUE_KEY)
    except Exception:
        return
    if kind not in (b"zset", "zset", "none", b"none", None):
        try:
            client.rename(QUEUE_KEY, QUEUE_KEY + ":legacy")
        except Exception:
            client.delete(QUEUE_KEY)


def enqueue(inference_id, redis_client=None):
    """Put an inference on the shared worker queue without duplicating members."""
    client = redis_client or _redis_client
    ensure_queue(client)
    client.zadd(QUEUE_KEY, {str(int(inference_id)): time.time()}, nx=True)


def _request_review_once(lift_id, actor_id, canonical, fp):
    conn = connect()
    try:
        conn.begin()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM lifts WHERE id=%s FOR UPDATE", (lift_id,))
            if not cur.fetchone():
                raise HTTPException(404, "电梯档案不存在")
            cur.execute(
                """SELECT id FROM analysis_jobs
                   WHERE lift_id=%s AND fingerprint=%s AND status IN ('queued','running','completed')
                   ORDER BY id DESC LIMIT 1 FOR UPDATE""",
                (lift_id, fp),
            )
            existing = cur.fetchone()
            if existing:
                conn.commit()
                return {"job_id": existing["id"], "created": False}
            cur.execute(
                """INSERT INTO analysis_inferences(fingerprint,features_json,status)
                   VALUES(%s,%s,'queued')
                   ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)""",
                (fp, json.dumps(canonical, sort_keys=True, ensure_ascii=False)),
            )
            inference_id = cur.lastrowid
            cur.execute("SELECT * FROM analysis_inferences WHERE id=%s FOR UPDATE", (inference_id,))
            inference = cur.fetchone()
            if inference["status"] == "failed":
                cur.execute(
                    """UPDATE analysis_inferences
                       SET status='queued',error=NULL,locked_by=NULL,locked_until=NULL
                       WHERE id=%s""",
                    (inference_id,),
                )
                cur.execute(
                    """UPDATE analysis_jobs SET status='queued',error=NULL
                       WHERE inference_id=%s AND status='failed'""",
                    (inference_id,),
                )
                inference["status"] = "queued"
            completed = inference["status"] == "completed"
            cur.execute(
                """INSERT INTO analysis_jobs
                   (lift_id,status,features_json,fingerprint,inference_id,prediction,probability,
                    path_rules_json,requested_by,completed_at)
                   VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    lift_id, "completed" if completed else "queued",
                    json.dumps(canonical, sort_keys=True, ensure_ascii=False), fp, inference_id,
                    inference["prediction"] if completed else None,
                    inference["probability"] if completed else None,
                    inference["path_rules_json"] if completed else None,
                    actor_id, inference["completed_at"] if completed else None,
                ),
            )
            job_id = cur.lastrowid
        conn.commit()
        return {"job_id": job_id, "created": True, "inference_id": inference_id, "completed": completed}
    finally:
        conn.close()


def request_review(lift_id, actor_id, features, redis_client=None):
    """Create (or reuse) a review slip and its shared inference atomically.

    The lift row is locked first so two operator clicks on the same lift cannot
    create two live slips; the inference UNIQUE fingerprint makes two lifts with
    an identical input converge on one inference even when they race.

    Returns ``(job, created)`` where ``job`` is the public slip representation.
    """
    canonical = canonical_features(features)
    fp = fingerprint(canonical)
    outcome = _retry_transaction(lambda: _request_review_once(lift_id, actor_id, canonical, fp))
    if outcome["created"] and not outcome["completed"]:
        enqueue(outcome["inference_id"], redis_client)
    return get_job(outcome["job_id"]), outcome["created"]


def withdraw_job(job_id, actor_id, reason=""):
    _retry_transaction(lambda: _withdraw_once(job_id, actor_id, reason))
    return get_job(job_id)


def _withdraw_once(job_id, actor_id, reason=""):
    conn = connect()
    try:
        conn.begin()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM analysis_jobs WHERE id=%s FOR UPDATE", (job_id,))
            job = cur.fetchone()
            if not job:
                raise HTTPException(404, "复核单不存在")
            if job["status"] != "withdrawn":
                cur.execute(
                    """UPDATE analysis_jobs
                       SET status='withdrawn',withdrawn_at=%s,withdrawn_by=%s,withdraw_reason=%s
                       WHERE id=%s""",
                    (datetime.utcnow(), actor_id, (reason or "操作员撤回复核单")[:255], job_id),
                )
        conn.commit()
    finally:
        conn.close()
    return True


def retry_inference(inference_id, redis_client=None):
    """Requeue an interrupted/failed inference without resending delivered advice.

    Returns ``{"requeued": bool, "status": str}``. A retry while a live worker
    lease is still running is a no-op, and an already-completed inference is
    never recomputed: its delivered slips keep the conclusion they received.
    """
    outcome = _retry_transaction(lambda: _retry_once(inference_id))
    if outcome["requeued"]:
        enqueue(inference_id, redis_client)
    return outcome


def _retry_once(inference_id):
    conn = connect()
    try:
        conn.begin()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM analysis_inferences WHERE id=%s FOR UPDATE", (inference_id,))
            inference = cur.fetchone()
            if not inference:
                raise HTTPException(404, "推理任务不存在")
            if inference["status"] == "completed":
                conn.commit()
                return {"requeued": False, "status": "completed"}
            lease_live = (
                inference["status"] == "running"
                and inference["locked_until"] is not None
                and inference["locked_until"] > datetime.utcnow()
            )
            if lease_live:
                conn.commit()
                return {"requeued": False, "status": "running"}
            cur.execute(
                """UPDATE analysis_inferences
                   SET status='queued',error=NULL,locked_by=NULL,locked_until=NULL
                   WHERE id=%s AND status<>'completed'""",
                (inference_id,),
            )
            cur.execute(
                """UPDATE analysis_jobs SET status='queued',error=NULL
                   WHERE inference_id=%s AND status='failed'""",
                (inference_id,),
            )
        conn.commit()
    finally:
        conn.close()
    return {"requeued": True, "status": "queued"}
