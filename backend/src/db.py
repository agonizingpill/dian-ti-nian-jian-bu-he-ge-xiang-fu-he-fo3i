import json
import os
import time
from contextlib import contextmanager

import pymysql

from .triage import canonical_features, fingerprint

DB = {
    "host": os.getenv("MYSQL_HOST", "db"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "lift_user"),
    "password": os.getenv("MYSQL_PASSWORD", "lift_pass"),
    "database": os.getenv("MYSQL_DATABASE", "lift_review"),
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": True,
}

SCHEMA = [
"""CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, email VARCHAR(120) UNIQUE NOT NULL, name VARCHAR(80) NOT NULL, password_hash CHAR(64) NOT NULL, role ENUM('operator','supervisor','viewer') NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
"""CREATE TABLE IF NOT EXISTS lifts (id INT AUTO_INCREMENT PRIMARY KEY, asset_code VARCHAR(40) UNIQUE NOT NULL, building VARCHAR(120) NOT NULL, address VARCHAR(255) NOT NULL, manufacturer VARCHAR(80) NOT NULL, model VARCHAR(80) NOT NULL, rated_load INT NOT NULL, installed_year INT NOT NULL, inspection_due DATE NOT NULL, status ENUM('active','under_review','rectification','recheck','certified','suspended') NOT NULL DEFAULT 'active', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
"""CREATE TABLE IF NOT EXISTS inspection_items (id INT AUTO_INCREMENT PRIMARY KEY, lift_id INT NOT NULL, code VARCHAR(40) NOT NULL, title VARCHAR(160) NOT NULL, category ENUM('safety','mechanical','electrical','documentation') NOT NULL, critical BOOLEAN NOT NULL DEFAULT FALSE, score DECIMAL(5,2) NOT NULL, result ENUM('pass','fail','na') NOT NULL, notes TEXT, photo_ref VARCHAR(160), inspected_at DATETIME NOT NULL, status ENUM('open','rectified','verified') NOT NULL DEFAULT 'open', UNIQUE KEY uq_lift_code (lift_id, code), FOREIGN KEY (lift_id) REFERENCES lifts(id) ON DELETE CASCADE)""",
"""CREATE TABLE IF NOT EXISTS rectification_notices (id INT AUTO_INCREMENT PRIMARY KEY, lift_id INT NOT NULL, inspection_item_id INT NOT NULL, issue_summary VARCHAR(255) NOT NULL, severity ENUM('critical','major','minor') NOT NULL, deadline DATE NOT NULL, status ENUM('open','submitted','verified','rejected','closed') NOT NULL DEFAULT 'open', evidence_required VARCHAR(160) NOT NULL, created_by INT NOT NULL, closed_at DATETIME NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (lift_id) REFERENCES lifts(id) ON DELETE CASCADE, FOREIGN KEY (inspection_item_id) REFERENCES inspection_items(id), FOREIGN KEY (created_by) REFERENCES users(id))""",
"""CREATE TABLE IF NOT EXISTS rechecks (id INT AUTO_INCREMENT PRIMARY KEY, notice_id INT NOT NULL, scheduled_at DATETIME NOT NULL, inspector VARCHAR(100) NOT NULL, status ENUM('scheduled','evidence_rejected','passed','failed') NOT NULL DEFAULT 'scheduled', photo_ref VARCHAR(160), photo_item_code VARCHAR(40), result ENUM('pass','fail') NULL, remarks TEXT, created_by INT NOT NULL, reviewed_by INT NULL, reviewed_at DATETIME NULL, FOREIGN KEY (notice_id) REFERENCES rectification_notices(id) ON DELETE CASCADE, FOREIGN KEY (created_by) REFERENCES users(id), FOREIGN KEY (reviewed_by) REFERENCES users(id))""",
"""CREATE TABLE IF NOT EXISTS certificates (id INT AUTO_INCREMENT PRIMARY KEY, lift_id INT NOT NULL, certificate_no VARCHAR(60) UNIQUE NOT NULL, issued_at DATETIME NOT NULL, valid_until DATE NOT NULL, status ENUM('issued','revoked') NOT NULL DEFAULT 'issued', decision_reason TEXT NOT NULL, issued_by INT NOT NULL, FOREIGN KEY (lift_id) REFERENCES lifts(id) ON DELETE CASCADE, FOREIGN KEY (issued_by) REFERENCES users(id))""",
# One row per distinct triage input (fingerprint).  Every review request with
# the same input attaches to this row instead of running the tree again.
"""CREATE TABLE IF NOT EXISTS analysis_inferences (id INT AUTO_INCREMENT PRIMARY KEY, fingerprint CHAR(64) NOT NULL, features_json JSON NOT NULL, status ENUM('queued','running','completed','failed') NOT NULL DEFAULT 'queued', prediction VARCHAR(40), probability DECIMAL(6,4), path_rules_json JSON, model VARCHAR(160), attempts INT NOT NULL DEFAULT 0, error TEXT, locked_by VARCHAR(64) NULL, locked_until DATETIME NULL, heartbeat_at DATETIME NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, completed_at DATETIME NULL, UNIQUE KEY uq_analysis_inferences_fingerprint (fingerprint), KEY idx_analysis_inferences_status (status))""",
"""CREATE TABLE IF NOT EXISTS analysis_jobs (id INT AUTO_INCREMENT PRIMARY KEY, lift_id INT NOT NULL, status ENUM('queued','running','completed','failed','withdrawn') NOT NULL DEFAULT 'queued', features_json JSON NOT NULL, fingerprint CHAR(64) NULL, inference_id INT NULL, prediction VARCHAR(40), probability DECIMAL(6,4), path_rules_json JSON, error TEXT, requested_by INT NULL, withdrawn_at DATETIME NULL, withdrawn_by INT NULL, withdraw_reason VARCHAR(255) NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, completed_at DATETIME NULL, KEY idx_analysis_jobs_lift_status (lift_id, status), KEY idx_analysis_jobs_fingerprint (fingerprint), KEY idx_analysis_jobs_inference (inference_id), FOREIGN KEY (lift_id) REFERENCES lifts(id) ON DELETE CASCADE)""",
"""CREATE TABLE IF NOT EXISTS audit_logs (id INT AUTO_INCREMENT PRIMARY KEY, actor_id INT NULL, action VARCHAR(100) NOT NULL, entity_type VARCHAR(60) NOT NULL, entity_id INT NULL, detail TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (actor_id) REFERENCES users(id) ON DELETE SET NULL)""",
]

def _column_exists(cur, table, column):
    cur.execute(
        """SELECT 1 FROM information_schema.COLUMNS
           WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s AND COLUMN_NAME=%s""",
        (table, column),
    )
    return cur.fetchone() is not None


def _index_exists(cur, table, index):
    cur.execute(
        """SELECT 1 FROM information_schema.STATISTICS
           WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s AND INDEX_NAME=%s""",
        (table, index),
    )
    return cur.fetchone() is not None


def _migrate(cur):
    """Upgrade databases created by the previous single-job schema in place."""
    additions = {
        "fingerprint": "CHAR(64) NULL",
        "inference_id": "INT NULL",
        "requested_by": "INT NULL",
        "withdrawn_at": "DATETIME NULL",
        "withdrawn_by": "INT NULL",
        "withdraw_reason": "VARCHAR(255) NULL",
    }
    for column, definition in additions.items():
        if not _column_exists(cur, "analysis_jobs", column):
            cur.execute(f"ALTER TABLE analysis_jobs ADD COLUMN {column} {definition}")
    for index, definition in {
        "idx_analysis_jobs_lift_status": "(lift_id, status)",
        "idx_analysis_jobs_fingerprint": "(fingerprint)",
        "idx_analysis_jobs_inference": "(inference_id)",
    }.items():
        if not _index_exists(cur, "analysis_jobs", index):
            cur.execute(f"ALTER TABLE analysis_jobs ADD KEY {index} {definition}")
    cur.execute(
        """SELECT COLUMN_TYPE FROM information_schema.COLUMNS
           WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='analysis_jobs' AND COLUMN_NAME='status'"""
    )
    current = cur.fetchone()
    if current and "withdrawn" not in current["COLUMN_TYPE"]:
        cur.execute(
            "ALTER TABLE analysis_jobs MODIFY status ENUM('queued','running','completed','failed','withdrawn') NOT NULL DEFAULT 'queued'"
        )
    _attach_legacy_jobs(cur)


def _attach_legacy_jobs(cur):
    """Give pre-sharing jobs their fingerprint and shared inference.

    Older deployments created jobs without a fingerprint and pushed job ids onto
    the queue. The inference worker only understands inference ids, so migrating
    jobs here keeps previously queued work runnable and lets a legacy completed
    result be reused by later submissions with the same input.
    """
    cur.execute(
        """SELECT id,features_json,status,prediction,probability,path_rules_json,error,completed_at
           FROM analysis_jobs WHERE inference_id IS NULL OR fingerprint IS NULL"""
    )
    for job in cur.fetchall():
        raw = job["features_json"]
        features = raw if isinstance(raw, dict) else json.loads(raw or "{}")
        if not features:
            continue
        canonical = canonical_features(features)
        fp = fingerprint(canonical)
        legacy_completed = job["status"] == "completed" and job["prediction"] is not None
        cur.execute(
            """INSERT INTO analysis_inferences(
                   fingerprint,features_json,status,prediction,probability,path_rules_json,completed_at)
               VALUES(%s,%s,%s,%s,%s,%s,%s)
               ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)""",
            (
                fp, json.dumps(canonical, sort_keys=True, ensure_ascii=False),
                "completed" if legacy_completed else "queued",
                job["prediction"] if legacy_completed else None,
                job["probability"] if legacy_completed else None,
                job["path_rules_json"] if legacy_completed else None,
                job["completed_at"] if legacy_completed else None,
            ),
        )
        inference_id = cur.lastrowid
        cur.execute("SELECT status FROM analysis_inferences WHERE id=%s", (inference_id,))
        inference_status = cur.fetchone()["status"]
        if inference_status == "completed" and not legacy_completed:
            # Another job already produced this input's advice: reuse it in place.
            cur.execute(
                """UPDATE analysis_jobs j, analysis_inferences i
                   SET j.fingerprint=%s,j.inference_id=%s,j.status='completed',
                       j.prediction=i.prediction,j.probability=i.probability,
                       j.path_rules_json=i.path_rules_json,j.error=NULL,
                       j.completed_at=COALESCE(j.completed_at,i.completed_at)
                   WHERE j.id=%s AND i.id=%s""",
                (fp, inference_id, job["id"], inference_id),
            )
        elif not legacy_completed:
            cur.execute(
                "UPDATE analysis_jobs SET fingerprint=%s,inference_id=%s,status='queued',error=NULL WHERE id=%s",
                (fp, inference_id, job["id"]),
            )
        else:
            cur.execute(
                "UPDATE analysis_jobs SET fingerprint=%s,inference_id=%s WHERE id=%s",
                (fp, inference_id, job["id"]),
            )


@contextmanager
def connection(retries=30):
    last = None
    for _ in range(retries):
        try:
            conn = pymysql.connect(**DB)
            try:
                yield conn
            finally:
                conn.close()
            return
        except Exception as exc:
            last = exc
            time.sleep(1)
    raise last


def connect():
    """Open one connection for callers that need explicit transactions/locks."""
    return pymysql.connect(**{**DB, "autocommit": False})


def init_schema():
    with connection() as conn:
        with conn.cursor() as cur:
            for statement in SCHEMA:
                cur.execute(statement)
            _migrate(cur)
