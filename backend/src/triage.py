"""Pure triage-input helpers shared by the API, schema migration and worker side.

Kept free of scikit-learn so the API container can canonicalise inputs and key
shared inferences without installing the model runtime.
"""

import hashlib
import json

FEATURE_NAMES = ["critical_fail_count", "average_score", "failed_item_count", "documentation_gap"]


def build_features(items):
    failed = [x for x in items if x["result"] == "fail"]
    scores = [float(x["score"]) for x in items if x["result"] != "na"]
    return {
        "critical_fail_count": sum(1 for x in failed if x["critical"]),
        "average_score": round(sum(scores) / len(scores), 2) if scores else 0,
        "failed_item_count": len(failed),
        "documentation_gap": sum(1 for x in items if x["category"] == "documentation" and x["result"] == "fail"),
    }


def canonical_features(features):
    """Normalise the four triage inputs so equivalent inputs share one fingerprint."""
    canonical = {}
    for name in FEATURE_NAMES:
        value = features[name]
        canonical[name] = round(float(value), 2) if name == "average_score" else int(value)
    return canonical


def fingerprint(features):
    canonical = canonical_features(features)
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
