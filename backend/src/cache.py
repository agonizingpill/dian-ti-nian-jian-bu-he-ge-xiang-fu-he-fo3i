"""Shared Redis handles used for the worker queue and health checks."""

import os

import redis

redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)
