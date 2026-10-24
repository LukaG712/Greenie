"""Cluster support — Redis-based shared state for multi-instance deployments.

Provides distributed locks, shared job queue, instance registration, and
session affinity so multiple Greenie instances can run behind a load balancer.

Key features:
- Distributed locks using Redis SET NX with TTL
- Shared job queue using Redis lists (LPUSH/BRPOP)
- Instance registration with automatic heartbeat and cleanup
- Session affinity via Redis hash storage
- Graceful fallback when Redis is unavailable

Usage:
    from hub.cluster import cluster

    # Initialize on startup
    await cluster.initialize()

    # Distributed lock
    async with cluster.lock("resource-key", timeout=30):
        # Critical section
        pass

    # Job queue
    await cluster.job_queue.push(job_data)
    job_data = await cluster.job_queue.pop()

    # Instance registration
    cluster.register_instance(instance_id)
    cluster.heartbeat(instance_id)
"""

import asyncio
import json
import logging
import os
import socket
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class DistributedLock:
    """Redis-based distributed lock with automatic expiry."""

    def __init__(self, redis, key: str, timeout: int = 30):
        self._redis = redis
        self._key = f"lock:{key}"
        self._timeout = timeout
        self._token = str(uuid.uuid4())

    async def acquire(self) -> bool:
        """Acquire the lock. Returns True if successful."""
        try:
            acquired = await self._redis.set(
                self._key, self._token, nx=True, ex=self._timeout
            )
            return acquired is not None
        except Exception as e:
            logger.warning("Failed to acquire lock %s: %s", self._key, e)
            return False

    async def release(self) -> bool:
        """Release the lock. Returns True if this holder owned it."""
        try:
            current = await self._redis.get(self._key)
            if current == self._token:
                await self._redis.delete(self._key)
                return True
            return False
        except Exception as e:
            logger.warning("Failed to release lock %s: %s", self._key, e)
            return False

    async def __aenter__(self):
        acquired = await self.acquire()
        if not acquired:
            raise RuntimeError(f"Could not acquire lock: {self._key}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()
        return False


class SharedJobQueue:
    """Redis-based job queue for cross-instance processing."""

    _QUEUE_KEY = "Greenie:jobs:queue"
    _CLAIMED_KEY = "Greenie:jobs:claimed"
    _DLQ_KEY = "Greenie:jobs:dlq"

    def __init__(self, redis):
        self._redis = redis

    async def push(self, job: Dict[str, Any]) -> str:
        """Push a job to the queue. Returns the job ID."""
        job_id = job.get("id", f"job-{uuid.uuid4().hex[:8]}")
        job["id"] = job_id
        job["queued_at"] = datetime.now(timezone.utc).isoformat()
        await self._redis.rpush(self._QUEUE_KEY, json.dumps(job))
        return job_id

    async def pop(self, instance_id: str, timeout: int = 5) -> Optional[Dict[str, Any]]:
        """Pop a job from the queue with blocking. Returns None on timeout."""
        try:
            result = await self._redis.brpop(self._QUEUE_KEY, timeout=timeout)
            if result is None:
                return None
            _, job_json = result
            job = json.loads(job_json)
            job["claimed_by"] = instance_id
            job["claimed_at"] = datetime.now(timezone.utc).isoformat()
            # Track claimed jobs
            await self._redis.hset(self._CLAIMED_KEY, job["id"], json.dumps(job))
            return job
        except Exception as e:
            logger.warning("Failed to pop job from queue: %s", e)
            return None

    async def complete(self, job_id: str, result: Any = None) -> bool:
        """Mark a job as completed."""
        try:
            job_json = await self._redis.hget(self._CLAIMED_KEY, job_id)
            if job_json:
                job = json.loads(job_json)
                job["state"] = "completed"
                job["result"] = result
                job["completed_at"] = datetime.now(timezone.utc).isoformat()
                await self._redis.hset(self._CLAIMED_KEY, job_id, json.dumps(job))
                return True
            return False
        except Exception as e:
            logger.warning("Failed to complete job %s: %s", job_id, e)
            return False

    async def fail(self, job_id: str, error: str, max_retries: int = 3, current_retry: int = 0) -> bool:
        """Handle a failed job — retry or move to dead letter queue."""
        try:
            job_json = await self._redis.hget(self._CLAIMED_KEY, job_id)
            if not job_json:
                return False
            job = json.loads(job_json)
            if current_retry < max_retries:
                job["retries"] = current_retry + 1
                job["last_error"] = error
                job["state"] = "pending"
                await self._redis.hdel(self._CLAIMED_KEY, job_id)
                await self._redis.rpush(self._QUEUE_KEY, json.dumps(job))
            else:
                job["state"] = "dead"
                job["last_error"] = error
                job["dead_at"] = datetime.now(timezone.utc).isoformat()
                await self._redis.hdel(self._CLAIMED_KEY, job_id)
                await self._redis.rpush(self._DLQ_KEY, json.dumps(job))
            return True
        except Exception as e:
            logger.warning("Failed to handle job failure %s: %s", job_id, e)
            return False

    async def size(self) -> int:
        """Get the number of pending jobs."""
        try:
            return await self._redis.llen(self._QUEUE_KEY)
        except Exception:
            return 0

    async def claimed_count(self) -> int:
        """Get the number of claimed (in-progress) jobs."""
        try:
            return await self._redis.hlen(self._CLAIMED_KEY)
        except Exception:
            return 0

    async def dlq_count(self) -> int:
        """Get the number of dead letter jobs."""
        try:
            return await self._redis.llen(self._DLQ_KEY)
        except Exception:
            return 0

    async def get_dead_letter(self) -> List[Dict[str, Any]]:
        """Get all jobs in the dead letter queue."""
        try:
            items = await self._redis.lrange(self._DLQ_KEY, 0, -1)
            return [json.loads(item) for item in items]
        except Exception:
            return []

    async def get_claimed(self) -> List[Dict[str, Any]]:
        """Get all claimed (in-progress) jobs."""
        try:
            values = await self._redis.hvals(self._CLAIMED_KEY)
            return [json.loads(v) for v in values]
        except Exception:
            return []


class InstanceRegistry:
    """Track active instances with heartbeat-based health monitoring."""

    _INSTANCES_KEY = "Greenie:instances"
    _HEARTBEAT_INTERVAL = 10  # seconds
    _HEARTBEAT_TIMEOUT = 30  # seconds

    def __init__(self, redis):
        self._redis = redis

    async def register(self, instance_id: str, host: str = "", port: int = 0) -> Dict[str, Any]:
        """Register an instance in the cluster."""
        instance = {
            "id": instance_id,
            "host": host or socket.gethostname(),
            "port": port,
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "last_heartbeat": datetime.now(timezone.utc).isoformat(),
            "status": "active",
        }
        try:
            await self._redis.hset(self._INSTANCES_KEY, instance_id, json.dumps(instance))
            await self._redis.expire(self._INSTANCES_KEY, 3600)  # 1 hour TTL, renewed on heartbeat
            logger.info("Registered instance %s at %s:%d", instance_id, instance["host"], instance["port"])
        except Exception as e:
            logger.warning("Failed to register instance: %s", e)
        return instance

    async def heartbeat(self, instance_id: str) -> bool:
        """Update heartbeat for an instance."""
        try:
            instance_json = await self._redis.hget(self._INSTANCES_KEY, instance_id)
            if instance_json:
                if isinstance(instance_json, bytes):
                    instance_json = instance_json.decode()
                instance = json.loads(instance_json)
                instance["last_heartbeat"] = datetime.now(timezone.utc).isoformat()
                instance["status"] = "active"
                await self._redis.hset(self._INSTANCES_KEY, instance_id, json.dumps(instance))
                await self._redis.expire(self._INSTANCES_KEY, 3600)
                return True
            return False
        except Exception as e:
            logger.warning("Failed to update heartbeat: %s", e)
            return False

    async def deregister(self, instance_id: str) -> bool:
        """Remove an instance from the cluster."""
        try:
            deleted = await self._redis.hdel(self._INSTANCES_KEY, instance_id)
            if deleted:
                logger.info("Deregistered instance %s", instance_id)
            return deleted > 0
        except Exception as e:
            logger.warning("Failed to deregister instance: %s", e)
            return False

    async def get_instances(self) -> List[Dict[str, Any]]:
        """Get all registered instances."""
        try:
            values = await self._redis.hvals(self._INSTANCES_KEY)
            # Handle bytes from Redis with decode_responses=False
            decoded_values = []
            for v in values:
                if isinstance(v, bytes):
                    v = v.decode()
                decoded_values.append(v)
            instances = [json.loads(v) for v in decoded_values]
            # Mark stale instances
            now = time.time()
            for inst in instances:
                try:
                    hb_time = datetime.fromisoformat(inst["last_heartbeat"]).timestamp()
                    if now - hb_time > self._HEARTBEAT_TIMEOUT:
                        inst["status"] = "stale"
                except (ValueError, KeyError):
                    inst["status"] = "stale"
            return instances
        except Exception as e:
            logger.warning("Failed to get instances: %s", e)
            return []

    async def get_active_instances(self) -> List[Dict[str, Any]]:
        """Get only active (non-stale) instances."""
        instances = await self.get_instances()
        return [i for i in instances if i["status"] == "active"]

    async def count(self) -> int:
        """Get the number of registered instances."""
        try:
            return await self._redis.hlen(self._INSTANCES_KEY)
        except Exception:
            return 0


class SessionAffinity:
    """Redis-based session affinity for WebSocket connections."""

    _SESSIONS_KEY = "Greenie:sessions"
    _DEFAULT_TTL = 3600  # 1 hour

    def __init__(self, redis):
        self._redis = redis

    async def bind(self, session_id: str, instance_id: str, ttl: int = _DEFAULT_TTL) -> bool:
        """Bind a session to an instance."""
        try:
            await self._redis.hset(self._SESSIONS_KEY, session_id, instance_id)
            await self._redis.expire(self._SESSIONS_KEY, ttl)
            return True
        except Exception as e:
            logger.warning("Failed to bind session: %s", e)
            return False

    async def get_instance(self, session_id: str) -> Optional[str]:
        """Get the instance ID for a session."""
        try:
            result = await self._redis.hget(self._SESSIONS_KEY, session_id)
            if isinstance(result, bytes):
                result = result.decode()
            return result
        except Exception:
            return None

    async def release(self, session_id: str) -> bool:
        """Release a session binding."""
        try:
            return await self._redis.hdel(self._SESSIONS_KEY, session_id) > 0
        except Exception:
            return False

    async def cleanup_stale(self, active_instance_ids: List[str]) -> int:
        """Remove sessions bound to non-active instances."""
        try:
            removed = 0
            sessions = await self._redis.hgetall(self._SESSIONS_KEY)
            for session_id, instance_id in sessions.items():
                if isinstance(session_id, bytes):
                    session_id = session_id.decode()
                if isinstance(instance_id, bytes):
                    instance_id = instance_id.decode()
                if instance_id not in active_instance_ids:
                    await self._redis.hdel(self._SESSIONS_KEY, session_id)
                    removed += 1
            return removed
        except Exception as e:
            logger.warning("Failed to cleanup stale sessions: %s", e)
            return 0


class ClusterManager:
    """Main entry point for cluster functionality.

    Provides lazy initialization with graceful fallback when Redis is unavailable.
    All methods check availability before executing.
    """

    def __init__(self):
        self._redis = None
        self._available = False
        self._instance_id: Optional[str] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

        # Redis connection config from environment
        self._redis_host = os.environ.get("REDIS_HOST", "localhost")
        self._redis_port = int(os.environ.get("REDIS_PORT", "6379"))
        self._redis_password = os.environ.get("REDIS_PASSWORD", "")
        self._redis_db = int(os.environ.get("REDIS_DB", "0"))

        # Sub-components (initialized lazily)
        self._job_queue: Optional[SharedJobQueue] = None
        self._instance_registry: Optional[InstanceRegistry] = None
        self._session_affinity: Optional[SessionAffinity] = None

    @property
    def available(self) -> bool:
        """Check if Redis is available."""
        return self._available

    @property
    def instance_id(self) -> Optional[str]:
        """Get this instance's ID."""
        return self._instance_id

    @property
    def job_queue(self) -> Optional[SharedJobQueue]:
        """Get the shared job queue."""
        return self._job_queue

    @property
    def instance_registry(self) -> Optional[InstanceRegistry]:
        """Get the instance registry."""
        return self._instance_registry

    @property
    def session_affinity(self) -> Optional[SessionAffinity]:
        """Get the session affinity manager."""
        return self._session_affinity

    async def initialize(self) -> bool:
        """Initialize Redis connection and sub-components.

        Returns True if Redis is available, False otherwise.
        """
        try:
            import redis.asyncio as aioredis
        except ImportError:
            logger.info("Redis not available (install redis[asyncio] for multi-instance support)")
            return False

        try:
            self._redis = aioredis.Redis(
                host=self._redis_host,
                port=self._redis_port,
                password=self._redis_password or None,
                db=self._redis_db,
                decode_responses=False,
            )
            # Test connection
            await self._redis.ping()
            self._available = True

            # Initialize sub-components
            self._job_queue = SharedJobQueue(self._redis)
            self._instance_registry = InstanceRegistry(self._redis)
            self._session_affinity = SessionAffinity(self._redis)

            # Generate instance ID
            self._instance_id = f"Greenie-{uuid.uuid4().hex[:8]}"

            # Register this instance
            port = int(os.environ.get("HUB_PORT", "8080"))
            await self._instance_registry.register(self._instance_id, port=port)

            # Start heartbeat
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

            logger.info(
                "Cluster initialized: instance %s connected to Redis at %s:%d",
                self._instance_id, self._redis_host, self._redis_port,
            )
            return True

        except Exception as e:
            logger.info("Redis not available at %s:%d: %s (running in single-instance mode)",
                       self._redis_host, self._redis_port, e)
            self._available = False
            self._redis = None
            return False

    async def shutdown(self) -> None:
        """Clean up cluster resources on shutdown."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        if self._instance_id and self._instance_registry:
            await self._instance_registry.deregister(self._instance_id)

        if self._redis:
            await self._redis.close()
            logger.info("Cluster shutdown complete")

    async def lock(self, key: str, timeout: int = 30) -> DistributedLock:
        """Create a distributed lock.

        Usage:
            async with cluster.lock("resource", timeout=30):
                # critical section
                pass
        """
        if not self._available or self._redis is None:
            raise RuntimeError("Cluster not available")
        return DistributedLock(self._redis, key, timeout)

    async def _heartbeat_loop(self) -> None:
        """Periodically update heartbeat for this instance."""
        if not self._instance_registry or not self._instance_id:
            return
        while True:
            try:
                await asyncio.sleep(10)
                await self._instance_registry.heartbeat(self._instance_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Heartbeat failed: %s", e)

    async def status(self) -> Dict[str, Any]:
        """Get cluster status for health checks."""
        result = {
            "available": self._available,
            "instance_id": self._instance_id,
            "redis_host": self._redis_host,
            "redis_port": self._redis_port,
        }
        if self._available and self._instance_registry:
            instances = await self._instance_registry.get_instances()
            result["instances"] = instances
            result["active_instances"] = len([i for i in instances if i["status"] == "active"])
        return result


# Global singleton
cluster = ClusterManager()
