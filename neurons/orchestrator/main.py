"""
BEAM Orchestrator Entry Point

Run with: python -m neurons.orchestrator.main

The Orchestrator coordinates bandwidth tasks with BeamCore:
- Registers with BeamCore on startup
- Sends periodic heartbeats with status
- Polls BeamCore for transfer assignments
- Manages local worker pool
- Submits proof-of-bandwidth to BeamCore

Architecture:
┌────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATOR                               │
│                                                                    │
│  BeamCore ◀────── Register/Heartbeat ──────▶ Transfer Assignments  │
│      │                                              │              │
│      ▼                                              ▼              │
│  PoB Submission ◀──── Task Results ◀──── Worker Pool               │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘

All coordination flows through BeamCore. Workers connect to BeamCore,
not directly to orchestrators.
"""

import asyncio
import logging
import os
import signal
import socket
import sys
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from core.orchestrator import Orchestrator, get_orchestrator
from core.config import get_settings
# Cluster mode removed - running in standalone mode only
# from core.cluster import (
#     ClusterConfig,
#     ClusterState,
#     ClusterCoordinator,
#     ClusterNode,
#     Region,
# )
from routes import health, orchestrators
from middleware.rate_limiting import RateLimitMiddleware, get_rate_limiter, RateLimitConfig
from middleware.metrics import MetricsMiddleware, get_metrics_collector, get_metrics_response

# ---------------------------------------------------------------------------
# Core API self-registration helpers
# ---------------------------------------------------------------------------
import httpx

_core_api_heartbeat_task: Optional[asyncio.Task] = None


async def _register_with_core_api(settings, hotkey: str) -> bool:
    """Register this orchestrator with the Core API (BeamCore)."""
    url = f"{settings.subnet_core_url}/orchestrators/register"
    local_ip = settings.external_ip or _get_local_ip()
    orch_url = f"http://{local_ip}:{settings.api_port}"

    payload = {
        "hotkey": hotkey,
        "url": orch_url,
        "ip": local_ip,
        "port": settings.api_port,
        "region": settings.region,
        "max_workers": settings.max_workers,
        "uid": None,
        "fee_percentage": settings.fee_percentage,
        "signature": "local",
    }

    logging.getLogger(__name__).info(
        f"Registering with Core API: fee_percentage={settings.fee_percentage}%"
    )
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            logging.getLogger(__name__).info(
                f"Registered with Core API: {data}"
            )
            return True
    except Exception as e:
        logging.getLogger(__name__).warning(
            f"Failed to register with Core API at {url}: {e}"
        )
        return False


async def _send_heartbeat(settings, hotkey: str, worker_count: int = 0, balance_tao: float = -1.0, coldkey_balance_tao: float = -1.0, pending_payments: int = 0) -> bool:
    """Send a heartbeat to the Core API."""
    url = f"{settings.subnet_core_url}/orchestrators/heartbeat"
    payload = {
        "hotkey": hotkey,
        "current_workers": worker_count,
        "avg_bandwidth_mbps": 0.0,
        "total_bytes_relayed": 0,
        "signature": "local",
        "balance_tao": balance_tao,
        "coldkey_balance_tao": coldkey_balance_tao,
        "pending_payments": pending_payments,
        "fee_percentage": settings.fee_percentage,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            return True
    except Exception as e:
        logging.getLogger(__name__).debug(f"Core API heartbeat failed: {e}")
        return False


async def _heartbeat_loop(settings, hotkey: str, get_worker_count, get_balance_info=None):
    """Periodically send heartbeats to the Core API."""
    logger = logging.getLogger(__name__)
    interval = 60  # seconds
    loop = asyncio.get_event_loop()
    while True:
        await asyncio.sleep(interval)
        count = get_worker_count() if callable(get_worker_count) else 0
        balance_tao = -1.0
        coldkey_balance_tao = -1.0
        pending_payments = 0
        if callable(get_balance_info):
            try:
                balance_tao, coldkey_balance_tao, pending_payments = await asyncio.wait_for(
                    loop.run_in_executor(None, get_balance_info),
                    timeout=15.0,
                )
            except asyncio.TimeoutError:
                logger.warning("Balance fetch timed out after 15s")
            except Exception as e:
                logger.warning(f"Balance fetch failed: {e}")
        await _send_heartbeat(settings, hotkey, count, balance_tao, coldkey_balance_tao, pending_payments)


# Configure logging - both console and file
LOG_DIR = os.environ.get("LOG_DIR", "/tmp/beam_logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
log_datefmt = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    datefmt=log_datefmt,
)

# Add file handler for log viewer
file_handler = logging.FileHandler(f"{LOG_DIR}/orchestrator.log")
file_handler.setFormatter(logging.Formatter(log_format, datefmt=log_datefmt))
logging.getLogger().addHandler(file_handler)

logger = logging.getLogger(__name__)

# Global instances
orchestrator: Orchestrator = None
# Cluster mode removed - standalone only
# cluster_coordinator: Optional[ClusterCoordinator] = None
# cluster_state: Optional[ClusterState] = None


def _get_local_ip() -> str:
    """Get the local IP address for cluster communication."""
    try:
        # Create a socket to determine the outbound IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def _parse_cluster_uids(uids_str: str) -> list:
    """Parse comma-separated UID string into list of integers."""
    if not uids_str:
        return []
    return [int(uid.strip()) for uid in uids_str.split(",") if uid.strip()]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global orchestrator

    settings = get_settings()

    # Configure logging level
    logging.getLogger().setLevel(settings.log_level)

    # Initialize rate limiter
    rate_limiter = get_rate_limiter()
    await rate_limiter.start_cleanup()

    # Rate limit configs for legacy endpoints removed
    # All worker/transfer coordination now handled by BeamCore

    # Initialize metrics collector
    metrics_collector = get_metrics_collector()

    # Initialize orchestrator
    orchestrator = get_orchestrator()
    await orchestrator.initialize()

    # ==========================================================================
    # Initialize Subnet Schema (shared beam tables)
    # ==========================================================================
    if settings.database_url:
        try:
            from beam.db.base import init_db as init_subnet_db
            logger.info("Initializing subnet database schema...")
            await init_subnet_db(settings.database_url)
            logger.info("Subnet database schema initialized")
        except ImportError:
            logger.warning("beam.db not available, skipping subnet schema")
        except Exception as e:
            logger.warning(f"Failed to initialize subnet schema: {e}")

    # Client authentication removed - auth handled by BeamCore

    # Link metrics collector to orchestrator
    metrics_collector.set_orchestrator(orchestrator)
    await metrics_collector.start()

    # Start orchestrator background tasks
    await orchestrator.start()

    # Cluster mode removed - standalone only
    # Destination handlers removed - handled by BeamCore
    # Gateway registry removed - handled by BeamCore

    logger.info("=" * 60)
    logger.info("BEAM Orchestrator started")
    logger.info("=" * 60)
    logger.info(f"Hotkey: {orchestrator.hotkey}")
    logger.info(f"Network: {settings.subtensor_network}")
    logger.info(f"Subnet: {settings.netuid}")
    logger.info(f"API: http://{settings.api_host}:{settings.api_port}")
    logger.info("=" * 60)

    # ======================================================================
    # Register with Core API (BeamCore)
    # ======================================================================
    global _core_api_heartbeat_task
    registered = await _register_with_core_api(settings, orchestrator.hotkey)
    if registered:
        logger.info("Successfully registered with Core API")
        # Start periodic heartbeat
        def _get_worker_count():
            try:
                return len(orchestrator.workers) if hasattr(orchestrator, 'workers') else 0
            except Exception:
                return 0

        def _get_balance_info():
            balance = -1.0
            coldkey_balance = -1.0
            pending = 0
            if orchestrator.subtensor and orchestrator.wallet:
                try:
                    bal = orchestrator.subtensor.get_balance(orchestrator.wallet.hotkey.ss58_address)
                    balance = float(bal)
                except Exception as e:
                    logger.debug(f"Failed to fetch hotkey balance: {e}")
                try:
                    ck_bal = orchestrator.subtensor.get_balance(orchestrator.wallet.coldkeypub.ss58_address)
                    coldkey_balance = float(ck_bal)
                except Exception as e:
                    logger.debug(f"Failed to fetch coldkey balance: {e}")
            try:
                if hasattr(orchestrator, '_reward_mgr'):
                    pending = len(orchestrator._reward_mgr._payment_retry_queue)
            except Exception:
                pass
            return balance, coldkey_balance, pending

        _core_api_heartbeat_task = asyncio.create_task(
            _heartbeat_loop(settings, orchestrator.hotkey, _get_worker_count, _get_balance_info)
        )
    else:
        logger.warning("Could not register with Core API - will retry on next startup")

    yield

    # Cleanup
    logger.info("Shutting down BEAM Orchestrator...")

    # Cancel Core API heartbeat
    if _core_api_heartbeat_task and not _core_api_heartbeat_task.done():
        _core_api_heartbeat_task.cancel()
        try:
            await _core_api_heartbeat_task
        except asyncio.CancelledError:
            pass

    await orchestrator.stop()
    await metrics_collector.stop()
    await rate_limiter.stop_cleanup()

    logger.info("BEAM Orchestrator stopped")


# Create FastAPI app
app = FastAPI(
    title="BEAM Orchestrator",
    description="""
BEAM Orchestrator - Decentralized bandwidth mining coordinator.

The Orchestrator connects to BeamCore and:
- Registers with BeamCore on startup
- Sends periodic heartbeats with status updates
- Receives transfer assignments from BeamCore
- Manages local worker pools and task distribution
- Submits proof-of-bandwidth to BeamCore

All worker registration, transfer coordination, and validator communication
is handled centrally by BeamCore.

## Endpoints

### Health
Monitor the Orchestrator's health and view metrics.

### Orchestrators
Registration and heartbeat endpoints for BeamCore communication.
    """,
    version="0.1.0",
    lifespan=lifespan,
)

# Add middleware (order matters - first added = last to process request)
app.add_middleware(MetricsMiddleware, metrics_collector=get_metrics_collector())
app.add_middleware(RateLimitMiddleware, rate_limiter=get_rate_limiter())

# Add CORS middleware if configured
_cors_settings = get_settings()
_cors_origins = _cors_settings.get_cors_origins()
if _cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins,
        allow_credentials=_cors_settings.cors_allow_credentials,
        allow_methods=_cors_settings.get_cors_methods(),
        allow_headers=_cors_settings.get_cors_headers(),
    )
    logger.info(f"CORS enabled for origins: {_cors_origins}")

# Mount route modules
app.include_router(health.router)
app.include_router(orchestrators.router)


# =============================================================================
# Additional API Routes
# =============================================================================

@app.get("/")
async def root():
    """Root endpoint with API info."""
    return {
        "service": "BEAM Orchestrator",
        "version": "0.1.0",
        "description": "Central coordinator for decentralized bandwidth mining",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/state")
async def get_state():
    """Get full Orchestrator state."""
    if orchestrator:
        return orchestrator.get_state()
    return {"error": "Orchestrator not initialized"}


@app.get("/workers/stats")
async def get_worker_stats():
    """Get detailed worker statistics."""
    if orchestrator:
        return orchestrator.get_worker_stats()
    return {"error": "Orchestrator not initialized"}


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    from fastapi.responses import Response

    content, content_type = get_metrics_response()
    return Response(content=content, media_type=content_type)


@app.get("/metrics/json")
async def metrics_json():
    """JSON metrics endpoint for non-Prometheus consumers."""
    metrics_collector = get_metrics_collector()
    rate_limiter = get_rate_limiter()

    return {
        "uptime_seconds": time.time() - metrics_collector._start_time,
        "orchestrator": orchestrator.get_state() if orchestrator else {},
        "rate_limiter": rate_limiter.get_stats(),
    }


# Cluster endpoints removed - standalone mode only


# =============================================================================
# Main
# =============================================================================

def main():
    """Main entry point."""
    settings = get_settings()

    # Handle signals
    def signal_handler(sig, frame):
        logger.info("Shutdown signal received")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Print banner
    cluster_mode = "STANDALONE"  # Cluster mode removed
    print(f"""
╔════════════════════════════════════════════════════════════════════╗
║                                                                    ║
║        ██████╗ ███████╗ █████╗ ███╗   ███╗                        ║
║        ██╔══██╗██╔════╝██╔══██╗████╗ ████║                        ║
║        ██████╔╝█████╗  ███████║██╔████╔██║                        ║
║        ██╔══██╗██╔══╝  ██╔══██║██║╚██╔╝██║                        ║
║        ██████╔╝███████╗██║  ██║██║ ╚═╝ ██║                        ║
║        ╚═════╝ ╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝                        ║
║                                                                    ║
║                       ORCHESTRATOR                                 ║
║            Decentralized Bandwidth Mining Coordinator              ║
║                                                                    ║
║                     Mode: {cluster_mode:^12}                          ║
║                                                                    ║
╚════════════════════════════════════════════════════════════════════╝
    """)

    # Auto-open log viewer in browser (disabled by default, set OPEN_LOG_VIEWER=true to enable)
    if os.environ.get("OPEN_LOG_VIEWER", "").lower() in ("true", "1", "yes"):
        import webbrowser
        import threading
        log_viewer_url = os.environ.get("LOG_VIEWER_URL", "http://localhost:8080/logs/")
        def open_logs():
            time.sleep(1.5)  # Wait for server to start
            webbrowser.open(log_viewer_url)
        threading.Thread(target=open_logs, daemon=True).start()

    # Run server
    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
