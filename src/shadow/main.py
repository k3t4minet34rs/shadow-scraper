import asyncio
import logging
import os
import signal
from datetime import datetime, timezone
from .config import load_config
from .ws_activity import ActivityWSClient
from .leaders import LeaderManager
from .risk import RiskEngine
from .execution import ExecutionClient
from .strategy import MirrorStrategy
from .clickhouse_writer import ClickHouseWriter
from dotenv import load_dotenv
from .position_book import PositionBook
from .state_sync import StateSync
from .redeemer import Redeemer
from .telemetry import init_metrics_otlp
import contextlib
load_dotenv()

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


async def main() -> None:
    cfg = load_config()
    pos_book = PositionBook()

    exec_client = ExecutionClient()
    risk = RiskEngine(cfg.risk, position_book=pos_book, clob_client=exec_client.client)

    ch_writer = ClickHouseWriter(
        host="localhost",
        port=8123,
        database="default",
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    await ch_writer.start()

    strategy = MirrorStrategy(
        risk=risk,
        exec_client=exec_client,
        bankroll_usdc=100.0,
        position_book=pos_book,
        live=cfg.general.live,
        ch_writer=ch_writer,
    )

    state_sync = StateSync(
        strategy=strategy,
        host="localhost",
        port=8123,
        database="default",
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        sync_interval_secs=30.0,
    )

    leader_mgr = LeaderManager(cfg.leaders.wallets)
    init_metrics_otlp(endpoint="http://localhost:8428/opentelemetry")
    logger.info("Hydrating leader positions from Data API...")

    await asyncio.to_thread(leader_mgr.hydrate_positions_from_data_api)
    logger.info("Hydration complete")

    # Hydrate bot state from ClickHouse (restores seq counts, entered tokens,
    # open positions from earlier in the same UTC day) then start the 30s sync loop.
    await state_sync.start()

    # Start auto-redeemer — claims settled positions every 5 minutes so
    # USDC is recycled back into the wallet without manual intervention.
    redeemer = Redeemer()
    await redeemer.start()

    stop_event = asyncio.Event()

    def _handle_signal() -> None:
        logger.info("Received shutdown signal, closing positions...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    async def handle_activity(payload: dict, recv_time: float):
        classified = leader_mgr.classify_trade(payload)
        if not classified:
            return
        wallet, token_id, size, price, side, kind = classified

        # Write every classified leader trade to ClickHouse regardless of
        # what the strategy decides — this is the raw data for analysis.
        ts = payload.get("timestamp")
        event_time = (
            datetime.fromtimestamp(ts, tz=timezone.utc)
            if ts
            else datetime.now(timezone.utc)
        )
        ch_writer.enqueue_leader_trade(
            event_time=event_time,
            recv_time=datetime.fromtimestamp(recv_time, tz=timezone.utc),
            wallet=wallet,
            token_id=token_id,
            event_slug=payload.get("slug") or "",
            side=side,
            price=price,
            size=size,
            classified=kind,
        )

        await strategy.on_leader_trade(
            wallet=wallet,
            token_id=token_id,
            side=side,
            price=price,
            size=size,
            kind=kind,
            payload=payload,
            recv_time=recv_time,
        )

    ws_client = ActivityWSClient(cfg.ws.activity_url, on_trade=handle_activity)

    # Run WS + balance poller concurrently; wait for stop signal.
    ws_task = asyncio.create_task(ws_client.run())
    balance_task = asyncio.create_task(risk.run_balance_loop(interval_secs=30.0))
    await stop_event.wait()

    # Shutdown: cancel background tasks, flush state + ClickHouse queue.
    balance_task.cancel()
    ws_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await balance_task
    with contextlib.suppress(asyncio.CancelledError):
        await ws_task
    await redeemer.stop()
    await state_sync.stop()   # final state flush before closing the writer
    await ch_writer.stop()

def main_sync() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    main_sync()
