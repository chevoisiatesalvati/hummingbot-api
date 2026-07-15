"""Tests for executor creation DB persistence race fix."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from services.executor_service import ExecutorService


def _metadata(executor_id: str) -> dict:
    return {
        "account_name": "master_account",
        "connector_name": "hyperliquid_perpetual",
        "trading_pair": "kPEPE-USD",
        "executor_type": "position_executor",
        "controller_id": "main",
        "created_at": datetime.now(timezone.utc),
        "config": {"type": "position_executor", "id": executor_id},
    }


def _build_service() -> tuple[ExecutorService, AsyncMock]:
    session = MagicMock()
    mock_repo = AsyncMock()
    mock_repo.create_executor = AsyncMock(return_value=MagicMock())

    @asynccontextmanager
    async def session_context():
        yield session

    db_manager = MagicMock()
    db_manager.get_session_context = session_context

    trading_service = MagicMock()
    svc = ExecutorService(trading_service=trading_service, db_manager=db_manager)
    return svc, mock_repo


def test_persist_executor_created_uses_passed_metadata_when_memory_cleared():
    async def _run() -> None:
        executor_id = "6zV7fWhR28yrnTxshpz15S1cP6pM2qG2FKh7AFVxUHHa"
        metadata = _metadata(executor_id)
        svc, mock_repo = _build_service()

        executor = MagicMock()
        executor.status.name = "TERMINATED"

        # Simulate control-loop race: in-memory metadata already deleted.
        svc._executor_metadata.clear()

        with patch(
            "services.executor_service.ExecutorRepository",
            return_value=mock_repo,
        ):
            await svc._persist_executor_created(executor_id, executor, metadata)

        mock_repo.create_executor.assert_awaited_once()
        kwargs = mock_repo.create_executor.await_args.kwargs
        assert kwargs["executor_id"] == executor_id
        assert kwargs["executor_type"] == "position_executor"
        assert kwargs["account_name"] == "master_account"
        assert kwargs["connector_name"] == "hyperliquid_perpetual"
        assert kwargs["trading_pair"] == "kPEPE-USD"
        assert kwargs["status"] == "TERMINATED"

    asyncio.run(_run())


def test_control_loop_skips_completion_while_creation_persist_pending():
    async def _run() -> None:
        executor_id = "pending-create-id"
        svc = ExecutorService(trading_service=MagicMock(), db_manager=None)
        executor = MagicMock()
        executor.is_closed = True
        svc._active_executors[executor_id] = executor
        svc._executors_pending_creation_persist.add(executor_id)

        completed_ids = []
        for eid, ex in svc._active_executors.items():
            if ex.is_closed:
                if eid in svc._executors_pending_creation_persist:
                    continue
                completed_ids.append(eid)

        assert completed_ids == []

        svc._executors_pending_creation_persist.discard(executor_id)
        completed_ids = []
        for eid, ex in svc._active_executors.items():
            if ex.is_closed:
                if eid in svc._executors_pending_creation_persist:
                    continue
                completed_ids.append(eid)

        assert completed_ids == [executor_id]

    asyncio.run(_run())
