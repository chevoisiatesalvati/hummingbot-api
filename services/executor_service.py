"""
ExecutorService manages executor lifecycle and orchestration.
This service enables running Hummingbot executors directly via API
without Docker containers or full strategy setup.
"""
import asyncio
import json
import logging
import time
import types
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Type

from fastapi import HTTPException
from hummingbot.strategy_v2.executors.arbitrage_executor.arbitrage_executor import ArbitrageExecutor
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ExecutorConfigBase
from hummingbot.strategy_v2.executors.dca_executor.data_types import DCAExecutorConfig
from hummingbot.strategy_v2.executors.dca_executor.dca_executor import DCAExecutor
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.grid_executor.data_types import GridExecutorConfig
from hummingbot.strategy_v2.executors.grid_executor.grid_executor import GridExecutor
from hummingbot.strategy_v2.executors.lp_executor.data_types import LPExecutorConfig
from hummingbot.strategy_v2.executors.lp_executor.lp_executor import LPExecutor
from hummingbot.strategy_v2.executors.order_executor.data_types import OrderExecutorConfig
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
from hummingbot.strategy_v2.executors.twap_executor.data_types import TWAPExecutorConfig
from hummingbot.strategy_v2.executors.twap_executor.twap_executor import TWAPExecutor
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig
from hummingbot.strategy_v2.executors.xemm_executor.xemm_executor import XEMMExecutor
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

from database import AsyncDatabaseManager, ExecutorRepository
from models.executors import PositionHold
from services.trading_service import AccountTradingInterface, TradingService
from utils.executor_log_capture import ExecutorLogCapture, current_executor_id

logger = logging.getLogger(__name__)


def _json_default(obj):
    """JSON serializer for objects not serializable by default."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, Enum):
        return obj.name
    if isinstance(obj, TrackedOrder):
        return {
            "order_id": obj.order_id,
            "price": float(obj.price) if obj.price else None,
            "executed_amount_base": float(obj.executed_amount_base) if obj.executed_amount_base else 0.0,
            "executed_amount_quote": float(obj.executed_amount_quote) if obj.executed_amount_quote else 0.0,
            "is_filled": obj.is_filled if hasattr(obj, 'is_filled') else False,
            "is_open": obj.is_open if hasattr(obj, 'is_open') else False,
        }
    # Handle Pydantic models
    if hasattr(obj, 'model_dump'):
        return obj.model_dump(mode='json')
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _coerce_json_compatible(obj):
    """Recursively coerce a value into JSON-compatible primitives.

    Mirrors the result of ``json.loads(json.dumps(obj, default=_json_default))``
    without the string round-trip: containers are walked recursively and any
    object handled by ``_json_default`` is coerced to the same output type.
    """
    # JSON-native primitives are returned as-is.
    if obj is None or isinstance(obj, (str, bool, int, float)):
        return obj
    if isinstance(obj, dict):
        # json.dumps coerces non-string scalar keys (int/float/bool/None) to
        # strings; replicate that so the output shape is identical.
        coerced = {}
        for key, value in obj.items():
            if isinstance(key, str):
                str_key = key
            elif isinstance(key, bool):
                str_key = "true" if key else "false"
            elif key is None:
                str_key = "null"
            elif isinstance(key, (int, float)):
                str_key = json.dumps(key)
            else:
                raise TypeError(
                    f"keys must be str, int, float, bool or None, not {type(key).__name__}"
                )
            coerced[str_key] = _coerce_json_compatible(value)
        return coerced
    if isinstance(obj, (list, tuple)):
        # json.dumps serializes tuples as JSON arrays (-> lists on decode).
        return [_coerce_json_compatible(item) for item in obj]
    # Non-native types: route through the same coercion as the JSON encoder,
    # then recurse into the (possibly nested) replacement value.
    return _coerce_json_compatible(_json_default(obj))


class ExecutorService:
    """
    Service for managing trading executors without Docker containers.

    This service provides:
    - Dynamic executor creation for any market/connector
    - Executor lifecycle management (start, stop, cleanup)
    - Real-time executor status monitoring
    - Database persistence of executor state and history
    """

    # Mapping of executor type strings to (executor_class, config_class)
    EXECUTOR_REGISTRY: Dict[str, tuple[Type[ExecutorBase], Type[ExecutorConfigBase]]] = {
        "position_executor": (PositionExecutor, PositionExecutorConfig),
        "grid_executor": (GridExecutor, GridExecutorConfig),
        "dca_executor": (DCAExecutor, DCAExecutorConfig),
        "arbitrage_executor": (ArbitrageExecutor, ArbitrageExecutorConfig),
        "twap_executor": (TWAPExecutor, TWAPExecutorConfig),
        "xemm_executor": (XEMMExecutor, XEMMExecutorConfig),
        "order_executor": (OrderExecutor, OrderExecutorConfig),
        "lp_executor": (LPExecutor, LPExecutorConfig),
    }

    def __init__(
        self,
        trading_service: TradingService,
        db_manager: AsyncDatabaseManager,
        default_account: str = "master_account",
        update_interval: float = 1.0,
        max_retries: int = 10
    ):
        """
        Initialize ExecutorService.

        Args:
            trading_service: TradingService for trading operations and interfaces
            db_manager: AsyncDatabaseManager for persistence
            default_account: Default account to use
            update_interval: Executor update interval in seconds
            max_retries: Maximum retries for executor operations
        """
        self._trading_service = trading_service
        self.db_manager = db_manager
        self.default_account = default_account
        self.update_interval = update_interval
        self.max_retries = max_retries

        # Trading interfaces per account (lazy initialized via TradingService)
        self._trading_interfaces: Dict[str, AccountTradingInterface] = {}

        # Active executors: executor_id -> executor instance
        self._active_executors: Dict[str, ExecutorBase] = {}

        # Executor metadata: executor_id -> metadata dict
        self._executor_metadata: Dict[str, Dict[str, Any]] = {}

        # Position holds: key = "account_name|connector_name|trading_pair"
        # Tracks aggregated positions from executors stopped with keep_position=True
        self._positions_held: Dict[str, PositionHold] = {}

        # Executor log capture
        self._log_capture = ExecutorLogCapture()
        self._log_capture.install()

        # Control loop task
        self._control_loop_task: Optional[asyncio.Task] = None
        self._is_running = False

        # Connectors whose exchange open orders were imported during this recovery pass.
        self._recovery_open_orders_imported: set[str] = set()
        self._recovery_claimed_legs: set[tuple] = set()
        # Shared HL position snapshots during startup recovery/cleanup (avoids N× API calls).
        self._startup_positions_cache: Optional[Dict[tuple, Dict[str, Dict]]] = None
        self._recovery_task: Optional[asyncio.Task] = None
        self._recovery_in_progress = False
        # Executors waiting for their initial DB insert (control loop must not complete them yet).
        self._executors_pending_creation_persist: set[str] = set()
        # Cached HL userFills keyed by (account, connector); avoids 429 bursts on executor search.
        self._hl_fills_cache: Dict[tuple[str, str], tuple[float, Dict[str, List[Dict[str, Any]]]]] = {}
        self._hl_fills_cache_ttl_seconds = 300.0
        self._hl_fills_fetch_locks: Dict[tuple[str, str], asyncio.Lock] = {}
        self._hl_fills_last_error: Optional[str] = None

    def schedule_startup_recovery(self) -> None:
        """Kick off DB executor recovery in the background (non-blocking startup)."""
        if self._recovery_task and not self._recovery_task.done():
            logger.debug("Startup recovery already in progress")
            return
        self._recovery_task = asyncio.create_task(
            self._run_startup_recovery_guarded(),
            name="executor_startup_recovery",
        )
        logger.info("Scheduled background executor recovery")

    @property
    def recovery_in_progress(self) -> bool:
        return self._recovery_in_progress

    async def _run_startup_recovery_guarded(self) -> None:
        self._recovery_in_progress = True
        try:
            await self.run_startup_recovery()
            logger.info("Background executor recovery finished")
        except asyncio.CancelledError:
            logger.info("Background executor recovery cancelled")
            raise
        except Exception as exc:
            logger.error("Background executor recovery failed: %s", exc, exc_info=True)
        finally:
            self._recovery_in_progress = False

    def _begin_startup_positions_batch(self) -> None:
        self._startup_positions_cache = {}

    def _end_startup_positions_batch(self) -> None:
        self._startup_positions_cache = None

    async def _resolve_positions(
        self,
        account_name: str,
        connector_name: str,
    ) -> Dict[str, Dict]:
        """Return exchange positions, reusing one snapshot per account/connector during startup."""
        cache = self._startup_positions_cache
        if cache is not None:
            key = (account_name, connector_name)
            if key not in cache:
                cache[key] = await self._trading_service.get_positions(
                    account_name, connector_name
                )
            return cache[key]
        return await self._trading_service.get_positions(account_name, connector_name)

    async def run_startup_recovery(self) -> None:
        """Run DB recovery and orphan cleanup with batched position snapshots."""
        self._begin_startup_positions_batch()
        try:
            await self.recover_running_executors_from_db()
            await self.cleanup_orphaned_executors()
            await self.recover_positions_from_db()
        finally:
            self._end_startup_positions_batch()

    def start(self):
        """Start the executor service control loop."""
        if not self._is_running:
            self._is_running = True
            self._control_loop_task = asyncio.create_task(self._control_loop())
            logger.info("ExecutorService started")

    async def recover_positions_from_db(self):
        """
        Recover position holds from the dedicated position_holds table on startup.
        """
        if not self.db_manager:
            return

        try:
            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)

                records = await repo.get_active_position_holds()

                for record in records:
                    controller_id = record.controller_id or "main"
                    position_key = self._get_position_key(
                        record.account_name,
                        record.connector_name,
                        record.trading_pair,
                        controller_id
                    )

                    executor_ids = []
                    if record.executor_ids:
                        try:
                            executor_ids = json.loads(record.executor_ids)
                        except (json.JSONDecodeError, TypeError):
                            pass

                    position = PositionHold(
                        trading_pair=record.trading_pair,
                        connector_name=record.connector_name,
                        account_name=record.account_name,
                        controller_id=controller_id,
                        buy_amount_base=Decimal(str(record.buy_amount_base or 0)),
                        buy_amount_quote=Decimal(str(record.buy_amount_quote or 0)),
                        sell_amount_base=Decimal(str(record.sell_amount_base or 0)),
                        sell_amount_quote=Decimal(str(record.sell_amount_quote or 0)),
                        realized_pnl_quote=Decimal(str(record.realized_pnl_quote or 0)),
                        cum_fees_quote=Decimal(str(record.cum_fees_quote or 0)),
                        executor_ids=executor_ids,
                        last_updated=record.last_updated,
                    )
                    # Settle any matched volume from legacy unsettled data
                    position._calculate_realized_pnl()
                    self._positions_held[position_key] = position

                if self._positions_held:
                    logger.info(f"Recovered {len(self._positions_held)} position holds from database")

        except Exception as e:
            logger.error(f"Error recovering positions from database: {e}", exc_info=True)

    async def recover_running_executors_from_db(self):
        """
        Rehydrate RUNNING executors from the database after an API restart.

        Without this step, ``cleanup_orphaned_executors`` treats every in-memory
        miss as an orphan and marks live legs TERMINATED (SYSTEM_CLEANUP) even
        when the exchange position is still open.
        """
        if not self.db_manager:
            logger.debug("No database manager available, skipping executor recovery")
            return

        recovered = 0
        failed: list[str] = []

        try:
            async with self.db_manager.get_session_context() as session:
                from database.repositories.executor_repository import ExecutorRepository
                repo = ExecutorRepository(session)
                records = await repo.get_active_executors()
                misterminated = await repo.get_misterminated_position_executors()

            seen_ids = set()
            all_records = []
            for record in records + misterminated:
                if record.executor_id in seen_ids:
                    continue
                seen_ids.add(record.executor_id)
                all_records.append(record)

            # Prefer the newest executor when several DB rows claim the same exchange leg.
            all_records.sort(
                key=lambda r: r.created_at or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )
            self._recovery_claimed_legs = set()

            for record in all_records:
                if record.executor_id in self._active_executors:
                    continue
                try:
                    leg_key = self._position_leg_key(record)
                    if leg_key and leg_key in self._recovery_claimed_legs:
                        await self._terminate_executor_record(
                            record.executor_id, "STALE_DUPLICATE"
                        )
                        logger.info(
                            "Terminated stale duplicate executor %s (%s %s)",
                            record.executor_id,
                            record.trading_pair,
                            leg_key[3],
                        )
                        continue

                    if record.status == "TERMINATED":
                        if not await self._record_has_live_exchange_position(record):
                            continue
                        async with self.db_manager.get_session_context() as session:
                            from database.repositories.executor_repository import ExecutorRepository
                            reactivate_repo = ExecutorRepository(session)
                            await reactivate_repo.reactivate_executor(record.executor_id)
                        record.status = "RUNNING"
                        record.close_type = None
                        logger.info(
                            "Reactivating misterminated position executor %s (%s)",
                            record.executor_id,
                            record.trading_pair,
                        )
                    account_name = record.account_name or self.default_account
                    connector_name = record.connector_name
                    if connector_name:
                        await self._ensure_exchange_open_orders_imported(account_name, connector_name)
                    if await self._recover_executor_record(record):
                        recovered += 1
                        if leg_key:
                            self._recovery_claimed_legs.add(leg_key)
                    else:
                        failed.append(record.executor_id)
                        if record.executor_type == "position_executor":
                            await self._terminate_executor_record(
                                record.executor_id, "RECOVERY_FAILED"
                            )
                except Exception as exc:
                    failed.append(record.executor_id)
                    logger.error(
                        "Failed to recover executor %s: %s",
                        record.executor_id,
                        exc,
                        exc_info=True,
                    )
            if recovered:
                logger.info("Recovered %d running executor(s) from database", recovered)
            if failed:
                logger.warning(
                    "Could not recover %d executor(s): %s",
                    len(failed),
                    failed[:10],
                )
        except Exception as e:
            logger.error(f"Error recovering running executors from database: {e}", exc_info=True)

    async def _recover_executor_record(self, record) -> bool:
        """Rebuild one RUNNING executor in memory from a DB record."""
        if not record.config:
            logger.warning(
                "Skipping recovery for %s — missing persisted config",
                record.executor_id,
            )
            return False

        try:
            executor_config = json.loads(record.config)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.warning(
                "Skipping recovery for %s — invalid config JSON: %s",
                record.executor_id,
                exc,
            )
            return False

        if not isinstance(executor_config, dict):
            return False

        executor_type = executor_config.get("type") or record.executor_type
        if not executor_type or executor_type not in self.EXECUTOR_REGISTRY:
            logger.warning(
                "Skipping recovery for %s — unsupported type %s",
                record.executor_id,
                executor_type,
            )
            return False

        executor_config = dict(executor_config)
        executor_config["type"] = executor_type
        executor_config["id"] = record.executor_id
        if record.controller_id and not executor_config.get("controller_id"):
            executor_config["controller_id"] = record.controller_id

        account = record.account_name or self.default_account
        trading_interface = self._get_trading_interface(account)
        connector_name = executor_config.get("connector_name") or record.connector_name
        trading_pair = executor_config.get("trading_pair") or record.trading_pair

        if connector_name:
            if trading_pair:
                await trading_interface.add_market(connector_name, trading_pair)
            else:
                await trading_interface.ensure_connector(connector_name)

        if "timestamp" not in executor_config or executor_config["timestamp"] is None:
            executor_config["timestamp"] = trading_interface.current_timestamp

        executor_class, config_class = self.EXECUTOR_REGISTRY[executor_type]
        try:
            typed_config = config_class(**executor_config)
        except Exception as exc:
            logger.warning(
                "Skipping recovery for %s — invalid typed config: %s",
                record.executor_id,
                exc,
            )
            return False

        try:
            executor = executor_class(
                strategy=trading_interface,
                config=typed_config,
                update_interval=self.update_interval,
                max_retries=self.max_retries,
            )
        except Exception as exc:
            logger.warning(
                "Skipping recovery for %s — failed to instantiate: %s",
                record.executor_id,
                exc,
            )
            return False

        if executor_type == "position_executor":
            attached = await self._attach_position_executor_to_exchange_position(
                executor, account
            )
            if not attached:
                return False

        executor_id = typed_config.id
        controller_id = record.controller_id or getattr(typed_config, "controller_id", "main") or "main"
        created_at = record.created_at or datetime.now(timezone.utc)
        self._active_executors[executor_id] = executor
        self._executor_metadata[executor_id] = {
            "account_name": account,
            "connector_name": connector_name,
            "trading_pair": trading_pair,
            "executor_type": executor_type,
            "controller_id": controller_id,
            "created_at": created_at,
            "config": executor_config,
        }

        token = current_executor_id.set(executor_id)
        executor.start()
        current_executor_id.reset(token)

        if executor.is_closed:
            await self._handle_executor_completion(executor_id)
            return False

        logger.info(
            "Recovered %s executor %s for %s/%s (controller=%s)",
            executor_type,
            executor_id,
            connector_name,
            trading_pair,
            controller_id,
        )
        return True

    async def _attach_position_executor_to_exchange_position(
        self,
        executor: ExecutorBase,
        account_name: str,
    ) -> bool:
        """Seed a PositionExecutor with the live exchange leg after API restart."""
        from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
        from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
        from hummingbot.strategy_v2.models.executors import TrackedOrder

        config = executor.config
        positions = await self._resolve_positions(
            account_name, config.connector_name
        )
        pos = positions.get(config.trading_pair) if positions else None
        if not pos:
            logger.warning(
                "Cannot recover position executor %s — no open %s position on %s",
                config.id,
                config.trading_pair,
                config.connector_name,
            )
            return False

        amount_raw = Decimal(str(pos.get("amount") or 0))
        entry_price = Decimal(str(pos.get("entry_price") or 0))
        if amount_raw.copy_abs() <= Decimal("0") or entry_price <= Decimal("0"):
            logger.warning(
                "Cannot recover position executor %s — empty position snapshot: %s",
                config.id,
                pos,
            )
            return False

        position_side = str(pos.get("position_side") or "").upper()
        exchange_is_short = position_side == "SHORT" or amount_raw < 0
        config_is_short = config.side == TradeType.SELL
        if exchange_is_short != config_is_short:
            logger.warning(
                "Cannot recover position executor %s — side mismatch "
                "(config=%s exchange=%s position_side=%s amount=%s)",
                config.id,
                config.side,
                "short" if exchange_is_short else "long",
                position_side,
                amount_raw,
            )
            return False

        amount = amount_raw.copy_abs()
        config_amount = Decimal(str(config.amount or 0))
        if config_amount > 0:
            rel_diff = (amount - config_amount).copy_abs() / config_amount
            if rel_diff > Decimal("0.15"):
                logger.warning(
                    "Recovering position executor %s with exchange amount %s "
                    "(config amount %s, %.1f%% drift)",
                    config.id,
                    amount,
                    config_amount,
                    float(rel_diff * 100),
                )
            config.amount = amount

        trading_interface = self._get_trading_interface(account_name)
        if not config.entry_price:
            config.entry_price = entry_price

        client_order_id = f"recovered_{config.id[:16]}"
        open_order = InFlightOrder(
            client_order_id=client_order_id,
            trading_pair=config.trading_pair,
            order_type=OrderType.MARKET,
            trade_type=config.side,
            amount=amount,
            creation_timestamp=trading_interface.current_timestamp,
            price=entry_price,
            exchange_order_id=f"recovered_{config.id[:16]}",
            initial_state=OrderState.FILLED,
            leverage=int(getattr(config, "leverage", 1) or 1),
            position=PositionAction.OPEN,
        )
        open_order.executed_amount_base = amount
        open_order.executed_amount_quote = amount * entry_price
        open_order.completely_filled_event.set()

        tracked = TrackedOrder(order_id=client_order_id)
        tracked.order = open_order
        executor._open_order = tracked

        uses_limit_tp = (
            config.triple_barrier_config.take_profit
            and config.triple_barrier_config.take_profit_order_type.is_limit_type()
        )
        adopted = await self._adopt_take_profit_limit_order_from_connector(
            executor, account_name
        )
        if uses_limit_tp and hasattr(executor, "_suppress_take_profit_limit_after_recovery"):
            executor._suppress_take_profit_limit_after_recovery = not adopted

        self._mark_position_executor_recovered(executor)
        return True

    async def _validate_position_executor_order_size(
        self,
        account_name: str,
        executor_config: Dict[str, Any],
    ) -> None:
        """Reject position executors whose open order would fail connector min notional."""
        connector_name = executor_config.get("connector_name")
        trading_pair = executor_config.get("trading_pair")
        amount_raw = executor_config.get("amount")
        if not connector_name or not trading_pair or amount_raw is None:
            return
        if "hyperliquid" not in connector_name:
            return

        amount = Decimal(str(amount_raw))
        trading_interface = self._get_trading_interface(account_name)
        connector = await trading_interface.ensure_connector(connector_name)
        rules = connector.trading_rules.get(trading_pair)
        if not rules:
            return

        quantized = connector.quantize_order_amount(trading_pair, amount)
        try:
            price = connector.get_price(trading_pair, True)
        except Exception:
            price = Decimal(str(executor_config.get("entry_price") or 0))
        if price <= 0:
            return

        notional = quantized * price
        min_notional = rules.min_notional_size
        if notional < min_notional:
            min_amount = (min_notional / price).quantize(quantized)
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Order notional ${notional:.2f} is below Hyperliquid minimum "
                    f"${min_notional} for {trading_pair} "
                    f"(amount {amount} quantizes to {quantized} at ${price:.2f}). "
                    f"Use at least ~{min_amount} base units."
                ),
            )

    async def _reconcile_position_executor_stale_open_order(
        self,
        executor: ExecutorBase,
        account_name: str,
    ) -> bool:
        """Clear ghost open orders when connector tracking ended but the executor still waits."""
        from hummingbot.core.data_type.in_flight_order import OrderState, OrderUpdate
        from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor

        STUCK_SUBMIT_SECONDS = 30

        if not isinstance(executor, PositionExecutor):
            return False
        if executor.is_closed:
            return False
        if executor.open_filled_amount > Decimal("0"):
            return False

        open_tracked = executor._open_order
        if not open_tracked or not open_tracked.order_id:
            return False

        order_id = open_tracked.order_id
        connector = await self._get_trading_interface(account_name).ensure_connector(
            executor.config.connector_name
        )
        tracker = connector._order_tracker
        live = tracker.fetch_order(client_order_id=order_id)
        lost = tracker.fetch_lost_order(client_order_id=order_id)

        reconciled = False
        failure_reason = None

        if live and not live.is_done:
            if open_tracked.order is None:
                open_tracked.order = live
            order_age = connector.current_timestamp - live.creation_timestamp
            if (
                not live.exchange_order_id
                and order_age > STUCK_SUBMIT_SECONDS
                and live.executed_amount_base <= Decimal("0")
                and not live.is_failure
            ):
                failure_reason = "SUBMIT_TIMEOUT"
                reconciled = True
                order_update = OrderUpdate(
                    client_order_id=order_id,
                    trading_pair=live.trading_pair,
                    update_timestamp=connector.current_timestamp,
                    new_state=OrderState.FAILED,
                    misc_updates={
                        "error_message": (
                            f"Order submission timed out after {order_age:.1f}s "
                            "without an exchange order id"
                        ),
                        "error_type": "TimeoutError",
                    },
                )
                tracker.process_order_update(order_update)
            elif not reconciled:
                return False

        if not reconciled:
            if live and (live.is_failure or (live.is_cancelled and live.executed_amount_base <= 0)):
                failure_reason = live.current_state.name
                reconciled = True
            elif lost and (lost.is_failure or (lost.is_cancelled and lost.executed_amount_base <= 0)):
                failure_reason = lost.current_state.name
                reconciled = True
            elif live is None and lost is None and open_tracked.order is None:
                failure_reason = "NOT_TRACKED"
                reconciled = True

        if not reconciled:
            return False

        executor._failed_orders.append(open_tracked)
        executor._open_order = None
        executor._current_retries += 1
        logger.warning(
            "Reconciled stale open order for executor %s (%s): order=%s reason=%s retry=%s/%s",
            executor.config.id,
            executor.config.trading_pair,
            order_id,
            failure_reason,
            executor._current_retries,
            executor._max_retries,
        )
        return True

    async def _reconcile_order_executor_stale_order(
        self,
        executor: ExecutorBase,
        account_name: str,
    ) -> bool:
        """Clear ghost orders for order executors stuck without an exchange order id."""
        from hummingbot.core.data_type.in_flight_order import OrderState, OrderUpdate
        from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor
        from hummingbot.strategy_v2.models.base import RunnableStatus

        STUCK_SUBMIT_SECONDS = 30

        if not isinstance(executor, OrderExecutor):
            return False
        if executor.is_closed or executor.status != RunnableStatus.RUNNING:
            return False
        if executor.executed_amount_base > Decimal("0"):
            return False

        tracked = executor._order
        if not tracked or not tracked.order_id:
            return False

        connector = await self._get_trading_interface(account_name).ensure_connector(
            executor.config.connector_name
        )
        tracker = connector._order_tracker
        live = tracker.fetch_order(client_order_id=tracked.order_id)
        if live and not tracked.order:
            tracked.order = live

        if not live or live.is_done:
            return False

        order_age = connector.current_timestamp - live.creation_timestamp
        if (
            live.exchange_order_id
            or order_age <= STUCK_SUBMIT_SECONDS
            or live.executed_amount_base > Decimal("0")
            or live.is_failure
        ):
            return False

        order_update = OrderUpdate(
            client_order_id=tracked.order_id,
            trading_pair=live.trading_pair,
            update_timestamp=connector.current_timestamp,
            new_state=OrderState.FAILED,
            misc_updates={
                "error_message": (
                    f"Order submission timed out after {order_age:.1f}s "
                    "without an exchange order id"
                ),
                "error_type": "TimeoutError",
            },
        )
        tracker.process_order_update(order_update)
        return True

    async def _assist_executor_shutdown(
        self,
        executor: ExecutorBase,
        account_name: str,
    ) -> None:
        """Unblock SHUTTING_DOWN executors stuck on ghost or unbound orders."""
        from hummingbot.core.data_type.in_flight_order import OrderState, OrderUpdate
        from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor
        from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
        from hummingbot.strategy_v2.models.base import RunnableStatus

        STUCK_SUBMIT_SECONDS = 30

        if executor.is_closed or executor.status != RunnableStatus.SHUTTING_DOWN:
            return

        if isinstance(executor, OrderExecutor):
            config = executor.config
            connector = await self._get_trading_interface(account_name).ensure_connector(
                config.connector_name
            )
            tracked = executor._order
            if not tracked or not tracked.order_id:
                if executor.executed_amount_base <= Decimal("0"):
                    executor.close_type = CloseType.EARLY_STOP
                    executor.stop()
                return

            live = connector._order_tracker.fetch_order(client_order_id=tracked.order_id)
            if live and not tracked.order:
                tracked.order = live

            if tracked.is_filled:
                return

            if tracked.is_open and live:
                order_age = connector.current_timestamp - live.creation_timestamp
                if (
                    not live.exchange_order_id
                    and order_age > STUCK_SUBMIT_SECONDS
                    and live.executed_amount_base <= Decimal("0")
                ):
                    connector._order_tracker.process_order_update(
                        OrderUpdate(
                            client_order_id=tracked.order_id,
                            trading_pair=live.trading_pair,
                            update_timestamp=connector.current_timestamp,
                            new_state=OrderState.FAILED,
                            misc_updates={
                                "error_message": "Order cleared during executor shutdown",
                                "error_type": "ShutdownClear",
                            },
                        )
                    )
                    executor._order = None
                    executor.close_type = CloseType.EARLY_STOP
                    executor.stop()
                return

            if not tracked.is_open and not tracked.is_filled:
                executor._order = None
                executor.close_type = CloseType.EARLY_STOP
                executor.stop()
            return

        if not isinstance(executor, PositionExecutor):
            return

        config = executor.config
        connector = await self._get_trading_interface(account_name).ensure_connector(
            config.connector_name
        )
        open_tracked = executor._open_order
        if open_tracked and open_tracked.order_id:
            live = connector._order_tracker.fetch_order(client_order_id=open_tracked.order_id)
            if live and not open_tracked.order:
                open_tracked.order = live
            if (
                live
                and open_tracked.is_open
                and not live.exchange_order_id
                and connector.current_timestamp - live.creation_timestamp > STUCK_SUBMIT_SECONDS
                and live.executed_amount_base <= Decimal("0")
            ):
                connector._order_tracker.process_order_update(
                    OrderUpdate(
                        client_order_id=open_tracked.order_id,
                        trading_pair=live.trading_pair,
                        update_timestamp=connector.current_timestamp,
                        new_state=OrderState.FAILED,
                        misc_updates={
                            "error_message": "Open order cleared during executor shutdown",
                            "error_type": "ShutdownClear",
                        },
                    )
                )
                executor._open_order = None
            elif not open_tracked.is_done and open_tracked.order is None:
                executor._open_order = None

        if (
            executor.open_filled_amount <= Decimal("0")
            and executor.all_orders_completed()
            and executor.close_type != CloseType.POSITION_HOLD
        ):
            executor.stop()

    async def _sync_position_executor_open_fill_from_exchange(
        self,
        executor: ExecutorBase,
        account_name: str,
    ) -> bool:
        """Backfill open-order executed amounts from exchange when fill tracking left them at zero."""
        from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
        from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
        from hummingbot.strategy_v2.executors.position_executor.position_executor import PositionExecutor
        from hummingbot.strategy_v2.models.executors import TrackedOrder

        if not isinstance(executor, PositionExecutor):
            return False
        if executor.is_closed:
            return False
        try:
            if executor.open_filled_amount > Decimal("0"):
                return False
        except Exception:
            return False

        config = executor.config
        positions = await self._resolve_positions(
            account_name, config.connector_name
        )
        pos = positions.get(config.trading_pair) if positions else None
        if not pos:
            return False

        amount_raw = Decimal(str(pos.get("amount") or 0))
        entry_price = Decimal(str(pos.get("entry_price") or 0))
        if amount_raw.copy_abs() <= Decimal("0") or entry_price <= Decimal("0"):
            return False

        position_side = str(pos.get("position_side") or "").upper()
        exchange_is_short = position_side == "SHORT" or amount_raw < 0
        config_is_short = config.side == TradeType.SELL
        if exchange_is_short != config_is_short:
            return False

        amount = amount_raw.copy_abs()
        if config.amount and config.amount > 0:
            config.amount = amount
        if not config.entry_price:
            config.entry_price = entry_price

        tracked = executor._open_order
        if tracked and tracked.order:
            order = tracked.order
            order.amount = amount
            order.price = entry_price
            order.executed_amount_base = amount
            order.executed_amount_quote = amount * entry_price
            order.current_state = OrderState.FILLED
            order.completely_filled_event.set()
        else:
            trading_interface = self._get_trading_interface(account_name)
            client_order_id = f"synced_{config.id[:16]}"
            open_order = InFlightOrder(
                client_order_id=client_order_id,
                trading_pair=config.trading_pair,
                order_type=OrderType.MARKET,
                trade_type=config.side,
                amount=amount,
                creation_timestamp=trading_interface.current_timestamp,
                price=entry_price,
                exchange_order_id=client_order_id,
                initial_state=OrderState.FILLED,
                leverage=int(getattr(config, "leverage", 1) or 1),
                position=PositionAction.OPEN,
            )
            open_order.executed_amount_base = amount
            open_order.executed_amount_quote = amount * entry_price
            open_order.completely_filled_event.set()
            tracked = TrackedOrder(order_id=client_order_id)
            tracked.order = open_order
            executor._open_order = tracked

        open_filled = executor.open_filled_amount
        logger.info(
            "Synced open fill for executor %s (%s): amount=%s entry=%s open_filled=%s",
            config.id,
            config.trading_pair,
            amount,
            entry_price,
            open_filled,
        )
        return True

    @staticmethod
    def _is_synthetic_order_id(order_id: Optional[str]) -> bool:
        if not order_id:
            return False
        lowered = str(order_id).lower()
        return lowered.startswith("recovered_") or lowered.startswith("hbpt")

    def _executor_may_need_hl_pnl_repair(self, record) -> bool:
        """Return True for terminated HL executors that may need userFills PnL repair.

        Triggers when:
        - recovered/synthetic open order ids, or
        - stored net PnL is zero despite a real close, or
        - real open+close order ids exist (MARKET closes can be FILLED with null
          average_fill_price, freezing mid as close_price until repaired from fills).
        """
        if (
            record.executor_type != "position_executor"
            or record.status != "TERMINATED"
            or not record.connector_name
            or "hyperliquid" not in record.connector_name
        ):
            return False
        try:
            final_state = json.loads(record.final_state) if record.final_state else {}
        except (json.JSONDecodeError, TypeError):
            final_state = {}
        order_ids = final_state.get("order_ids") or []
        if order_ids and self._is_synthetic_order_id(str(order_ids[0])):
            return True
        if (
            (record.net_pnl_quote or 0) == 0
            and record.close_type
            and record.close_type not in ("STALE_DUPLICATE", "MISTAKE", "MANUAL")
        ):
            return True
        # Real open+close client order ids: attempt HL userFills repair (cached, no-op if
        # fills missing or PnL already matches).
        if (
            len(order_ids) >= 2
            and not self._is_synthetic_order_id(str(order_ids[0]))
            and not self._is_synthetic_order_id(str(order_ids[1]))
        ):
            return True
        return False

    def _get_initialized_hyperliquid_connector(
        self,
        account_name: str,
        connector_name: str,
    ):
        """Return an already-initialized HL connector without triggering a new init."""
        connector_service = self._trading_service.connector_service
        if not connector_service.is_trading_connector_initialized(account_name, connector_name):
            return None
        return connector_service.get_account_connectors(account_name).get(connector_name)

    async def _ensure_hyperliquid_connector(
        self,
        account_name: str,
        connector_name: str,
    ):
        """Return HL connector, initializing credentials/session when needed."""
        connector = self._get_initialized_hyperliquid_connector(account_name, connector_name)
        if connector is not None:
            return connector
        try:
            return await self._get_trading_interface(account_name).ensure_connector(
                connector_name
            )
        except Exception as exc:
            self._hl_fills_last_error = f"ensure_connector failed: {exc}"
            logger.warning(
                "Could not initialize Hyperliquid connector for %s/%s: %s",
                account_name,
                connector_name,
                exc,
            )
            return None

    async def _load_hyperliquid_fills_by_oid(
        self,
        account_name: str,
        connector_name: str,
        *,
        ensure_init: bool = True,
        allow_during_recovery: bool = False,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch HL userFills once and index by exchange / client order id.

        On failure, sets ``self._hl_fills_last_error`` for bulk-repair diagnostics.
        """
        self._hl_fills_last_error = None
        if "hyperliquid" not in connector_name:
            self._hl_fills_last_error = "not a hyperliquid connector"
            return {}
        # Opportunistic get/search skips during recovery to avoid 429; bulk repair
        # must still be able to load fills (caller passes allow_during_recovery=True).
        if self._recovery_in_progress and not allow_during_recovery:
            self._hl_fills_last_error = "recovery_in_progress"
            return {}

        cache_key = (account_name, connector_name)
        cached = self._hl_fills_cache.get(cache_key)
        if cached and (time.monotonic() - cached[0]) < self._hl_fills_cache_ttl_seconds:
            return cached[1]

        if cache_key not in self._hl_fills_fetch_locks:
            self._hl_fills_fetch_locks[cache_key] = asyncio.Lock()

        async with self._hl_fills_fetch_locks[cache_key]:
            cached = self._hl_fills_cache.get(cache_key)
            if cached and (time.monotonic() - cached[0]) < self._hl_fills_cache_ttl_seconds:
                return cached[1]

            if ensure_init:
                connector = await self._ensure_hyperliquid_connector(
                    account_name, connector_name
                )
            else:
                connector = self._get_initialized_hyperliquid_connector(
                    account_name, connector_name
                )
            if connector is None:
                self._hl_fills_last_error = (
                    f"connector not available for {account_name}/{connector_name}"
                )
                logger.warning("Skipping HL fills fetch: %s", self._hl_fills_last_error)
                return {}

            try:
                from hummingbot.connector.derivative.hyperliquid_perpetual import hyperliquid_perpetual_constants as hl_constants

                user_address = getattr(connector, "hyperliquid_perpetual_address", None)
                if not user_address:
                    self._hl_fills_last_error = "connector missing hyperliquid_perpetual_address"
                    return {}

                fills_response = await connector._api_post(
                    path_url=hl_constants.ACCOUNT_TRADE_LIST_URL,
                    data={
                        "type": hl_constants.TRADES_TYPE,
                        "user": user_address,
                    },
                )
                by_oid: Dict[str, List[Dict[str, Any]]] = {}
                for row in fills_response or []:
                    oid = str(row.get("oid", ""))
                    cloid = str(row.get("cloid") or "")
                    if oid:
                        by_oid.setdefault(oid, []).append(row)
                    if cloid:
                        by_oid.setdefault(cloid, []).append(row)
                self._hl_fills_cache[cache_key] = (time.monotonic(), by_oid)
                self._hl_fills_last_error = None
                return by_oid
            except Exception as exc:
                self._hl_fills_last_error = str(exc)
                logger.warning(
                    "Could not load Hyperliquid fills for %s/%s: %s",
                    account_name,
                    connector_name,
                    exc,
                )
                return {}

    @staticmethod
    def _fills_for_oid(fills_by_oid: Dict[str, List[Dict[str, Any]]], oid: str) -> List[Dict[str, Any]]:
        if not oid:
            return []
        return fills_by_oid.get(str(oid), fills_by_oid.get(oid, []))

    @classmethod
    def _fills_for_oids(
        cls,
        fills_by_oid: Dict[str, List[Dict[str, Any]]],
        *oids: Optional[str],
    ) -> List[Dict[str, Any]]:
        """Merge fill rows for any of the given exchange/client order ids (deduped)."""
        out: List[Dict[str, Any]] = []
        seen: set[tuple] = set()
        for oid in oids:
            for row in cls._fills_for_oid(fills_by_oid, oid or ""):
                key = (
                    row.get("tid"),
                    row.get("time"),
                    row.get("px"),
                    row.get("sz"),
                    row.get("side"),
                )
                if key in seen:
                    continue
                seen.add(key)
                out.append(row)
        return out

    @staticmethod
    def _vwap_from_hl_fills(fills: List[Dict[str, Any]]) -> Optional[Decimal]:
        base = Decimal("0")
        quote = Decimal("0")
        for row in fills:
            sz = Decimal(str(row.get("sz", 0)))
            px = Decimal(str(row.get("px", 0)))
            if sz <= 0 or px <= 0:
                continue
            base += sz
            quote += px * sz
        if base <= 0:
            return None
        return quote / base

    @staticmethod
    def _base_from_hl_fills(fills: List[Dict[str, Any]]) -> Decimal:
        total = Decimal("0")
        for row in fills:
            sz = Decimal(str(row.get("sz", 0)))
            if sz > 0:
                total += sz
        return total

    @staticmethod
    def _quote_from_hl_fills(fills: List[Dict[str, Any]]) -> Decimal:
        total = Decimal("0")
        for row in fills:
            sz = Decimal(str(row.get("sz", 0)))
            px = Decimal(str(row.get("px", 0)))
            if sz > 0 and px > 0:
                total += px * sz
        return total

    @staticmethod
    def _fees_from_hl_fills(fills: List[Dict[str, Any]]) -> Decimal:
        return sum((Decimal(str(row.get("fee", 0))) for row in fills), Decimal("0"))

    @staticmethod
    def _closed_pnl_from_hl_fills(fills: List[Dict[str, Any]]) -> Decimal:
        """Sum HL closedPnl on fill rows (pre-fee realized on closing fills)."""
        return sum(
            (Decimal(str(row.get("closedPnl", 0))) for row in fills),
            Decimal("0"),
        )

    @classmethod
    def _hl_fills_have_closed_pnl_field(cls, fills: List[Dict[str, Any]]) -> bool:
        return any("closedPnl" in row for row in fills)

    @classmethod
    def _size_mismatch_from_hl_fills(
        cls,
        open_fills: List[Dict[str, Any]],
        close_fills: List[Dict[str, Any]],
        *,
        rel_tol: Decimal = Decimal("0.01"),
    ) -> bool:
        open_base = cls._base_from_hl_fills(open_fills)
        close_base = cls._base_from_hl_fills(close_fills)
        if open_base <= 0 or close_base <= 0:
            return False
        ratio = abs(open_base - close_base) / max(open_base, close_base)
        return ratio > rel_tol

    @classmethod
    def _compute_realized_pnl_from_hl_fills(
        cls,
        open_fills: List[Dict[str, Any]],
        close_fills: List[Dict[str, Any]],
        is_buy: bool,
    ) -> Optional[tuple[Decimal, Decimal, Decimal]]:
        """Return (net_pnl_quote, net_pnl_pct, filled_amount_quote) from HL userFills.

        Prefer sum(closedPnl) on **close** fills minus open+close fees (HL closedPnl is
        pre-fee). Never use open-leg closedPnl (flip opens realize a prior position).

        Fallback when closedPnl is absent: matched-base VWAP so unequal open/close
        sizes cannot invent phantom PnL from quote notional mismatch.
        """
        breakdown = cls._hl_pnl_breakdown_from_fills(open_fills, close_fills, is_buy)
        if breakdown is None:
            return None
        return (
            breakdown["proposed_pnl"],
            breakdown["proposed_pct"],
            breakdown["proposed_filled_quote"],
        )

    @classmethod
    def _hl_pnl_breakdown_from_fills(
        cls,
        open_fills: List[Dict[str, Any]],
        close_fills: List[Dict[str, Any]],
        is_buy: bool,
    ) -> Optional[Dict[str, Any]]:
        """Compute proposed PnL plus HL ground-truth fields for dry-run comparison."""
        if not open_fills or not close_fills:
            return None

        open_base = cls._base_from_hl_fills(open_fills)
        close_base = cls._base_from_hl_fills(close_fills)
        open_quote = cls._quote_from_hl_fills(open_fills)
        close_quote = cls._quote_from_hl_fills(close_fills)
        fees_open = cls._fees_from_hl_fills(open_fills)
        fees_close = cls._fees_from_hl_fills(close_fills)
        fees_total = fees_open + fees_close
        open_vwap = cls._vwap_from_hl_fills(open_fills)
        close_vwap = cls._vwap_from_hl_fills(close_fills)
        size_mismatch = cls._size_mismatch_from_hl_fills(open_fills, close_fills)

        if close_base <= 0 or open_base <= 0 or open_quote <= 0 or close_quote <= 0:
            return None

        hl_closed_pnl = cls._closed_pnl_from_hl_fills(close_fills)
        use_closed_pnl = cls._hl_fills_have_closed_pnl_field(close_fills)

        matched_base = min(open_base, close_base)
        if open_vwap is None or close_vwap is None:
            return None

        if use_closed_pnl:
            # SQD / HL fill tape: closedPnl is pre-fee realized on the close.
            # Never include open-leg closedPnl (flip opens realize a prior position).
            trade_pnl = hl_closed_pnl
            method = "closed_pnl"
            # Flip/partial opens: open fill fees cover closing another position too.
            # Attribute only close-leg fees so net matches HL UI for the close.
            if size_mismatch:
                fees_for_net = fees_close
            else:
                fees_for_net = fees_total
        else:
            trade_pnl = (
                (close_vwap - open_vwap) * matched_base
                if is_buy
                else (open_vwap - close_vwap) * matched_base
            )
            method = "matched_vwap"
            fees_for_net = fees_total

        net_pnl = trade_pnl - fees_for_net
        # Percent on residual/matched close notional (correct for flips).
        pct_den = close_vwap * matched_base
        net_pnl_pct = net_pnl / pct_den if pct_den > 0 else Decimal("0")
        # Round-trip quote volume uses matched open notional + close notional.
        matched_open_quote = open_vwap * matched_base
        filled_amount_quote = matched_open_quote + close_quote

        # HL comparison target uses the same formula we would persist.
        hl_net_pnl = net_pnl

        return {
            "proposed_pnl": net_pnl,
            "proposed_pct": net_pnl_pct,
            "proposed_filled_quote": filled_amount_quote,
            "proposed_fees": fees_for_net,
            "proposed_close": close_vwap,
            "proposed_entry": (
                # Flip opens have misleading VWAP; keep entry unset for caller to preserve.
                None if size_mismatch else open_vwap
            ),
            "hl_closed_pnl": hl_closed_pnl,
            "hl_fees_open": fees_open,
            "hl_fees_close": fees_close,
            "hl_fees_total": fees_total,
            "hl_net_pnl": hl_net_pnl,
            "open_base": open_base,
            "close_base": close_base,
            "matched_base": matched_base,
            "size_mismatch": size_mismatch,
            "method": method,
        }

    async def _resolve_executor_fill_oids(
        self,
        record,
        custom_info: Dict[str, Any],
    ) -> tuple[Optional[str], Optional[str], List[str], List[str]]:
        """Resolve open/close order lookup keys (client + exchange ids) for HL fills.

        Returns:
            (open_client_oid, close_client_oid, open_lookup_keys, close_lookup_keys)
        """
        order_ids = custom_info.get("order_ids") or []
        open_oid = str(order_ids[0]) if order_ids else None
        close_oid = str(order_ids[1]) if len(order_ids) > 1 else None
        if self._is_synthetic_order_id(open_oid):
            open_oid = None
        if self._is_synthetic_order_id(close_oid):
            close_oid = None

        open_keys: List[str] = [open_oid] if open_oid else []
        close_keys: List[str] = [close_oid] if close_oid else []

        if not self.db_manager or not record.trading_pair:
            return open_oid, close_oid, open_keys, close_keys

        try:
            config = json.loads(record.config) if record.config else {}
        except (json.JSONDecodeError, TypeError):
            config = {}
        is_buy = not self._parse_config_side_is_short(config if isinstance(config, dict) else {})
        open_side = "BUY" if is_buy else "SELL"
        close_side = "SELL" if is_buy else "BUY"

        try:
            from database.repositories.order_repository import OrderRepository

            async with self.db_manager.get_session_context() as session:
                repo = OrderRepository(session)
                orders = await repo.get_orders(
                    account_name=record.account_name,
                    connector_name=record.connector_name,
                    trading_pair=record.trading_pair,
                    status="FILLED",
                    limit=100,
                )
        except Exception:
            return open_oid, close_oid, open_keys, close_keys

        created_at = record.created_at
        closed_at = record.closed_at
        candidates = []
        for order in orders:
            oid = str(order.client_order_id or "")
            if not oid:
                continue
            if created_at and order.created_at and order.created_at < created_at:
                continue
            if closed_at and order.created_at and order.created_at > closed_at:
                continue
            candidates.append(order)

        if not open_oid:
            for order in candidates:
                if (order.trade_type or "").upper() == open_side:
                    open_oid = str(order.client_order_id)
                    open_keys = [open_oid]
                    break

        if not close_oid:
            for order in reversed(candidates):
                if (order.trade_type or "").upper() == close_side:
                    if open_oid and str(order.client_order_id) == open_oid:
                        continue
                    close_oid = str(order.client_order_id)
                    close_keys = [close_oid]
                    break

        # Prefer looking up HL fills by exchange oid as well as client order id.
        for order in candidates:
            cid = str(order.client_order_id or "")
            eid = str(order.exchange_order_id or "") if getattr(order, "exchange_order_id", None) else ""
            if cid and open_oid and cid == open_oid and eid and eid not in open_keys:
                open_keys.append(eid)
            if cid and close_oid and cid == close_oid and eid and eid not in close_keys:
                close_keys.append(eid)

        return open_oid, close_oid, open_keys, close_keys

    async def _persist_repaired_executor_pnl(
        self,
        executor_id: str,
        stored_pnl: Decimal,
        net_pnl: Decimal,
        net_pnl_pct: Decimal,
        filled_quote: Decimal,
        open_fills: List[Dict[str, Any]],
        close_fills: List[Dict[str, Any]],
        final_state_patch: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Write HL-fill repaired PnL back to the executor record when it differs."""
        if not self.db_manager:
            return False
        if abs(stored_pnl - net_pnl) < Decimal("0.0001") and not final_state_patch:
            return False

        fees = sum(
            Decimal(str(row.get("fee", 0)))
            for row in (open_fills + close_fills)
        )
        try:
            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)
                await repo.update_executor(
                    executor_id=executor_id,
                    net_pnl_quote=net_pnl,
                    net_pnl_pct=net_pnl_pct,
                    filled_amount_quote=filled_quote,
                    cum_fees_quote=fees,
                    final_state=json.dumps(final_state_patch, default=_json_default)
                    if final_state_patch
                    else None,
                )
        except Exception as exc:
            logger.warning(
                "Could not persist repaired PnL for executor %s: %s",
                executor_id,
                exc,
            )
            return False

        return True

    async def _repair_executor_pnl_from_hl_fills(
        self,
        formatted: Dict[str, Any],
        record,
        fills_by_oid: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        """Correct persisted PnL using HL fill prices when order tracking recorded zero fills."""
        if (
            record.executor_type != "position_executor"
            or record.status != "TERMINATED"
            or not fills_by_oid
        ):
            return formatted

        custom_info = formatted.get("custom_info") or {}
        open_oid, close_oid, open_keys, close_keys = await self._resolve_executor_fill_oids(
            record, custom_info
        )
        if not open_oid or not close_oid:
            return formatted

        try:
            config = json.loads(record.config) if record.config else {}
        except (json.JSONDecodeError, TypeError):
            config = {}
        is_buy = not self._parse_config_side_is_short(config if isinstance(config, dict) else {})

        open_fills = self._fills_for_oids(fills_by_oid, *open_keys)
        close_fills = self._fills_for_oids(fills_by_oid, *close_keys)
        breakdown = self._hl_pnl_breakdown_from_fills(
            open_fills,
            close_fills,
            is_buy=is_buy,
        )
        if not breakdown:
            return formatted

        net_pnl = breakdown["proposed_pnl"]
        net_pnl_pct = breakdown["proposed_pct"]
        filled_quote = breakdown["proposed_filled_quote"]
        fees = breakdown["proposed_fees"]
        formatted["net_pnl_quote"] = float(net_pnl)
        formatted["net_pnl_pct"] = float(net_pnl_pct)
        formatted["filled_amount_quote"] = float(filled_quote)
        formatted["cum_fees_quote"] = float(fees)

        stored_pnl = Decimal(str(record.net_pnl_quote or 0))
        final_state_patch = dict(custom_info)
        order_ids = custom_info.get("order_ids") or []
        if order_ids and self._is_synthetic_order_id(str(order_ids[0])):
            final_state_patch["order_ids"] = [open_oid, close_oid]
        close_vwap = breakdown["proposed_close"]
        entry_vwap = breakdown["proposed_entry"]
        if entry_vwap is not None:
            final_state_patch["current_position_average_price"] = float(entry_vwap)
        if close_vwap is not None:
            final_state_patch["close_price"] = float(close_vwap)
        formatted["custom_info"] = final_state_patch

        if await self._persist_repaired_executor_pnl(
            record.executor_id,
            stored_pnl,
            net_pnl,
            net_pnl_pct,
            filled_quote,
            open_fills,
            close_fills,
            final_state_patch=final_state_patch,
        ):
            record.net_pnl_quote = net_pnl
            record.net_pnl_pct = net_pnl_pct
            record.filled_amount_quote = filled_quote
            record.cum_fees_quote = fees
            record.final_state = json.dumps(final_state_patch, default=_json_default)
        return formatted

    _HL_PNL_REPAIR_SKIP_CLOSE_TYPES = frozenset({"STALE_DUPLICATE", "MISTAKE", "MANUAL"})
    _HL_PNL_MATCH_EPSILON = Decimal("0.05")
    _HL_PNL_UPDATE_EPSILON = Decimal("0.0001")

    async def repair_hl_executor_pnl_bulk(
        self,
        *,
        dry_run: bool = True,
        force: bool = False,
        account_name: Optional[str] = None,
        connector_name: Optional[str] = None,
        trading_pair: Optional[str] = None,
        controller_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Compare / repair terminated HL position-executor PnL from userFills.

        dry_run=True (default): no DB writes; returns stored vs proposed vs HL fields.
        dry_run=False: persist updates. Refuses if any would_update row has match_hl=False
        unless force=True.
        """
        account = account_name or self.default_account
        connector = connector_name or "hyperliquid_perpetual"
        if "hyperliquid" not in connector:
            return {
                "dry_run": dry_run,
                "force": force,
                "examined": 0,
                "error": "connector_name must be a hyperliquid connector",
                "rows": [],
                "summary": {},
            }

        if not self.db_manager:
            return {
                "dry_run": dry_run,
                "force": force,
                "examined": 0,
                "error": "database not available",
                "rows": [],
                "summary": {},
            }

        async with self.db_manager.get_session_context() as session:
            repo = ExecutorRepository(session)
            records = await repo.get_executors(
                account_name=account,
                connector_name=connector,
                trading_pair=trading_pair,
                executor_type="position_executor",
                status="TERMINATED",
                controller_id=controller_id,
                limit=None,
            )

        fills_by_oid = await self._load_hyperliquid_fills_by_oid(
            account,
            connector,
            ensure_init=True,
            allow_during_recovery=True,
        )
        fills_error = getattr(self, "_hl_fills_last_error", None)

        rows: List[Dict[str, Any]] = []
        for record in records:
            close_type = str(record.close_type or "")
            if close_type in self._HL_PNL_REPAIR_SKIP_CLOSE_TYPES:
                rows.append(
                    self._hl_pnl_repair_row_stub(
                        record, status="skipped_close_type", fills_available=bool(fills_by_oid)
                    )
                )
                continue
            try:
                row = await self._build_hl_pnl_repair_row(record, fills_by_oid)
            except Exception as exc:
                logger.warning(
                    "HL PnL repair row failed for %s: %s", record.executor_id, exc
                )
                rows.append(
                    self._hl_pnl_repair_row_stub(
                        record, status="error", error=str(exc), fills_available=bool(fills_by_oid)
                    )
                )
                continue
            rows.append(row)

        would_update = [r for r in rows if r.get("status") == "would_update"]
        diverging = [r for r in would_update if not r.get("match_hl")]
        matching = [r for r in would_update if r.get("match_hl")]
        stale_rows = [r for r in rows if r.get("status") == "skipped_close_type"]
        stale_nonzero = [
            r
            for r in stale_rows
            if abs(float(r.get("stored_pnl") or 0)) >= float(self._HL_PNL_UPDATE_EPSILON)
            or abs(float(r.get("stored_fees") or 0)) >= float(self._HL_PNL_UPDATE_EPSILON)
        ]

        summary = {
            "examined": len(rows),
            "would_update": len(would_update),
            "unchanged": sum(1 for r in rows if r.get("status") == "unchanged"),
            "skipped": sum(1 for r in rows if str(r.get("status", "")).startswith("skipped")),
            "errors": sum(1 for r in rows if r.get("status") == "error"),
            "would_update_matching_hl": len(matching),
            "would_update_diverging_hl": len(diverging),
            "would_zero_stale": len(stale_nonzero),
            "applied": 0,
            "stale_zeroed": 0,
            "refused": False,
            "fills_loaded": len(fills_by_oid),
            "fills_error": fills_error,
        }

        # Surface divergences first for dry-run review.
        rows.sort(
            key=lambda r: (
                0 if r.get("status") == "would_update" and not r.get("match_hl") else
                1 if r.get("status") == "would_update" else
                2 if r.get("status") == "error" else
                3
            )
        )

        if not dry_run:
            if diverging and not force:
                summary["refused"] = True
                summary["refuse_reason"] = (
                    f"{len(diverging)} would_update row(s) diverge from HL "
                    "(match_hl=false); pass force=true to apply anyway"
                )
                return {
                    "dry_run": dry_run,
                    "force": force,
                    "summary": summary,
                    "diverging": diverging,
                    "rows": rows,
                }

            applied = 0
            for record in records:
                row = next(
                    (r for r in rows if r.get("executor_id") == record.executor_id),
                    None,
                )
                if not row or row.get("status") != "would_update":
                    continue
                if not row.get("match_hl") and not force:
                    continue
                formatted = self._format_db_record(record)
                updated = await self._repair_executor_pnl_from_hl_fills(
                    formatted, record, fills_by_oid
                )
                if abs(
                    Decimal(str(updated.get("net_pnl_quote") or 0))
                    - Decimal(str(row.get("stored_pnl") or 0))
                ) >= self._HL_PNL_UPDATE_EPSILON:
                    applied += 1
                    row["status"] = "updated"
                    row["applied_pnl"] = updated.get("net_pnl_quote")
            summary["applied"] = applied

            # Zero junk close types so Condor / Executors KPIs are not polluted.
            stale_zeroed = await self._zero_excluded_close_type_pnls(
                account=account,
                connector=connector,
                trading_pair=trading_pair,
                controller_id=controller_id,
            )
            summary["stale_zeroed"] = stale_zeroed
            for row in stale_nonzero:
                row["status"] = "stale_zeroed"
                row["proposed_pnl"] = 0.0
                row["proposed_fees"] = 0.0

        return {
            "dry_run": dry_run,
            "force": force,
            "summary": summary,
            "diverging": diverging,
            "stale_nonzero": stale_nonzero,
            "rows": rows,
        }

    async def _zero_excluded_close_type_pnls(
        self,
        *,
        account: str,
        connector: str,
        trading_pair: Optional[str] = None,
        controller_id: Optional[str] = None,
    ) -> int:
        """Set net PnL/fees to 0 for STALE_DUPLICATE / MISTAKE / MANUAL rows."""
        if not self.db_manager:
            return 0
        zeroed = 0
        async with self.db_manager.get_session_context() as session:
            repo = ExecutorRepository(session)
            records = await repo.get_executors(
                account_name=account,
                connector_name=connector,
                trading_pair=trading_pair,
                executor_type="position_executor",
                status="TERMINATED",
                controller_id=controller_id,
                limit=None,
            )
            for record in records:
                if str(record.close_type or "") not in self._HL_PNL_REPAIR_SKIP_CLOSE_TYPES:
                    continue
                stored = Decimal(str(record.net_pnl_quote or 0))
                fees = Decimal(str(record.cum_fees_quote or 0))
                if (
                    abs(stored) < self._HL_PNL_UPDATE_EPSILON
                    and abs(fees) < self._HL_PNL_UPDATE_EPSILON
                ):
                    continue
                try:
                    await repo.update_executor(
                        executor_id=record.executor_id,
                        net_pnl_quote=Decimal("0"),
                        net_pnl_pct=Decimal("0"),
                        cum_fees_quote=Decimal("0"),
                    )
                    zeroed += 1
                except Exception as exc:
                    logger.warning(
                        "Could not zero excluded close_type PnL for %s: %s",
                        record.executor_id,
                        exc,
                    )
        return zeroed

    def _hl_pnl_repair_row_stub(
        self,
        record,
        *,
        status: str,
        error: Optional[str] = None,
        fills_available: bool = False,
    ) -> Dict[str, Any]:
        custom_info = {}
        try:
            custom_info = json.loads(record.final_state) if record.final_state else {}
        except (json.JSONDecodeError, TypeError):
            custom_info = {}
        return {
            "executor_id": record.executor_id,
            "trading_pair": record.trading_pair,
            "close_type": record.close_type,
            "closed_at": record.closed_at.isoformat() if record.closed_at else None,
            "stored_pnl": float(record.net_pnl_quote or 0),
            "stored_pct": float(record.net_pnl_pct or 0),
            "stored_fees": float(record.cum_fees_quote or 0),
            "stored_close": float(custom_info.get("close_price") or 0) or None,
            "status": status,
            "fills_available": fills_available,
            "error": error,
        }

    async def _build_hl_pnl_repair_row(
        self,
        record,
        fills_by_oid: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        stub = self._hl_pnl_repair_row_stub(
            record, status="skipped_no_fills", fills_available=bool(fills_by_oid)
        )
        if not fills_by_oid:
            stub["status"] = "skipped_no_fills"
            return stub

        custom_info = {}
        try:
            custom_info = json.loads(record.final_state) if record.final_state else {}
        except (json.JSONDecodeError, TypeError):
            custom_info = {}

        open_oid, close_oid, open_keys, close_keys = await self._resolve_executor_fill_oids(
            record, custom_info
        )
        if not open_oid or not close_oid:
            stub["status"] = "skipped_unresolved_oids"
            return stub

        try:
            config = json.loads(record.config) if record.config else {}
        except (json.JSONDecodeError, TypeError):
            config = {}
        is_buy = not self._parse_config_side_is_short(
            config if isinstance(config, dict) else {}
        )

        open_fills = self._fills_for_oids(fills_by_oid, *open_keys)
        close_fills = self._fills_for_oids(fills_by_oid, *close_keys)
        if not open_fills or not close_fills:
            stub["status"] = "skipped_no_fills"
            stub["open_oid"] = open_oid
            stub["close_oid"] = close_oid
            stub["open_fill_n"] = len(open_fills)
            stub["close_fill_n"] = len(close_fills)
            return stub

        breakdown = self._hl_pnl_breakdown_from_fills(open_fills, close_fills, is_buy=is_buy)
        if not breakdown:
            stub["status"] = "skipped_no_fills"
            return stub

        stored_pnl = Decimal(str(record.net_pnl_quote or 0))
        proposed = breakdown["proposed_pnl"]
        hl_net = breakdown["hl_net_pnl"]
        delta_stored = proposed - stored_pnl
        delta_hl = proposed - hl_net
        match_hl = abs(delta_hl) <= self._HL_PNL_MATCH_EPSILON
        would_change = abs(delta_stored) >= self._HL_PNL_UPDATE_EPSILON

        return {
            "executor_id": record.executor_id,
            "trading_pair": record.trading_pair,
            "close_type": record.close_type,
            "closed_at": record.closed_at.isoformat() if record.closed_at else None,
            "open_oid": open_oid,
            "close_oid": close_oid,
            "stored_pnl": float(stored_pnl),
            "stored_pct": float(record.net_pnl_pct or 0),
            "stored_fees": float(record.cum_fees_quote or 0),
            "stored_close": float(custom_info.get("close_price") or 0) or None,
            "proposed_pnl": float(proposed),
            "proposed_pct": float(breakdown["proposed_pct"]),
            "proposed_fees": float(breakdown["proposed_fees"]),
            "proposed_close": float(breakdown["proposed_close"])
            if breakdown["proposed_close"] is not None
            else None,
            "hl_closed_pnl": float(breakdown["hl_closed_pnl"]),
            "hl_fees_open": float(breakdown["hl_fees_open"]),
            "hl_fees_close": float(breakdown["hl_fees_close"]),
            "hl_fees_total": float(breakdown["hl_fees_total"]),
            "hl_net_pnl": float(hl_net),
            "delta_stored_vs_proposed": float(delta_stored),
            "delta_proposed_vs_hl": float(delta_hl),
            "match_hl": match_hl,
            "size_mismatch": bool(breakdown["size_mismatch"]),
            "method": breakdown["method"],
            "open_base": float(breakdown["open_base"]),
            "close_base": float(breakdown["close_base"]),
            "status": "would_update" if would_change else "unchanged",
            "fills_available": True,
        }

    async def _refresh_position_executor_orders_before_persist(
        self,
        executor: ExecutorBase,
        account_name: str,
    ) -> None:
        """Refresh tracked orders from connector and HL fills before persisting PnL."""
        from hummingbot.core.data_type.in_flight_order import OrderState

        if not isinstance(executor, PositionExecutor):
            return

        config = executor.config
        connector_name = config.connector_name
        if "hyperliquid" not in connector_name:
            return

        connector = await self._get_trading_interface(account_name).ensure_connector(
            connector_name
        )
        for attr in ("_open_order", "_close_order", "_take_profit_limit_order"):
            tracked = getattr(executor, attr, None)
            if not tracked or not tracked.order_id:
                continue
            live = connector.in_flight_orders.get(tracked.order_id)
            if live:
                tracked.order = live

        fills_by_oid = await self._load_hyperliquid_fills_by_oid(account_name, connector_name)
        open_oid = (
            str(executor._open_order.order.exchange_order_id)
            if executor._open_order and executor._open_order.order and executor._open_order.order.exchange_order_id
            else (executor._open_order.order_id if executor._open_order else None)
        )
        close_oid = (
            str(executor._close_order.order.exchange_order_id)
            if executor._close_order and executor._close_order.order and executor._close_order.order.exchange_order_id
            else (executor._close_order.order_id if executor._close_order else None)
        )
        if not open_oid and executor._open_order:
            open_oid = executor._open_order.order_id
        if not close_oid and executor._close_order:
            close_oid = executor._close_order.order_id

        for tracked, oid in (
            (executor._open_order, open_oid),
            (executor._close_order, close_oid),
        ):
            if not tracked or not oid:
                continue
            fills = self._fills_for_oid(fills_by_oid, oid)
            if not fills or not tracked.order:
                continue
            base = sum(Decimal(str(f.get("sz", 0))) for f in fills)
            quote = sum(Decimal(str(f.get("px", 0))) * Decimal(str(f.get("sz", 0))) for f in fills)
            if base <= 0:
                continue
            tracked.order.executed_amount_base = base
            tracked.order.executed_amount_quote = quote
            tracked.order.current_state = OrderState.FILLED
            tracked.order.completely_filled_event.set()

    @staticmethod
    def _mark_position_executor_recovered(executor: ExecutorBase) -> None:
        """Skip open-order budget validation for executors seeded from live exchange state."""
        executor._recovered_from_exchange = True

        async def _skip_balance_validation(_self) -> None:
            return

        executor.validate_sufficient_balance = types.MethodType(
            _skip_balance_validation, executor
        )

    async def _adopt_take_profit_limit_order_from_connector(
        self,
        executor: ExecutorBase,
        account_name: str,
    ) -> bool:
        """Bind an existing exchange TP limit order to a recovered PositionExecutor."""
        from hummingbot.strategy_v2.models.executors import TrackedOrder

        config = executor.config
        if (
            not config.triple_barrier_config.take_profit
            or not config.triple_barrier_config.take_profit_order_type.is_limit_type()
        ):
            return False

        connector = await self._get_trading_interface(account_name).ensure_connector(
            config.connector_name
        )
        close_side = executor.close_order_side
        for order in connector.in_flight_orders.values():
            if order.trading_pair != config.trading_pair:
                continue
            if order.trade_type != close_side:
                continue
            if not order.order_type.is_limit_type():
                continue
            if order.is_done:
                continue
            tracked = TrackedOrder(order_id=order.client_order_id)
            tracked.order = order
            executor._take_profit_limit_order = tracked
            logger.info(
                "Recovered TP limit order %s for executor %s (%s)",
                order.client_order_id,
                config.id,
                config.trading_pair,
            )
            return True
        return False

    async def _ensure_exchange_open_orders_imported(
        self, account_name: str, connector_name: str
    ) -> None:
        key = f"{account_name}:{connector_name}"
        if key in self._recovery_open_orders_imported:
            return
        self._recovery_open_orders_imported.add(key)
        try:
            imported = await self._import_exchange_open_orders(account_name, connector_name)
            if imported:
                logger.info(
                    "Imported %d exchange open order(s) for %s/%s before executor recovery",
                    imported,
                    account_name,
                    connector_name,
                )
        except Exception as exc:
            logger.warning(
                "Could not import exchange open orders for %s/%s: %s",
                account_name,
                connector_name,
                exc,
            )

    async def _import_exchange_open_orders(
        self, account_name: str, connector_name: str
    ) -> int:
        """Register live exchange open orders into connector.in_flight_orders for recovery adopt."""
        from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
        from hummingbot.core.data_type.in_flight_order import OrderState, PerpetualDerivativeInFlightOrder

        connector = await self._get_trading_interface(account_name).ensure_connector(connector_name)
        if not hasattr(connector, "fetch_frontend_open_orders"):
            return 0

        open_orders = await connector.fetch_frontend_open_orders()
        if not open_orders:
            return 0

        imported = 0
        for raw in open_orders:
            coin = raw.get("coin")
            if not coin:
                continue
            try:
                trading_pair = await connector.trading_pair_associated_to_exchange_symbol(symbol=coin)
            except Exception:
                continue
            side_raw = raw.get("side")
            trade_type = TradeType.BUY if side_raw == "B" else TradeType.SELL
            oid = raw.get("oid")
            if oid is None:
                continue
            exchange_order_id = str(oid)
            cloid = raw.get("cloid")
            client_order_id = str(cloid) if cloid else exchange_order_id
            if client_order_id in connector.in_flight_orders:
                continue
            order_type_raw = str(raw.get("orderType") or "Limit")
            if "market" in order_type_raw.lower():
                order_type = OrderType.MARKET
            elif "maker" in order_type_raw.lower():
                order_type = OrderType.LIMIT_MAKER
            else:
                order_type = OrderType.LIMIT
            amount = Decimal(str(raw.get("sz") or raw.get("origSz") or "0"))
            price = Decimal(str(raw.get("limitPx") or "0"))
            if amount <= 0:
                continue
            timestamp_ms = raw.get("timestamp") or 0
            leverage = connector.get_leverage(trading_pair=trading_pair)
            position_action = PositionAction.OPEN
            order = PerpetualDerivativeInFlightOrder(
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=trading_pair,
                order_type=order_type,
                trade_type=trade_type,
                amount=amount,
                price=price,
                creation_timestamp=timestamp_ms / 1000 if timestamp_ms else connector.current_timestamp,
                initial_state=OrderState.OPEN,
                leverage=leverage,
                position=position_action,
            )
            connector._order_tracker.start_tracking_order(order)
            imported += 1

        return imported

    def _parse_config_side_is_short(self, config: dict) -> bool:
        side = config.get("side")
        if side is None:
            return False
        if isinstance(side, int):
            return side == 2
        if isinstance(side, str):
            return side.upper() in ("SELL", "SHORT", "2")
        side_name = getattr(side, "name", str(side)).upper()
        return side_name in ("SELL", "SHORT")

    def _position_leg_key(self, record) -> Optional[tuple]:
        """Unique exchange leg: (account, connector, pair, LONG|SHORT)."""
        if record.executor_type != "position_executor":
            return None
        try:
            config = json.loads(record.config) if record.config else {}
        except (json.JSONDecodeError, TypeError):
            config = {}
        if not isinstance(config, dict):
            config = {}
        side_label = "SHORT" if self._parse_config_side_is_short(config) else "LONG"
        return (
            record.account_name or self.default_account,
            record.connector_name or config.get("connector_name"),
            record.trading_pair or config.get("trading_pair"),
            side_label,
        )

    async def _terminate_executor_record(self, executor_id: str, close_type: str) -> None:
        if not self.db_manager:
            return
        try:
            async with self.db_manager.get_session_context() as session:
                from database.repositories.executor_repository import ExecutorRepository
                repo = ExecutorRepository(session)
                await repo.update_executor(
                    executor_id=executor_id,
                    status="TERMINATED",
                    close_type=close_type,
                )
        except Exception as exc:
            logger.error(
                "Failed to terminate executor record %s (%s): %s",
                executor_id,
                close_type,
                exc,
            )

    async def _record_has_live_exchange_position(self, record) -> bool:
        """True when the exchange still holds a leg matching this executor's side."""
        if record.executor_type != "position_executor":
            return False
        try:
            config = json.loads(record.config) if record.config else {}
        except (json.JSONDecodeError, TypeError):
            config = {}
        if not isinstance(config, dict):
            config = {}

        try:
            positions = await self._resolve_positions(
                record.account_name, record.connector_name
            )
        except Exception as exc:
            logger.warning(
                "Could not verify exchange position for %s: %s",
                record.executor_id,
                exc,
            )
            return False

        pos = positions.get(record.trading_pair) if positions else None
        if not pos:
            return False

        amount_raw = Decimal(str(pos.get("amount") or 0))
        if amount_raw.copy_abs() <= Decimal("0"):
            return False

        config_is_short = self._parse_config_side_is_short(config)
        position_side = str(pos.get("position_side") or "").upper()
        exchange_is_short = position_side == "SHORT" or amount_raw < 0
        return exchange_is_short == config_is_short

    async def cleanup_orphaned_executors(self):
        """
        Clean up orphaned executors from database on startup.

        Only marks TERMINATED when a RUNNING record is not in memory **and**
        there is no live exchange position left to manage. Records that still
        have an open leg are left RUNNING so operators/agents can see the gap.
        """
        if not self.db_manager:
            logger.debug("No database manager available, skipping orphaned executor cleanup")
            return

        try:
            active_executor_ids = list(self._active_executors.keys())

            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)

                records = await repo.get_active_executors()
                protected_ids: list[str] = []
                for record in records:
                    if record.executor_id in active_executor_ids:
                        continue
                    if await self._record_has_live_exchange_position(record):
                        protected_ids.append(record.executor_id)
                        logger.error(
                            "Executor %s still has an open %s position on %s but "
                            "could not be recovered — leaving DB status RUNNING",
                            record.executor_id,
                            record.trading_pair,
                            record.connector_name,
                        )

                cleaned_count = 0
                if records:
                    keep_ids = active_executor_ids + protected_ids
                    cleaned_count = await repo.cleanup_orphaned_executors(
                        active_executor_ids=keep_ids,
                        close_type="SYSTEM_CLEANUP",
                    )

            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} orphaned executors from database")
            else:
                logger.debug("No orphaned executors found in database")

        except Exception as e:
            logger.error(f"Error cleaning up orphaned executors: {e}", exc_info=True)

    async def stop(self):
        """Stop the executor service and all active executors."""
        self._is_running = False

        if self._recovery_task and not self._recovery_task.done():
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except asyncio.CancelledError:
                pass
            self._recovery_task = None

        if self._control_loop_task:
            self._control_loop_task.cancel()
            try:
                await self._control_loop_task
            except asyncio.CancelledError:
                pass
            self._control_loop_task = None

        # Stop all active executors
        for executor_id in list(self._active_executors.keys()):
            try:
                executor = self._active_executors.get(executor_id)
                if executor:
                    executor.stop()
            except Exception as e:
                logger.error(f"Error stopping executor {executor_id}: {e}")

        # Clear active executors
        self._active_executors.clear()
        self._executor_metadata.clear()

        # Cleanup trading interfaces
        for trading_interface in self._trading_interfaces.values():
            await trading_interface.cleanup()
        self._trading_interfaces.clear()

        logger.info("ExecutorService stopped")

    async def _control_loop(self):
        """Main control loop that updates all active executors."""
        while self._is_running:
            try:
                # Update timestamps for all trading interfaces via TradingService
                self._trading_service.update_all_timestamps()

                # Backfill open-order fill amounts when exchange position exists but
                # connector fill tracking left executed_amount_base at zero.
                # Defer while background startup recovery is running to avoid
                # competing HL position polls against the batched recovery pass.
                if not self._recovery_in_progress:
                    for executor_id, executor in list(self._active_executors.items()):
                        if executor.is_closed:
                            continue
                        metadata = self._executor_metadata.get(executor_id, {})
                        executor_type = metadata.get("executor_type")
                        account_name = metadata.get("account_name") or self.default_account

                        from hummingbot.strategy_v2.models.base import RunnableStatus

                        if executor.status == RunnableStatus.SHUTTING_DOWN:
                            if executor_type in ("position_executor", "order_executor"):
                                await self._assist_executor_shutdown(executor, account_name)
                            continue

                        if executor_type == "position_executor":
                            await self._reconcile_position_executor_stale_open_order(
                                executor, account_name
                            )
                            await self._sync_position_executor_open_fill_from_exchange(
                                executor, account_name
                            )
                        elif executor_type == "order_executor":
                            await self._reconcile_order_executor_stale_order(
                                executor, account_name
                            )

                # Check for completed executors
                completed_ids = []
                for executor_id, executor in self._active_executors.items():
                    if executor.is_closed:
                        if executor_id in self._executors_pending_creation_persist:
                            continue
                        completed_ids.append(executor_id)

                # Handle completed executors
                for executor_id in completed_ids:
                    await self._handle_executor_completion(executor_id)

            except Exception as e:
                logger.error(f"Error in executor control loop: {e}", exc_info=True)

            await asyncio.sleep(self.update_interval)

    def _get_trading_interface(self, account_name: str) -> AccountTradingInterface:
        """Get or create an AccountTradingInterface for the account."""
        if account_name not in self._trading_interfaces:
            self._trading_interfaces[account_name] = self._trading_service.get_trading_interface(account_name)
        return self._trading_interfaces[account_name]

    def _validate_executor_config(
        self,
        executor_config: Dict[str, Any],
        default_timestamp: Optional[float] = None
    ) -> tuple[Type[ExecutorBase], Type[ExecutorConfigBase], ExecutorConfigBase]:
        """
        Validate the executor type and build the typed executor config.

        Pure validation step: no IO, no executor started, no DB access.

        Args:
            executor_config: Executor configuration dictionary (must include 'type')
            default_timestamp: Timestamp to set on the config if not provided
                (required for time-based features like time_limit)

        Returns:
            Tuple of (executor_class, config_class, typed_config)

        Raises:
            HTTPException: 400 if the type is missing/invalid or the config is invalid
        """
        executor_type = executor_config.get("type")
        if not executor_type:
            raise HTTPException(
                status_code=400,
                detail="executor_config must include 'type' field"
            )

        if executor_type not in self.EXECUTOR_REGISTRY:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid executor type '{executor_type}'. Valid types: {list(self.EXECUTOR_REGISTRY.keys())}"
            )

        if "timestamp" not in executor_config or executor_config["timestamp"] is None:
            executor_config["timestamp"] = default_timestamp

        executor_class, config_class = self.EXECUTOR_REGISTRY[executor_type]
        try:
            typed_config = config_class(**executor_config)
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid executor config: {str(e)}"
            )

        return executor_class, config_class, typed_config

    async def _prepare_market(self, account: str, connector_name: Optional[str], trading_pair: Optional[str]):
        """Ensure the connector and market for the executor are ready on the account's trading interface."""
        trading_interface = self._get_trading_interface(account)
        if connector_name:
            if trading_pair:
                await trading_interface.add_market(connector_name, trading_pair)
            else:
                await trading_interface.ensure_connector(connector_name)

    def _instantiate_and_register(
        self,
        executor_class: Type[ExecutorBase],
        typed_config: ExecutorConfigBase,
        trading_interface: AccountTradingInterface,
        metadata: Dict[str, Any]
    ) -> tuple[str, ExecutorBase]:
        """
        Instantiate the executor, register it in memory and start it.

        Args:
            executor_class: Executor class to instantiate
            typed_config: Validated typed executor config
            trading_interface: Trading interface acting as the executor's strategy
            metadata: Metadata dict to register for the executor

        Returns:
            Tuple of (executor_id, executor)

        Raises:
            HTTPException: 400 if the executor fails to instantiate
        """
        try:
            executor = executor_class(
                strategy=trading_interface,
                config=typed_config,
                update_interval=self.update_interval,
                max_retries=self.max_retries
            )
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to create executor: {str(e)}"
            )

        executor_id = typed_config.id
        self._active_executors[executor_id] = executor
        self._executor_metadata[executor_id] = metadata

        # Set ContextVar so the asyncio Task created by start() inherits it
        token = current_executor_id.set(executor_id)
        executor.start()
        current_executor_id.reset(token)

        return executor_id, executor

    async def create_executor(
        self,
        executor_config: Dict[str, Any],
        account_name: Optional[str] = None,
        controller_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create and start a new executor.

        Args:
            executor_config: Executor configuration dictionary (must include 'type')
            account_name: Account to use (defaults to master_account)

        Returns:
            Dictionary with executor_id and initial status
        """
        account = account_name or self.default_account
        trading_interface = self._get_trading_interface(account)

        # Validate executor type and build the typed config
        executor_class, _config_class, typed_config = self._validate_executor_config(
            executor_config, default_timestamp=trading_interface.current_timestamp
        )
        executor_type = executor_config["type"]

        # Ensure connector and market are ready
        connector_name = executor_config.get("connector_name")
        trading_pair = executor_config.get("trading_pair")
        await self._prepare_market(account, connector_name, trading_pair)
        if executor_type == "position_executor":
            await self._validate_position_executor_order_size(account, executor_config)

        # Instantiate the executor, register it in memory and start it
        controller_id = controller_id or getattr(typed_config, "controller_id", "main") or "main"
        metadata = {
            "account_name": account,
            "connector_name": connector_name,
            "trading_pair": trading_pair,
            "executor_type": executor_type,
            "controller_id": controller_id,
            "created_at": datetime.now(timezone.utc),
            "config": executor_config
        }
        executor_id, executor = self._instantiate_and_register(executor_class, typed_config, trading_interface, metadata)

        self._executors_pending_creation_persist.add(executor_id)
        try:
            if executor_type == "position_executor" and not executor.is_closed:
                await self._sync_position_executor_open_fill_from_exchange(executor, account)

            # Persist to database before completion handling so metadata cannot be cleared first.
            await self._persist_executor_created(executor_id, executor, metadata)
        finally:
            self._executors_pending_creation_persist.discard(executor_id)

        # Capture created_at before potential cleanup
        created_at = metadata["created_at"].isoformat()

        # Check if executor terminated immediately (e.g., insufficient balance)
        # If so, handle completion now rather than waiting for control loop
        if executor.is_closed:
            await self._handle_executor_completion(executor_id)

        logger.info(f"Created {executor_type} executor {executor_id} for {connector_name}/{trading_pair}")

        return {
            "executor_id": executor_id,
            "executor_type": executor_type,
            "connector_name": connector_name,
            "trading_pair": trading_pair,
            "controller_id": controller_id,
            "status": executor.status.name,
            "created_at": created_at
        }

    async def get_executors(
        self,
        account_name: Optional[str] = None,
        connector_name: Optional[str] = None,
        trading_pair: Optional[str] = None,
        executor_type: Optional[str] = None,
        status: Optional[str] = None,
        controller_id: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get list of executors with optional filtering.

        Combines active executors from memory with completed executors from database.

        Args:
            account_name: Filter by account name
            connector_name: Filter by connector name
            trading_pair: Filter by trading pair
            executor_type: Filter by executor type
            status: Filter by status
            controller_id: Filter by controller ID

        Returns:
            List of executor information dictionaries
        """
        result = []

        # Process active executors from memory
        for executor_id, executor in self._active_executors.items():
            metadata = self._executor_metadata.get(executor_id, {})

            # Apply filters
            if account_name and metadata.get("account_name") != account_name:
                continue
            if connector_name and metadata.get("connector_name") != connector_name:
                continue
            if trading_pair and metadata.get("trading_pair") != trading_pair:
                continue
            if executor_type and metadata.get("executor_type") != executor_type:
                continue
            if status and executor.status.name != status:
                continue
            if controller_id and metadata.get("controller_id", "main") != controller_id:
                continue

            result.append(self._format_executor_info(executor_id, executor))

        # Get completed executors from database
        if self.db_manager:
            try:
                async with self.db_manager.get_session_context() as session:
                    repo = ExecutorRepository(session)

                    db_executors = await repo.get_executors(
                        account_name=account_name,
                        connector_name=connector_name,
                        trading_pair=trading_pair,
                        executor_type=executor_type,
                        status=status,
                        controller_id=controller_id,
                        limit=limit
                    )

                    repair_candidates = [
                        r for r in db_executors if self._executor_may_need_hl_pnl_repair(r)
                    ]
                    fills_by_oid: Dict[str, List[Dict[str, Any]]] = {}
                    if repair_candidates:
                        repair_account = account_name or self.default_account
                        repair_connector = connector_name or "hyperliquid_perpetual"
                        fills_by_oid = await self._load_hyperliquid_fills_by_oid(
                            repair_account, repair_connector
                        )

                    for record in db_executors:
                        # Skip if already in active executors (safety check)
                        if record.executor_id in self._active_executors:
                            continue
                        # Stale RUNNING rows left in DB must not appear as live executors.
                        if record.status == "RUNNING":
                            continue
                        formatted = self._format_db_record(record)
                        if fills_by_oid and self._executor_may_need_hl_pnl_repair(record):
                            formatted = await self._repair_executor_pnl_from_hl_fills(
                                formatted, record, fills_by_oid
                            )
                        result.append(formatted)
            except Exception as e:
                logger.error(f"Error fetching executors from database: {e}")

        return result

    async def get_executor(self, executor_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific executor.

        Checks active executors in memory first, then falls back to database.

        Args:
            executor_id: The executor ID

        Returns:
            Detailed executor information or None if not found
        """
        # Check active executors first (memory)
        executor = self._active_executors.get(executor_id)
        if executor:
            return self._format_executor_info(executor_id, executor)

        # Fallback to database for completed executors
        if self.db_manager:
            try:
                async with self.db_manager.get_session_context() as session:
                    repo = ExecutorRepository(session)

                    record = await repo.get_executor_by_id(executor_id)
                    if record:
                        formatted = self._format_db_record(record)
                        if self._executor_may_need_hl_pnl_repair(record):
                            fills_by_oid = await self._load_hyperliquid_fills_by_oid(
                                record.account_name or self.default_account,
                                record.connector_name,
                            )
                            if fills_by_oid:
                                formatted = await self._repair_executor_pnl_from_hl_fills(
                                    formatted, record, fills_by_oid
                                )
                        return formatted
            except Exception as e:
                logger.error(f"Error fetching executor from database: {e}")

        return None

    def get_executor_logs(
        self,
        executor_id: str,
        level: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[dict]:
        """
        Get captured log entries for an executor.

        Only available for active executors (logs are cleared on completion).

        Args:
            executor_id: The executor ID
            level: Optional filter by level (ERROR, WARNING, INFO, DEBUG)
            limit: Maximum number of entries to return

        Returns:
            List of log entry dicts
        """
        return self._log_capture.get_logs(executor_id, level=level, limit=limit)

    async def stop_executor(
        self,
        executor_id: str,
        keep_position: bool = False
    ) -> Dict[str, Any]:
        """
        Stop an active executor.

        Args:
            executor_id: The executor ID to stop
            keep_position: Whether to keep the position open

        Returns:
            Dictionary with stop confirmation
        """
        executor = self._active_executors.get(executor_id)
        if not executor:
            raise HTTPException(status_code=404, detail=f"Executor {executor_id} not found")

        if executor.is_closed:
            raise HTTPException(status_code=400, detail=f"Executor {executor_id} is already closed")

        # Trigger early stop
        try:
            executor.early_stop(keep_position=keep_position)
        except Exception as e:
            logger.error(f"Error stopping executor {executor_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Error stopping executor: {str(e)}")

        logger.info(f"Initiated stop for executor {executor_id} (keep_position={keep_position})")

        return {
            "executor_id": executor_id,
            "status": "stopping",
            "keep_position": keep_position
        }

    async def _handle_executor_completion(self, executor_id: str):
        """Handle cleanup when an executor completes."""
        # Atomically claim the executor so a concurrent completion (e.g. the
        # control loop racing with the synchronous call in create_executor)
        # returns early instead of double-persisting / double-aggregating.
        executor = self._active_executors.pop(executor_id, None)
        if executor is None:
            return

        metadata = self._executor_metadata.get(executor_id, {})

        # Refresh HL fill data before reading PnL for position executors.
        account_name = metadata.get("account_name") or self.default_account
        await self._refresh_position_executor_orders_before_persist(executor, account_name)

        # Check if this is a POSITION_HOLD close type (keep_position=True)
        if executor.close_type == CloseType.POSITION_HOLD:
            await self._aggregate_position_hold(executor_id, executor, metadata)

        # Persist final state to database
        await self._persist_executor_completed(executor_id, executor)

        # Active executor already claimed via pop above; drop its metadata last
        # (metadata is read above and re-fetched inside the persist/aggregate
        # helpers, so it must stay until after those awaits complete).
        if executor_id in self._executor_metadata:
            del self._executor_metadata[executor_id]

        # Clean up captured logs
        self._log_capture.clear(executor_id)

        close_type = executor.close_type.name if executor.close_type else "UNKNOWN"
        logger.info(f"Executor {executor_id} completed with close_type: {close_type}")

    def _format_executor_info(
        self,
        executor_id: str,
        executor: ExecutorBase
    ) -> Dict[str, Any]:
        """Format executor information for API response."""
        metadata = self._executor_metadata.get(executor_id, {})
        executor_type = metadata.get("executor_type")

        # Get executor_info as a dict and strip heavy custom_info fields BEFORE
        # serialization so they never get coerced (fill_events, grid
        # levels_by_state, etc.); then coerce in-place to JSON-compatible
        # primitives instead of doing a json.dumps/json.loads string round-trip.
        executor_info = executor.executor_info
        dumped = executor_info.model_dump()
        dumped["custom_info"] = self._strip_heavy_fields(dumped.get("custom_info"), executor_type)
        result = _coerce_json_compatible(dumped)

        # Add metadata
        result["executor_id"] = executor_id
        result["executor_type"] = executor_type
        result["account_name"] = metadata.get("account_name")
        result["created_at"] = metadata.get("created_at").isoformat() if metadata.get("created_at") else None

        if metadata.get("connector_name"):
            result["connector_name"] = metadata.get("connector_name")
        if metadata.get("trading_pair"):
            result["trading_pair"] = metadata.get("trading_pair")
        result["controller_id"] = metadata.get("controller_id", "main")

        # Read status/close_type directly from executor
        result["status"] = executor.status.name
        result["close_type"] = executor.close_type.name if executor.close_type else None
        result["is_active"] = not executor.is_closed

        # Add side from executor_info (it's a property, not serialized by model_dump)
        side = executor_info.side
        if side is not None:
            # Convert TradeType enum or int to string
            result["side"] = side.name if hasattr(side, 'name') else str(side)

        # Add log capture info
        result["error_count"] = self._log_capture.get_error_count(executor_id)
        result["last_error"] = self._log_capture.get_last_error(executor_id)

        return result

    @staticmethod
    def _strip_heavy_fields(custom_info: Optional[Dict], executor_type: Optional[str] = None) -> Optional[Dict]:
        """Remove heavy fields from custom_info to reduce payload size."""
        if not custom_info:
            return custom_info
        heavy_fields = {"fill_events"}
        if executor_type == "grid_executor":
            heavy_fields |= {"levels_by_state", "filled_orders", "failed_orders", "canceled_orders"}
        return {k: v for k, v in custom_info.items() if k not in heavy_fields}

    def _format_db_record(self, record) -> Dict[str, Any]:
        """Format a database ExecutorRecord for API response."""
        # Parse error_log from DB for completed executors
        error_count = 0
        last_error = None
        if record.error_log:
            try:
                errors = json.loads(record.error_log)
                error_count = len(errors)
                if errors:
                    last_error = errors[-1].get("message")
            except (json.JSONDecodeError, TypeError):
                pass

        return {
            "executor_id": record.executor_id,
            "executor_type": record.executor_type,
            "account_name": record.account_name,
            "connector_name": record.connector_name,
            "trading_pair": record.trading_pair,
            "side": None,
            "status": record.status,
            "close_type": record.close_type,
            "is_active": record.status == "RUNNING",
            "is_trading": False,
            "timestamp": None,
            "created_at": record.created_at.isoformat() if record.created_at else None,
            "close_timestamp": record.closed_at.timestamp() if record.closed_at else None,
            "closed_at": record.closed_at.isoformat() if record.closed_at else None,
            "controller_id": record.controller_id or "main",
            "net_pnl_quote": float(record.net_pnl_quote) if record.net_pnl_quote else 0.0,
            "net_pnl_pct": float(record.net_pnl_pct) if record.net_pnl_pct else 0.0,
            "cum_fees_quote": float(record.cum_fees_quote) if record.cum_fees_quote else 0.0,
            "filled_amount_quote": float(record.filled_amount_quote) if record.filled_amount_quote else 0.0,
            "config": json.loads(record.config) if record.config else None,
            "custom_info": self._strip_heavy_fields(
                json.loads(record.final_state), record.executor_type
            ) if record.final_state else None,
            "error_count": error_count,
            "last_error": last_error,
        }

    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics for active executors.

        Returns:
            Dictionary with aggregate statistics for active executors only.
        """
        executors = []

        # Get active executors from memory
        for executor_id, executor in self._active_executors.items():
            executors.append(self._format_executor_info(executor_id, executor))

        active_count = len(executors)
        total_pnl = sum(e.get("net_pnl_quote", 0) for e in executors)
        total_volume = sum(e.get("filled_amount_quote", 0) for e in executors)

        by_type: Dict[str, int] = {}
        by_connector: Dict[str, int] = {}
        by_status: Dict[str, int] = {}

        for e in executors:
            ex_type = e.get("executor_type", "unknown")
            connector = e.get("connector_name", "unknown")
            status = e.get("status", "unknown")

            by_type[ex_type] = by_type.get(ex_type, 0) + 1
            by_connector[connector] = by_connector.get(connector, 0) + 1
            by_status[status] = by_status.get(status, 0) + 1

        return {
            "total_active": active_count,
            "total_pnl_quote": total_pnl,
            "total_volume_quote": total_volume,
            "by_type": by_type,
            "by_connector": by_connector,
            "by_status": by_status
        }

    async def get_performance_report(
        self,
        controller_id: Optional[str] = None,
        market_data_service=None
    ) -> Dict[str, Any]:
        """
        Generate a performance report aggregating executor metrics.

        Combines database aggregations (completed executors) with in-memory
        active executor and position hold unrealized PnL.
        Excludes POSITION_HOLD close_type from realized PnL to avoid double-counting.

        Args:
            controller_id: Filter by controller ID (None = all)
            market_data_service: MarketDataService for position hold unrealized PnL

        Returns:
            Dictionary with performance metrics ready for PerformanceReportResponse.
        """
        import math

        report: Dict[str, Any] = {
            "controller_id": controller_id,
            "total_executors": 0,
            "by_status": {},
            "pnl_total_quote": 0.0,
            "unrealized_pnl_quote": 0.0,
            "global_pnl_quote": 0.0,
            "pnl_pct_avg": 0.0,
            "fees_total_quote": 0.0,
            "volume_total_quote": 0.0,
            "win_rate": 0.0,
            "sharpe_ratio": None,
            "by_type": [],
            "active_positions": 0,
        }

        if self.db_manager:
            try:
                async with self.db_manager.get_session_context() as session:
                    repo = ExecutorRepository(session)
                    db_data = await repo.get_performance_report(controller_id=controller_id)

                report["total_executors"] = db_data["total_executors"]
                report["by_status"] = db_data["status_counts"]
                report["pnl_total_quote"] = db_data["pnl_total_quote"]
                report["pnl_pct_avg"] = db_data["pnl_pct_avg"]
                report["fees_total_quote"] = db_data["fees_total_quote"]
                report["volume_total_quote"] = db_data["volume_total_quote"]
                report["win_rate"] = db_data["win_rate"]
                report["by_type"] = db_data["by_type"]

                # Sharpe ratio: mean(pnl) / std(pnl), requires >= 2 values
                pnl_values = db_data.get("pnl_values", [])
                if len(pnl_values) >= 2:
                    mean_pnl = sum(pnl_values) / len(pnl_values)
                    variance = sum((v - mean_pnl) ** 2 for v in pnl_values) / (len(pnl_values) - 1)
                    std_pnl = math.sqrt(variance)
                    if std_pnl > 0:
                        report["sharpe_ratio"] = round(mean_pnl / std_pnl, 4)

            except Exception as e:
                logger.error(f"Error generating performance report: {e}", exc_info=True)

        # --- Unrealized PnL from active executors ---
        unrealized_pnl = 0.0
        for executor_id, executor in self._active_executors.items():
            metadata = self._executor_metadata.get(executor_id, {})
            if controller_id and metadata.get("controller_id", "main") != controller_id:
                continue
            try:
                unrealized_pnl += float(executor.executor_info.net_pnl_quote)
            except Exception:
                pass

        # --- Unrealized PnL from position holds ---
        positions = self.get_positions_held(controller_id=controller_id)
        report["active_positions"] = len(positions)

        # Accumulate fees from position holds (already paid, reduce PnL)
        position_hold_fees = sum(float(p.cum_fees_quote) for p in positions)

        if market_data_service:
            # First pass: try oracle for each position, collect misses grouped by connector
            missing_by_connector: Dict[str, List[tuple]] = {}  # connector_key -> [(position, trading_pair)]
            for p in positions:
                parts = p.trading_pair.split("-")
                if len(parts) != 2:
                    continue
                base, quote = parts
                rate = market_data_service.get_rate(base, quote)
                if rate is not None:
                    unrealized_pnl += float(p.get_unrealized_pnl(rate))
                else:
                    # Group by connector+account for batch fallback
                    connector_key = f"{p.connector_name}|{p.account_name}"
                    missing_by_connector.setdefault(connector_key, []).append((p, p.trading_pair))

            # Second pass: batch-fetch missing prices from the actual connectors
            for connector_key, items in missing_by_connector.items():
                connector_name, account_name = connector_key.split("|", 1)
                trading_pairs = [tp for _, tp in items]
                try:
                    prices = await market_data_service.get_prices(
                        connector_name=connector_name,
                        trading_pairs=trading_pairs,
                        account_name=account_name,
                    )
                    if isinstance(prices, dict) and "error" not in prices:
                        for pos, tp in items:
                            price = prices.get(tp)
                            if price is not None and price > 0:
                                unrealized_pnl += float(pos.get_unrealized_pnl(Decimal(str(price))))
                except Exception as e:
                    logger.warning(f"Fallback price fetch failed for {connector_name}: {e}")

        # Subtract position hold fees from unrealized PnL
        unrealized_pnl -= position_hold_fees

        report["unrealized_pnl_quote"] = round(unrealized_pnl, 8)
        report["position_hold_fees_quote"] = round(position_hold_fees, 8)
        report["global_pnl_quote"] = round(report["pnl_total_quote"] + unrealized_pnl, 8)

        return report

    async def _persist_executor_created(
        self,
        executor_id: str,
        executor: ExecutorBase,
        metadata: Dict[str, Any],
    ):
        """Persist executor creation to database."""
        if not self.db_manager:
            return

        executor_type = metadata.get("executor_type")
        account_name = metadata.get("account_name")
        connector_name = metadata.get("connector_name")
        trading_pair = metadata.get("trading_pair")
        if not all([executor_type, account_name, connector_name, trading_pair]):
            logger.error(
                "Cannot persist executor %s creation: incomplete metadata %s",
                executor_id,
                metadata,
            )
            return

        try:
            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)

                await repo.create_executor(
                    executor_id=executor_id,
                    executor_type=executor_type,
                    account_name=account_name,
                    connector_name=connector_name,
                    trading_pair=trading_pair,
                    config=json.dumps(metadata.get("config", {}), default=_json_default),
                    status=executor.status.name,
                    controller_id=metadata.get("controller_id", "main")
                )

            logger.debug(f"Persisted executor {executor_id} creation to database")

        except Exception as e:
            logger.error(f"Error persisting executor creation: {e}")

    async def _persist_executor_completed(self, executor_id: str, executor: ExecutorBase):
        """Persist executor completion to database."""
        if not self.db_manager:
            return

        try:
            # Read status/close_type directly from executor (most reliable)
            status_name = executor.status.name
            close_type = executor.close_type.name if executor.close_type else None

            # Get PnL values from executor_info
            try:
                executor_info = executor.executor_info
                net_pnl_quote = executor_info.net_pnl_quote
                net_pnl_pct = executor_info.net_pnl_pct
                cum_fees_quote = executor_info.cum_fees_quote
                filled_amount_quote = executor_info.filled_amount_quote
            except Exception as e:
                logger.debug(f"Error accessing executor_info for persistence: {e}")
                net_pnl_quote = Decimal("0")
                net_pnl_pct = Decimal("0")
                cum_fees_quote = Decimal("0")
                filled_amount_quote = Decimal("0")

            # Get custom_info directly from executor to avoid Pydantic serialization issues
            # with TrackedOrder and other complex types
            custom_info = executor.get_custom_info()
            # Serialize custom_info, fallback to None if serialization fails
            final_state_json = None
            metadata = self._executor_metadata.get(executor_id, {})
            executor_type = metadata.get("executor_type")
            if executor_type == "grid_executor":
                heavy_fields = {
                    "levels_by_state",
                    "filled_orders",
                    "failed_orders",
                    "canceled_orders",
                }
                custom_info = {k: v for k, v in custom_info.items() if k not in heavy_fields}

            try:
                final_state_json = json.dumps(custom_info, default=_json_default)
            except Exception as e:
                logger.warning(f"Failed to serialize custom_info for {executor_id}: {e}")
                # Try a simpler serialization without complex objects
                try:
                    simple_info = {k: v for k, v in custom_info.items()
                                   if isinstance(v, (str, int, float, bool, list, dict, type(None)))}
                    final_state_json = json.dumps(simple_info)
                except Exception:
                    final_state_json = None

            # Capture error logs before persisting
            error_log_json = None
            error_count = self._log_capture.get_error_count(executor_id)
            if error_count > 0:
                try:
                    error_entries = self._log_capture.get_logs(executor_id, level="ERROR")
                    error_log_json = json.dumps([
                        {
                            "timestamp": entry.get("timestamp"),
                            "message": entry.get("message"),
                            "exc_info": entry.get("exc_info"),
                        }
                        for entry in error_entries
                    ])
                except Exception as e:
                    logger.debug(f"Failed to serialize error logs for {executor_id}: {e}")

            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)

                updated = await repo.update_executor(
                    executor_id=executor_id,
                    status=status_name,
                    close_type=close_type,
                    net_pnl_quote=net_pnl_quote,
                    net_pnl_pct=net_pnl_pct,
                    cum_fees_quote=cum_fees_quote,
                    filled_amount_quote=filled_amount_quote,
                    final_state=final_state_json,
                    error_log=error_log_json
                )
                if updated is None and metadata:
                    upsert_type = metadata.get("executor_type")
                    upsert_account = metadata.get("account_name")
                    upsert_connector = metadata.get("connector_name")
                    upsert_pair = metadata.get("trading_pair")
                    if all([upsert_type, upsert_account, upsert_connector, upsert_pair]):
                        await repo.create_executor(
                            executor_id=executor_id,
                            executor_type=upsert_type,
                            account_name=upsert_account,
                            connector_name=upsert_connector,
                            trading_pair=upsert_pair,
                            config=json.dumps(metadata.get("config", {}), default=_json_default),
                            status=status_name,
                            controller_id=metadata.get("controller_id", "main"),
                        )
                        await repo.update_executor(
                            executor_id=executor_id,
                            status=status_name,
                            close_type=close_type,
                            net_pnl_quote=net_pnl_quote,
                            net_pnl_pct=net_pnl_pct,
                            cum_fees_quote=cum_fees_quote,
                            filled_amount_quote=filled_amount_quote,
                            final_state=final_state_json,
                            error_log=error_log_json
                        )
                    else:
                        logger.error(
                            "Cannot upsert completed executor %s: incomplete metadata %s",
                            executor_id,
                            metadata,
                        )

            logger.debug(f"Persisted executor {executor_id} completion to database")

        except Exception as e:
            logger.error(f"Error persisting executor completion: {e}")

    # ========================================
    # Position Hold Tracking Methods
    # ========================================

    def _get_position_key(
        self,
        account_name: str,
        connector_name: str,
        trading_pair: str,
        controller_id: str = "main"
    ) -> str:
        """Generate a unique key for position tracking."""
        return f"{account_name}|{connector_name}|{trading_pair}|{controller_id}"

    async def _aggregate_position_hold(
        self,
        executor_id: str,
        executor: ExecutorBase,
        metadata: Dict[str, Any]
    ):
        """
        Aggregate position data from an executor stopped with keep_position=True.

        This extracts the filled amounts from the executor and adds them to
        the aggregated position tracking.
        """
        account_name = metadata.get("account_name", self.default_account)
        connector_name = metadata.get("connector_name", "")
        trading_pair = metadata.get("trading_pair", "")
        controller_id = metadata.get("controller_id", "main")

        if not connector_name or not trading_pair:
            logger.warning(f"Cannot aggregate position for executor {executor_id}: missing connector/pair info")
            return

        position_key = self._get_position_key(account_name, connector_name, trading_pair, controller_id)

        # Get or create position hold
        if position_key not in self._positions_held:
            self._positions_held[position_key] = PositionHold(
                trading_pair=trading_pair,
                connector_name=connector_name,
                account_name=account_name,
                controller_id=controller_id
            )

        position = self._positions_held[position_key]

        # Extract filled amounts from executor
        try:
            # Try to get executor info
            try:
                executor_info = executor.executor_info
                custom_info = executor_info.custom_info or {}
            except Exception:
                custom_info = executor.get_custom_info() if hasattr(executor, 'get_custom_info') else {}

            # Get side from config or custom_info
            config = metadata.get("config", {})
            side = config.get("side", custom_info.get("side", "BUY"))

            # Extract filled amounts - try different sources
            filled_amount_base = Decimal("0")
            filled_amount_quote = Decimal("0")

            # Try from executor attributes directly
            if hasattr(executor, 'filled_amount_base'):
                filled_amount_base = Decimal(str(executor.filled_amount_base or 0))
            if hasattr(executor, 'filled_amount_quote'):
                filled_amount_quote = Decimal(str(executor.filled_amount_quote or 0))

            # Fallback to custom_info
            if filled_amount_base == 0 and custom_info:
                filled_amount_base = Decimal(str(custom_info.get("filled_amount_base", 0)))
            if filled_amount_quote == 0 and custom_info:
                filled_amount_quote = Decimal(str(custom_info.get("filled_amount_quote", 0)))

            # Check for held_position_orders (used by grid_executor, position_executor, etc.)
            held_orders = custom_info.get("held_position_orders", []) if custom_info else []

            # Extract cumulative fees from the executor
            executor_fees = Decimal("0")
            try:
                executor_fees = Decimal(str(executor.cum_fees_quote or 0))
            except Exception:
                pass

            if held_orders:
                buy_filled_base = Decimal("0")
                buy_filled_quote = Decimal("0")
                sell_filled_base = Decimal("0")
                sell_filled_quote = Decimal("0")
                orders_fees = Decimal("0")

                for order in held_orders:
                    if isinstance(order, dict):
                        trade_type = order.get("trade_type", "BUY")
                        exec_base = Decimal(str(order.get("executed_amount_base", 0)))
                        exec_quote = Decimal(str(order.get("executed_amount_quote", 0)))
                        orders_fees += Decimal(str(order.get("cumulative_fee_paid_quote", 0)))

                        if trade_type == "BUY":
                            buy_filled_base += exec_base
                            buy_filled_quote += exec_quote
                        else:
                            sell_filled_base += exec_base
                            sell_filled_quote += exec_quote

                # Use order-level fees if available, otherwise fall back to executor-level
                fees = orders_fees if orders_fees > 0 else executor_fees

                # Add buy and sell fills separately
                if buy_filled_base > 0:
                    # Split fees proportionally between buy and sell by quote volume
                    total_quote = buy_filled_quote + sell_filled_quote
                    buy_fee_share = fees * (buy_filled_quote / total_quote) if total_quote > 0 else fees
                    position.add_fill("BUY", buy_filled_base, buy_filled_quote, executor_id, fees_quote=buy_fee_share)
                if sell_filled_base > 0:
                    total_quote = buy_filled_quote + sell_filled_quote
                    sell_fee_share = fees * (sell_filled_quote / total_quote) if total_quote > 0 else fees
                    position.add_fill("SELL", sell_filled_base, sell_filled_quote, executor_id, fees_quote=sell_fee_share)

                logger.info(
                    f"Aggregated executor {executor_id} to position {position_key}: "
                    f"buy={buy_filled_base} base, sell={sell_filled_base} base, fees={fees} quote"
                )

            elif filled_amount_base > 0:
                # For non-grid executors with a single side
                position.add_fill(side, filled_amount_base, filled_amount_quote, executor_id, fees_quote=executor_fees)
                logger.info(
                    f"Aggregated executor {executor_id} to position {position_key}: "
                    f"{side} {filled_amount_base} base @ {filled_amount_quote} quote"
                )
            else:
                logger.debug(f"Executor {executor_id} has no filled amounts to aggregate")

            # Persist position hold to the dedicated table
            await self._persist_position_hold(position)

        except Exception as e:
            logger.error(f"Error aggregating position for executor {executor_id}: {e}", exc_info=True)

    async def _persist_position_hold(self, position: PositionHold):
        """Persist a position hold to the dedicated position_holds table."""
        if not self.db_manager:
            return
        try:
            async with self.db_manager.get_session_context() as session:
                repo = ExecutorRepository(session)
                await repo.upsert_position_hold(
                    account_name=position.account_name,
                    connector_name=position.connector_name,
                    trading_pair=position.trading_pair,
                    controller_id=position.controller_id,
                    buy_amount_base=position.buy_amount_base,
                    buy_amount_quote=position.buy_amount_quote,
                    sell_amount_base=position.sell_amount_base,
                    sell_amount_quote=position.sell_amount_quote,
                    realized_pnl_quote=position.realized_pnl_quote,
                    cum_fees_quote=position.cum_fees_quote,
                    executor_ids=position.executor_ids,
                )
        except Exception as e:
            logger.error(f"Error persisting position hold: {e}", exc_info=True)

    def get_positions_held(
        self,
        account_name: Optional[str] = None,
        connector_name: Optional[str] = None,
        trading_pair: Optional[str] = None,
        controller_id: Optional[str] = None
    ) -> List[PositionHold]:
        """
        Get held positions with optional filtering.

        Args:
            account_name: Filter by account name
            connector_name: Filter by connector name
            trading_pair: Filter by trading pair
            controller_id: Filter by controller ID

        Returns:
            List of PositionHold objects matching the filters
        """
        positions = []

        for position in self._positions_held.values():
            # Apply filters
            if account_name and position.account_name != account_name:
                continue
            if connector_name and position.connector_name != connector_name:
                continue
            if trading_pair and position.trading_pair != trading_pair:
                continue
            if controller_id and position.controller_id != controller_id:
                continue

            # Only include positions with actual volume
            if position.buy_amount_base > 0 or position.sell_amount_base > 0:
                positions.append(position)

        return positions

    def get_position_held(
        self,
        account_name: str,
        connector_name: str,
        trading_pair: str,
        controller_id: str = "main"
    ) -> Optional[PositionHold]:
        """
        Get a specific held position.

        Args:
            account_name: Account name
            connector_name: Connector name
            trading_pair: Trading pair
            controller_id: Controller ID

        Returns:
            PositionHold or None if not found
        """
        position_key = self._get_position_key(account_name, connector_name, trading_pair, controller_id)
        return self._positions_held.get(position_key)

    async def clear_position_held(
        self,
        account_name: str,
        connector_name: str,
        trading_pair: str,
        controller_id: str = "main"
    ) -> bool:
        """
        Clear a specific held position (after manual close or full exit).

        Args:
            account_name: Account name
            connector_name: Connector name
            trading_pair: Trading pair
            controller_id: Controller ID

        Returns:
            True if cleared, False if not found
        """
        position_key = self._get_position_key(account_name, connector_name, trading_pair, controller_id)
        if position_key in self._positions_held:
            del self._positions_held[position_key]
            # Mark position hold as CLEARED in the dedicated table
            if self.db_manager:
                try:
                    async with self.db_manager.get_session_context() as session:
                        repo = ExecutorRepository(session)
                        cleared = await repo.clear_position_hold(
                            account_name=account_name,
                            connector_name=connector_name,
                            trading_pair=trading_pair,
                            controller_id=controller_id
                        )
                        logger.info(f"Cleared position hold record from database for {position_key}: {cleared}")
                except Exception as e:
                    logger.error(f"Failed to clear position hold from database: {e}", exc_info=True)
            logger.info(f"Cleared position hold for {position_key}")
            return True
        return False

    def get_positions_summary(self) -> Dict[str, Any]:
        """
        Get summary of all held positions.

        Returns:
            Dictionary with total positions, PnL, and position list
        """
        positions = self.get_positions_held()
        total_realized_pnl = sum(float(p.realized_pnl_quote) for p in positions)

        return {
            "total_positions": len(positions),
            "total_realized_pnl": total_realized_pnl,
            "positions": [
                {
                    "trading_pair": p.trading_pair,
                    "connector_name": p.connector_name,
                    "account_name": p.account_name,
                    "buy_amount_base": float(p.buy_amount_base),
                    "buy_amount_quote": float(p.buy_amount_quote),
                    "sell_amount_base": float(p.sell_amount_base),
                    "sell_amount_quote": float(p.sell_amount_quote),
                    "net_amount_base": float(p.net_amount_base),
                    "buy_breakeven_price": float(p.buy_breakeven_price) if p.buy_breakeven_price else None,
                    "sell_breakeven_price": float(p.sell_breakeven_price) if p.sell_breakeven_price else None,
                    "matched_amount_base": float(p.matched_amount_base),
                    "unmatched_amount_base": float(p.unmatched_amount_base),
                    "position_side": p.position_side,
                    "realized_pnl_quote": float(p.realized_pnl_quote),
                    "cum_fees_quote": float(p.cum_fees_quote),
                    "executor_count": len(p.executor_ids),
                    "executor_ids": p.executor_ids,
                    "last_updated": p.last_updated.isoformat() if p.last_updated else None
                }
                for p in positions
            ]
        }
