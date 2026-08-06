"""controller_id must come from config when top-level create arg is missing/default."""

from services.executor_service import ExecutorService


def test_resolve_prefers_explicit_non_main():
    assert (
        ExecutorService.resolve_controller_id(
            "agent.strategy_1",
            config={"controller_id": "other"},
        )
        == "agent.strategy_1"
    )


def test_resolve_uses_config_when_explicit_missing_or_main():
    cfg = {"controller_id": "macdbb_scanner_aggressive_hl.macdbb_scanner_aggressive_hl_20"}
    assert ExecutorService.resolve_controller_id(None, config=cfg) == cfg["controller_id"]
    assert ExecutorService.resolve_controller_id("main", config=cfg) == cfg["controller_id"]


def test_resolve_defaults_to_main():
    assert ExecutorService.resolve_controller_id(None, config={}) == "main"
    assert ExecutorService.resolve_controller_id("main", config={}) == "main"


def test_effective_controller_id_for_legacy_main_column():
    assert (
        ExecutorService.effective_controller_id(
            top_level="main",
            config={
                "controller_id": "macdbb_scanner_aggressive_hl.macdbb_scanner_aggressive_hl_20"
            },
        )
        == "macdbb_scanner_aggressive_hl.macdbb_scanner_aggressive_hl_20"
    )
