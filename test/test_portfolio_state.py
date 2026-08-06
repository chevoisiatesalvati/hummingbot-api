"""
Tests for Portfolio State refresh behavior.

Run with: pytest test/test_portfolio_state.py -v
"""
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("hummingbot")


class TestPortfolioStateRefresh:
    """Tests for portfolio state refresh behavior."""

    @pytest.mark.asyncio
    async def test_refresh_true_calls_update_account_state(self):
        """refresh=True should call update_account_state."""
        from models.trading import PortfolioStateFilterRequest
        from routers.portfolio import get_portfolio_state

        mock_service = MagicMock()
        mock_service.update_account_state = AsyncMock()
        mock_service.get_accounts_state.return_value = {}

        request = PortfolioStateFilterRequest(refresh=True)
        await get_portfolio_state(request, mock_service)

        mock_service.update_account_state.assert_called_once()

    @pytest.mark.asyncio
    async def test_refresh_false_does_not_call_update_account_state(self):
        """refresh=False should NOT call update_account_state."""
        from models.trading import PortfolioStateFilterRequest
        from routers.portfolio import get_portfolio_state

        mock_service = MagicMock()
        mock_service.update_account_state = AsyncMock()
        mock_service.get_accounts_state.return_value = {}

        request = PortfolioStateFilterRequest(refresh=False)
        await get_portfolio_state(request, mock_service)

        mock_service.update_account_state.assert_not_called()


class TestBalanceRefresh:
    """Tests for _get_connector_tokens_info balance refresh."""

    @pytest.fixture
    def accounts_service(self):
        """Create AccountsService with mocked dependencies."""
        from services.accounts_service import AccountsService

        service = AccountsService.__new__(AccountsService)
        service._market_data_service = MagicMock()
        service._market_data_service.get_rate.return_value = Decimal("1")
        return service

    @pytest.fixture
    def mock_connector(self):
        """Create a mock connector."""
        connector = MagicMock()
        connector._update_balances = AsyncMock()
        connector.get_all_balances.return_value = {"USDT": Decimal("1000")}
        connector.get_available_balance.return_value = Decimal("1000")
        return connector

    @pytest.mark.asyncio
    async def test_calls_update_balances(self, accounts_service, mock_connector):
        """_get_connector_tokens_info should call _update_balances."""
        await accounts_service._get_connector_tokens_info(mock_connector, "okx")

        mock_connector._update_balances.assert_called_once()

    @pytest.mark.asyncio
    async def test_skips_update_balances_when_requested(self, accounts_service, mock_connector):
        """skip_balance_refresh=True should skip _update_balances."""
        await accounts_service._get_connector_tokens_info(
            mock_connector, "okx", skip_balance_refresh=True
        )

        mock_connector._update_balances.assert_not_called()

    @pytest.mark.asyncio
    async def test_balance_failure_preserves_stale_data(self, accounts_service, mock_connector):
        """_update_balances failure should preserve stale cached data."""
        mock_connector._update_balances = AsyncMock(side_effect=Exception("API error"))
        mock_connector.get_all_balances.return_value = {"USDT": Decimal("500")}

        result = await accounts_service._get_connector_tokens_info(mock_connector, "okx")

        # Should still return data from get_all_balances (stale cache)
        assert len(result) == 1
        assert result[0]["token"] == "USDT"
        assert result[0]["units"] == 500.0


class TestGatewayWalletState:
    """Gateway wallet balances must not be overwritten by the Gateway connector's own view."""

    @pytest.fixture
    def accounts_service(self):
        from services.accounts_service import AccountsService

        service = AccountsService.__new__(AccountsService)
        service.accounts_state = {}
        service._connector_service = MagicMock()
        return service

    @pytest.mark.asyncio
    async def test_gateway_connector_does_not_overwrite_wallet_balances(self, accounts_service):
        """A Gateway connector must not replace the full wallet balances with its own tokens."""
        wallet_balances = [
            {"token": "SOL", "units": 0.35, "price": 0.0, "value": 0.0, "available_units": 0.35},
            {"token": "USDC", "units": 1.31, "price": 1.0, "value": 1.31, "available_units": 1.31},
            {"token": "TRUMP", "units": 0.15, "price": 0.0, "value": 0.0, "available_units": 0.15},
        ]

        gateway_connector = MagicMock()
        accounts_service._connector_service.get_all_trading_connectors.return_value = {
            "master_account": {"solana-mainnet-beta": gateway_connector}
        }
        accounts_service._connector_service.is_gateway_connector.return_value = True

        async def fake_gateway_update(chain_networks=None):
            accounts_service.accounts_state["master_account"]["solana-mainnet-beta"] = wallet_balances

        accounts_service._update_gateway_balances = fake_gateway_update
        accounts_service._get_connector_tokens_info = AsyncMock(
            return_value=[{"token": "SOL", "units": 0.35}]
        )

        await accounts_service.update_account_state(connector_names=["solana-mainnet-beta"])

        accounts_service._get_connector_tokens_info.assert_not_called()
        assert accounts_service.accounts_state["master_account"]["solana-mainnet-beta"] == wallet_balances

    @pytest.mark.asyncio
    async def test_gateway_state_mirrored_to_other_accounts(self, accounts_service):
        """Non-master accounts with a Gateway connector get the same wallet balances."""
        wallet_balances = [{"token": "TRUMP", "units": 0.15, "price": 0.0, "value": 0.0}]

        accounts_service._connector_service.get_all_trading_connectors.return_value = {
            "trader_account": {"solana-mainnet-beta": MagicMock()}
        }
        accounts_service._connector_service.is_gateway_connector.return_value = True

        async def fake_gateway_update(chain_networks=None):
            accounts_service.accounts_state.setdefault("master_account", {})
            accounts_service.accounts_state["master_account"]["solana-mainnet-beta"] = wallet_balances

        accounts_service._update_gateway_balances = fake_gateway_update

        await accounts_service.update_account_state()

        assert accounts_service.accounts_state["trader_account"]["solana-mainnet-beta"] == wallet_balances
