import pytest

from dbt_mcp.discovery.tools import get_lineage
from dbt_mcp.errors import InvalidParameterError


class TestGetLineageValidation:
    """Test parameter validation."""

    async def test_requires_name_or_unique_id(self, mock_discovery_context, mock_mcp_context):
        """Should raise error when neither name nor unique_id is provided."""
        with pytest.raises(InvalidParameterError, match="Either 'name' or 'unique_id' must be provided"):
            await get_lineage.fn(mock_discovery_context, mock_mcp_context)

    async def test_rejects_both_name_and_unique_id(self, mock_discovery_context, mock_mcp_context):
        """Should raise error when both parameters are provided."""
        with pytest.raises(InvalidParameterError, match="Only one of"):
            await get_lineage.fn(
                mock_discovery_context, mock_mcp_context, name="customers", unique_id="model.test.customers"
            )

    async def test_validates_direction(self, mock_discovery_context, mock_mcp_context):
        """Should reject invalid direction values."""
        with pytest.raises(InvalidParameterError, match="direction must be one of"):
            await get_lineage.fn(mock_discovery_context, mock_mcp_context, name="customers", direction="invalid")

    async def test_validates_resource_types(self, mock_discovery_context, mock_mcp_context):
        """Should reject invalid resource types."""
        with pytest.raises(InvalidParameterError, match="Invalid resource type"):
            await get_lineage.fn(
                mock_discovery_context, mock_mcp_context, name="customers", types=["InvalidType"]
            )


class TestGetLineageResolution:
    """Test name-to-unique_id resolution."""

    async def test_no_matches_returns_helpful_error(self, mock_discovery_context, mock_mcp_context):
        """Should return helpful error when no matches found, suggesting exposure/test/metric."""
        mock_discovery_context.lineage_fetcher.search_all_resources.return_value = []

        with pytest.raises(InvalidParameterError) as exc_info:
            await get_lineage.fn(mock_discovery_context, mock_mcp_context, name="my_dashboard")

        error_message = str(exc_info.value)
        # Check that error message is helpful
        assert "No resource found with name 'my_dashboard'" in error_message
        assert "models, sources, seeds, snapshots" in error_message
        assert "exposure, test, or metric" in error_message
        assert "unique_id" in error_message
        assert "exposure.project.my_dashboard" in error_message

    async def test_single_match_resolves(self, mock_discovery_context, mock_mcp_context):
        """Should resolve name to unique_id when single match found."""
        mock_discovery_context.lineage_fetcher.search_all_resources.return_value = [
            {"uniqueId": "model.test.customers", "name": "customers", "resourceType": "Model"}
        ]
        mock_discovery_context.lineage_fetcher.fetch_lineage.return_value = {
            "target": {"uniqueId": "model.test.customers"},
            "ancestors": [],
            "descendants": [],
        }

        result = await get_lineage.fn(mock_discovery_context, mock_mcp_context, name="customers")

        assert result["target"]["uniqueId"] == "model.test.customers"
        mock_discovery_context.lineage_fetcher.fetch_lineage.assert_called_once()

    async def test_multiple_matches_elicitation_success(self, mock_discovery_context, mock_mcp_context):
        """Should use elicited choice when user accepts."""
        from unittest.mock import Mock

        mock_discovery_context.lineage_fetcher.search_all_resources.return_value = [
            {"uniqueId": "model.test.customers", "name": "customers", "resourceType": "Model"},
            {"uniqueId": "source.test.raw.customers", "name": "customers", "resourceType": "Source"},
        ]

        # Mock successful elicitation
        elicit_result = Mock()
        elicit_result.action = "accept"
        elicit_result.data = Mock()
        elicit_result.data.unique_id = "model.test.customers"
        mock_mcp_context.elicit.return_value = elicit_result

        mock_discovery_context.lineage_fetcher.fetch_lineage.return_value = {
            "target": {"uniqueId": "model.test.customers"},
            "ancestors": [],
            "descendants": [],
        }

        result = await get_lineage.fn(mock_discovery_context, mock_mcp_context, name="customers")

        # Should fetch lineage with user's chosen unique_id
        assert result["target"]["uniqueId"] == "model.test.customers"
        mock_discovery_context.lineage_fetcher.fetch_lineage.assert_called_once_with(
            unique_id="model.test.customers",
            direction="both",
            types=None
        )

    async def test_multiple_matches_returns_disambiguation(self, mock_discovery_context, mock_mcp_context):
        """Should return disambiguation response when elicitation fails."""
        mock_discovery_context.lineage_fetcher.search_all_resources.return_value = [
            {"uniqueId": "model.test.customers", "name": "customers", "resourceType": "Model"},
            {"uniqueId": "source.test.raw.customers", "name": "customers", "resourceType": "Source"},
        ]
        # Simulate elicitation failure
        mock_mcp_context.elicit.side_effect = Exception("Elicitation not supported")

        result = await get_lineage.fn(mock_discovery_context, mock_mcp_context, name="customers")

        assert result["status"] == "disambiguation_required"
        assert len(result["matches"]) == 2

    async def test_unique_id_skips_resolution(self, mock_discovery_context, mock_mcp_context):
        """Should use unique_id directly without searching."""
        mock_discovery_context.lineage_fetcher.fetch_lineage.return_value = {
            "target": {"uniqueId": "model.test.customers"},
            "ancestors": [],
        }

        await get_lineage.fn(mock_discovery_context, mock_mcp_context, unique_id="model.test.customers")

        mock_discovery_context.lineage_fetcher.search_all_resources.assert_not_called()
        mock_discovery_context.lineage_fetcher.fetch_lineage.assert_called_once()
