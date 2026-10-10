from dbt_mcp.dbt_cli.models.manifest import Manifest, looks_like_unique_id


def test_looks_like_unique_id_rejects_names_and_method_selectors() -> None:
    assert looks_like_unique_id("model.my_project.orders")
    assert looks_like_unique_id("source.my_project.raw.payments")
    assert not looks_like_unique_id("orders")
    assert not looks_like_unique_id("source:raw.payments")
    assert not looks_like_unique_id("my_project.orders")


def test_selector_for_method_resources() -> None:
    manifest = Manifest.model_validate(
        {
            "metrics": {
                "metric.my_project.revenue": {
                    "name": "revenue",
                    "package_name": "my_project",
                }
            },
            "semantic_models": {
                "semantic_model.my_project.orders": {
                    "name": "orders",
                    "package_name": "my_project",
                }
            },
            "saved_queries": {
                "saved_query.my_project.orders_query": {
                    "name": "orders_query",
                    "package_name": "my_project",
                }
            },
            "unit_tests": {
                "unit_test.my_project.test_orders": {
                    "name": "test_orders",
                    "package_name": "my_project",
                }
            },
        }
    )
    assert (
        manifest.selector_for_unique_id("metric.my_project.revenue") == "metric:revenue"
    )
    assert (
        manifest.selector_for_unique_id("semantic_model.my_project.orders")
        == "semantic_model:orders"
    )
    assert (
        manifest.selector_for_unique_id("saved_query.my_project.orders_query")
        == "saved_query:orders_query"
    )
    assert (
        manifest.selector_for_unique_id("unit_test.my_project.test_orders")
        == "unit_test:test_orders"
    )


def test_selector_qualifies_source_and_exposure_when_name_is_shared() -> None:
    manifest = Manifest.model_validate(
        {
            "sources": {
                "source.my_project.raw.payments": {
                    "name": "payments",
                    "source_name": "raw",
                    "package_name": "my_project",
                    "identifier": "payments",
                },
                "source.other_pkg.raw.payments": {
                    "name": "payments",
                    "source_name": "raw",
                    "package_name": "other_pkg",
                    "identifier": "payments",
                },
            },
            "exposures": {
                "exposure.my_project.revenue_dashboard": {
                    "name": "revenue_dashboard",
                    "package_name": "my_project",
                },
                "exposure.other_pkg.revenue_dashboard": {
                    "name": "revenue_dashboard",
                    "package_name": "other_pkg",
                },
            },
        }
    )
    assert (
        manifest.selector_for_unique_id("source.my_project.raw.payments")
        == "source:my_project.raw.payments"
    )
    assert (
        manifest.selector_for_unique_id("exposure.my_project.revenue_dashboard")
        == "exposure:my_project.revenue_dashboard"
    )


def test_selector_uses_fqn_when_model_name_matches_an_exposure() -> None:
    manifest = Manifest.model_validate(
        {
            "nodes": {
                "model.my_project.orders": {
                    "name": "orders",
                    "resource_type": "model",
                    "package_name": "my_project",
                    "fqn": ["my_project", "staging", "orders"],
                }
            },
            "exposures": {
                "exposure.my_project.orders": {
                    "name": "orders",
                    "package_name": "my_project",
                }
            },
        }
    )
    assert (
        manifest.selector_for_unique_id("model.my_project.orders")
        == "my_project.staging.orders"
    )
    assert (
        manifest.selector_for_unique_id("exposure.my_project.orders")
        == "exposure:orders"
    )


def test_selector_returns_none_when_unique_id_is_absent() -> None:
    manifest = Manifest()
    assert manifest.selector_for_unique_id("model.my_project.missing") is None
    assert manifest.selector_for_unique_id("seed.my_project.country_codes") is None
