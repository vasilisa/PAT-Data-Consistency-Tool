"""
tests/test_loader_schema.py
===========================
Unit tests for loader schema propagation helpers.
"""
from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from dash_app.engine import loader


class _FakeDataset:
    def __init__(self, name: str, registry: dict[str, dict]):
        self.name = name
        self.registry = registry

    def read_schema(self):
        return list(self.registry[self.name].get("schema", []))

    def write_schema(self, schema):
        self.registry.setdefault(self.name, {})["written_schema"] = list(schema)


class _FakeProject:
    def __init__(self, recipe_obj):
        self._recipe_obj = recipe_obj

    def list_datasets(self):
        return [{"name": loader.AGG_DATASET_NAME}]

    def list_recipes(self):
        return [{"name": loader.AGG_RECIPE_NAME}]

    def get_recipe(self, _name):
        return self._recipe_obj


@pytest.fixture
def fake_dataiku(monkeypatch):
    registry = {
        "tbl_DetailedData": {
            "schema": [
                {"name": "AsAt_Month", "type": "string"},
                {"name": "Segment", "type": "string"},
                {"name": "Region", "type": "string"},
                {"name": "Premium", "type": "double"},
            ]
        },
        loader.AGG_DATASET_NAME: {},
    }

    fake_module = SimpleNamespace(
        Dataset=lambda name: _FakeDataset(name, registry),
    )
    monkeypatch.setitem(sys.modules, "dataiku", fake_module)
    return registry


class TestBuildAggSchema:

    def test_builds_expected_columns_in_order(self, fake_dataiku):
        schema = loader._build_agg_schema(["AsAt_Month", "Segment"])

        assert [c["name"] for c in schema] == ["AsAt_Month", "Segment", "Premium"]
        assert schema[-1]["type"] == "double"

    def test_raises_when_group_col_missing_from_source_schema(self, fake_dataiku):
        with pytest.raises(RuntimeError, match="Grouping column 'Unknown_Col'"):
            loader._build_agg_schema(["AsAt_Month", "Unknown_Col"])


class TestSyncAggOutputSchema:

    def test_writes_schema_to_agg_dataset(self, fake_dataiku, monkeypatch):
        expected_schema = [
            {"name": "AsAt_Month", "type": "string"},
            {"name": "Premium", "type": "double"},
        ]
        monkeypatch.setattr(loader, "_build_agg_schema", lambda _cols: expected_schema)

        loader._sync_agg_output_schema(["AsAt_Month"])

        assert fake_dataiku[loader.AGG_DATASET_NAME]["written_schema"] == expected_schema


class TestEnsureDdAggregatedSchemaHook:

    def test_calls_schema_sync_before_build(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "dataiku", SimpleNamespace())

        call_order = []
        recipe_obj = object()
        project = _FakeProject(recipe_obj)

        monkeypatch.setattr(loader, "_get_dd_group_cols", lambda _loaded: ["AsAt_Month"])
        monkeypatch.setattr(loader, "_get_recipe_output_refs", lambda _recipe: {loader.AGG_DATASET_NAME})
        monkeypatch.setattr(
            loader,
            "_configure_agg_recipe",
            lambda _recipe, _cols: call_order.append("configure"),
        )
        monkeypatch.setattr(
            loader,
            "_sync_agg_output_schema",
            lambda _cols: call_order.append("sync"),
        )
        monkeypatch.setattr(
            loader,
            "_build_agg_recipe",
            lambda _project: call_order.append("build"),
        )

        assert loader._ensure_dd_aggregated(project, {}) is True
        assert call_order == ["configure", "sync", "build"]
