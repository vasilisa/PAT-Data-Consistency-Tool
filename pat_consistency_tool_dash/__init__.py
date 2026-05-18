"""Compatibility package exposing the existing dash_app modules under the DSS package name.

This keeps local imports working when code uses the packaged namespace
`pat_consistency_tool_dash` used in Dataiku project libraries.
"""
from pathlib import Path

# Route submodule discovery (engine, runner, ui, app) to the existing dash_app tree.
__path__ = [str(Path(__file__).resolve().parent.parent / "dash_app")]
