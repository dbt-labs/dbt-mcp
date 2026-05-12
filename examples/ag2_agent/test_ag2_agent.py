"""Structural tests for AG2 dbt-mcp example — syntax and import verification."""
import ast
from pathlib import Path

EXAMPLE_DIR = Path(__file__).parent

def _parse(filename: str) -> ast.Module:
    src = (EXAMPLE_DIR / filename).read_text()
    return ast.parse(src)  # raises SyntaxError if invalid

def test_main_remote_syntax():
    _parse("main_remote.py")

def test_main_multiagent_syntax():
    _parse("main_multiagent.py")

def test_main_stdio_syntax():
    _parse("main_stdio.py")

def test_multiagent_uses_swarm():
    src = (EXAMPLE_DIR / "main_multiagent.py").read_text()
    # Should use AG2 swarm or group chat pattern
    assert "SwarmAgent" in src or "GroupChat" in src or "run_swarm" in src or "initiate_group_chat" in src

def test_remote_uses_mcp():
    src = (EXAMPLE_DIR / "main_remote.py").read_text()
    assert "mcp" in src.lower() or "ClientSession" in src or "sse_client" in src
