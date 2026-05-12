from pathlib import Path


def get_prompt(name: str) -> str:
    return (Path(__file__).parent / f"{name}.md").read_text(encoding="utf-8")
