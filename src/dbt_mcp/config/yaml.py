from pathlib import Path

import yaml


def try_read_yaml(file_path: Path) -> dict | None:
    try:
        suffix = file_path.suffix.lower()
        if suffix not in {".yml", ".yaml"}:
            return None
        alternate_suffix = ".yaml" if suffix == ".yml" else ".yml"
        alternate_path = file_path.with_suffix(alternate_suffix)
        if file_path.exists():
            return yaml.safe_load(file_path.read_text())
        if alternate_path.exists():
            return yaml.safe_load(alternate_path.read_text())
    except Exception:
        return None
    return None
