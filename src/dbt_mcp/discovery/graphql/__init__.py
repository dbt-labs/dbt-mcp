from importlib import resources


def load_query(filename: str) -> str:
    return resources.files(__name__).joinpath(filename).read_text(encoding="utf-8")
