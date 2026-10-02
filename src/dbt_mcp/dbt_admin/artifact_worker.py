"""Standalone artifact worker; imports no server modules or credentials."""

import json
import os
import sys

import jq

if sys.platform != "win32":
    import resource


def main() -> None:
    if sys.platform == "linux":
        memory_bytes = int(sys.argv[3])
        resource.setrlimit(resource.RLIMIT_AS, (memory_bytes, memory_bytes))
    os.environ.clear()
    try:
        with open(sys.argv[1], encoding="utf-8") as source:
            document = json.load(source)
        encoder = json.JSONEncoder(separators=(",", ":"))
        sys.stdout.write("[")
        for index, value in enumerate(jq.compile(sys.argv[2]).input(document)):
            if index:
                sys.stdout.write(",")
            for chunk in encoder.iterencode(value):
                sys.stdout.write(chunk)
        sys.stdout.write("]")
    except json.JSONDecodeError:
        sys.stderr.write(
            "jq_filter requires a JSON artifact; this artifact is not valid JSON"
        )
        sys.exit(2)
    except ValueError as error:
        sys.stderr.write(f"Invalid jq filter: {str(error)[:4096]}")
        sys.exit(2)
    except MemoryError:
        sys.stderr.write(
            "Artifact worker memory limit exceeded; select a smaller artifact or step."
        )
        sys.exit(3)


if __name__ == "__main__":
    main()
