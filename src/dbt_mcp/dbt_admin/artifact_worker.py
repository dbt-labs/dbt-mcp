"""Standalone artifact worker; imports no server modules or credentials."""

import json
import os
import sys

import jq

if sys.platform != "win32":
    import resource


def main() -> None:
    if sys.platform == "linux" and sys.argv[3]:
        memory_bytes = int(sys.argv[3])
        resource.setrlimit(resource.RLIMIT_AS, (memory_bytes, memory_bytes))
    os.environ.clear()
    try:
        # jq's text parser accepts JSON streams; artifacts must be one document.
        program = jq.compile(
            'if length == 1 then .[0] else error("parse error: Expected one JSON document") end | (\n'
            f"{sys.argv[2]}\n)"
        )
        with open(sys.argv[1], encoding="utf-8") as source:
            results = program.input_text(source.read(), slurp=True)
        encoder = json.JSONEncoder(separators=(",", ":"))
        sys.stdout.write("[")
        for index, value in enumerate(results):
            if index:
                sys.stdout.write(",")
            for chunk in encoder.iterencode(value):
                sys.stdout.write(chunk)
        sys.stdout.write("]")
    except ValueError as error:
        if str(error).startswith("parse error:"):
            sys.stderr.write(
                "jq_filter requires a JSON artifact; this artifact is not valid JSON"
            )
        else:
            sys.stderr.write(f"Invalid jq filter: {str(error)[:4096]}")
        sys.exit(2)
    except MemoryError:
        sys.stderr.write(
            "Artifact worker memory limit exceeded; select a smaller artifact or step."
        )
        sys.exit(3)


if __name__ == "__main__":
    main()
