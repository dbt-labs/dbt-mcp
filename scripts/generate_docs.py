import argparse
import logging
import re
import sys
import subprocess
import tempfile
from pathlib import Path

from dbt_mcp.tools.toolsets import toolsets

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

README_PATH = Path(__file__).parents[1] / "README.md"
DIAGRAM_PATH = Path(__file__).parents[1] / "docs" / "diagram.d2"
PNG_PATH = Path(__file__).parents[1] / "docs" / "d2.png"


def format_toolset_heading(toolset_value: str) -> str:
    """
    Format toolset enum value into a readable heading using regex.

    Examples:
        sql -> SQL
        semantic_layer -> Semantic Layer
        dbt_cli -> dbt CLI
        admin_api -> Admin API
    """
    text = toolset_value.replace("_", " ").title()

    # Uppercase common acronyms
    text = re.sub(r"\b(Sql|Api|Cli|Lsp|Mcp)\b", lambda m: m.group(1).upper(), text)

    # "dbt" stylized as lowercase
    text = re.sub(r"\bDbt\b", "dbt", text)

    return text


def generate_readme_tools_section() -> str:
    """Generate the Tools section markdown from toolsets."""
    lines = ["## Tools", ""]

    for toolset, tool_names in toolsets.items():
        heading = format_toolset_heading(toolset.value)
        lines.append(f"### {heading}")

        sorted_tools = sorted([tool.value for tool in tool_names])
        for tool in sorted_tools:
            lines.append(f"- `{tool}`")

        lines.append("")  # Empty line after each section

    return "\n".join(lines)


def generate_diagram_tools_section() -> str:
    """Generate the tools section for diagram.d2 from toolsets."""
    lines = ["tools: Tools {"]

    for toolset in toolsets.keys():
        # Use enum value directly as key (e.g., "dbt_cli", "semantic_layer")
        key = toolset.value
        # Reuse same formatting as README headings
        label = format_toolset_heading(toolset.value)

        lines.append(f"  {key}: {label} {{")
        lines.append("    style.border-radius: 8")
        lines.append("  }")
        lines.append("")  # Empty line between tools

    lines.append("}")

    return "\n".join(lines)


def update_readme(check_only: bool = False) -> bool:
    """
    Update the Tools section in README.md.

    Args:
        check_only: If True, only check if update is needed without writing.

    Returns:
        True if README is up to date (or was updated), False if update is needed.
    """
    if not README_PATH.exists():
        logger.error(f"README.md not found at {README_PATH}")
        return False

    readme_content = README_PATH.read_text()
    new_tools_section = generate_readme_tools_section()

    # Replace Tools section (from "## Tools" to next "##" heading or end)
    pattern = r"(## Tools\n).*?(?=\n## |\Z)"
    if not re.search(pattern, readme_content, re.DOTALL):
        logger.error("Could not find '## Tools' section in README.md")
        return False

    updated_content = re.sub(
        pattern, new_tools_section + "\n", readme_content, flags=re.DOTALL
    )
    is_up_to_date = readme_content == updated_content

    if check_only:
        if is_up_to_date:
            logger.info("✓ README.md tools section is up to date")
        else:
            logger.error("✗ README.md tools section is out of date")
        return is_up_to_date
    else:
        README_PATH.write_text(updated_content)
        status = "was already up to date" if is_up_to_date else "updated successfully"
        logger.info(f"README.md tools section {status}")
        return True


def update_d2_diagram(check_only: bool = False) -> tuple[bool, bool]:
    """
    Update the tools section in diagram.d2.

    Args:
        check_only: If True, only check if update is needed without writing.

    Returns:
        Tuple of (success, content_changed):
        - success: True if diagram is up to date (or was updated), False on error.
        - content_changed: True if diagram content differs from generated content.
    """
    if not DIAGRAM_PATH.exists():
        logger.error(f"diagram.d2 not found at {DIAGRAM_PATH}")
        return False, False

    diagram_content = DIAGRAM_PATH.read_text()
    new_tools_section = generate_diagram_tools_section()

    # Replace tools section (from "tools: Tools {" to closing "}")
    pattern = r"tools: Tools \{.*?\n\}"
    if not re.search(pattern, diagram_content, re.DOTALL):
        logger.error("Could not find 'tools: Tools {' section in diagram.d2")
        return False, False

    updated_content = re.sub(
        pattern, new_tools_section, diagram_content, flags=re.DOTALL
    )
    content_changed = diagram_content != updated_content

    if check_only:
        if not content_changed:
            logger.info("✓ diagram.d2 tools section is up to date")
        else:
            logger.error("✗ diagram.d2 tools section is out of date")
        return not content_changed, content_changed
    else:
        DIAGRAM_PATH.write_text(updated_content)
        status = "updated successfully" if content_changed else "was already up to date"
        logger.info(f"diagram.d2 tools section {status}")
        return True, content_changed


def run_d2_command(output_path: Path) -> bool:
    """Run d2 to compile diagram.d2 to PNG."""
    try:
        result = subprocess.run(
            ["d2", str(DIAGRAM_PATH), str(output_path), "--pad", "20"],
            capture_output=True,
            text=True,
        )
        return result.returncode == 0
    except FileNotFoundError:
        logger.error("d2 command not found. Install with: brew install d2")
        return False


def update_d2_png(check_only: bool = False) -> bool:
    """
    Update/validate d2.png from diagram.d2.

    Args:
        check_only: If True, only check if PNG matches current diagram.

    Returns:
        True if PNG is up to date (or was updated), False if check failed.
    """
    if check_only:
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            if not run_d2_command(tmp_path):
                return False
            is_up_to_date = (
                PNG_PATH.exists() and PNG_PATH.read_bytes() == tmp_path.read_bytes()
            )
            if is_up_to_date:
                logger.info("✓ d2.png is up to date")
            else:
                logger.error("✗ d2.png is out of date")
            return is_up_to_date
        finally:
            tmp_path.unlink(missing_ok=True)
    else:
        success = run_d2_command(PNG_PATH)
        if success:
            logger.info("d2.png updated")
        return success


def update_all(check_only: bool = False) -> bool:
    """
    Update README.md, diagram.d2, and d2.png.

    Args:
        check_only: If True, only check if updates are needed without writing.

    Returns:
        True if all docs are up to date (or were updated), False if updates needed.
    """
    readme_ok = update_readme(check_only)
    diagram_ok, diagram_changed = update_d2_diagram(check_only)

    # Only update/check PNG if diagram.d2 changed (new toolset added)
    png_ok = True
    if diagram_changed:
        png_ok = update_d2_png(check_only)

    all_ok = readme_ok and diagram_ok and png_ok
    if check_only and not all_ok:
        logger.error("\nRun: task docs:generate to update the documentation.")

    return all_ok


def main():
    parser = argparse.ArgumentParser(
        description="Generate or validate documentation from toolsets"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check if documentation is up to date without modifying it",
    )
    args = parser.parse_args()

    success = update_all(check_only=args.check)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
