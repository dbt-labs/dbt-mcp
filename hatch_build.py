"""Hatchling build hook to build the UI before packaging."""

import shutil
import subprocess
import sys
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface  # type: ignore[import-not-found]


class UIBuildHook(BuildHookInterface):
    """Build hook that builds the UI assets before packaging."""

    PLUGIN_NAME = "ui-build"

    def initialize(self, version: str, build_data: dict) -> None:
        """Build the UI assets before the package is built."""
        if self.target_name not in ("wheel", "sdist"):
            return

        root = Path(self.root)
        ui_dir = root / "ui"

        if not ui_dir.exists():
            self.app.display_warning(f"UI directory not found: {ui_dir}")
            return

        # Check if pnpm is available
        pnpm_cmd = shutil.which("pnpm")
        if not pnpm_cmd:
            self.app.display_error(
                "pnpm is not installed. Please install pnpm to build the UI. "
                "You can install it with: npm install -g pnpm"
            )
            sys.exit(1)

        self.app.display_info("Building UI assets...")

        try:
            # Run pnpm install
            self.app.display_info("Running pnpm install...")
            subprocess.run(
                [pnpm_cmd, "install"],
                cwd=ui_dir,
                check=True,
                capture_output=True,
                text=True,
            )

            # Run pnpm build
            self.app.display_info("Running pnpm build...")
            subprocess.run(
                [pnpm_cmd, "build"],
                cwd=ui_dir,
                check=True,
                capture_output=True,
                text=True,
            )

            self.app.display_success("UI assets built successfully")
        except subprocess.CalledProcessError as e:
            self.app.display_error(f"Failed to build UI: {e.stderr}")
            sys.exit(1)
        except FileNotFoundError as e:
            self.app.display_error(f"Command not found: {e}")
            sys.exit(1)
