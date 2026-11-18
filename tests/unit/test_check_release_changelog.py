import importlib.util
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

# Load check_release_changelog dynamically since scripts/ is not an installed package
_script_path = Path(__file__).resolve().parents[2] / "scripts" / "check_release_changelog.py"
_spec = importlib.util.spec_from_file_location("check_release_changelog", _script_path)
assert _spec is not None and _spec.loader is not None
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)

get_new_unreleased_entries_on_main = _module.get_new_unreleased_entries_on_main
get_unreleased_entries_on_branch = _module.get_unreleased_entries_on_branch
main = _module.main


@pytest.fixture
def git_repo(tmp_path: Path):
    """Create a temporary git repository with main and release branches."""

    def run_git(*args: str) -> str:
        return subprocess.check_output(
            ["git", *args],
            cwd=tmp_path,
            text=True,
        ).strip()

    run_git("init", "-b", "main")
    run_git("config", "user.name", "Test User")
    run_git("config", "user.email", "test@example.com")

    # Initial commit on main with one unreleased change
    unreleased_dir = tmp_path / ".changes" / "unreleased"
    unreleased_dir.mkdir(parents=True)
    (unreleased_dir / ".gitkeep").touch()
    (unreleased_dir / "feature1.yaml").write_text("kind: Feature\n")
    run_git("add", ".")
    run_git("commit", "-m", "Initial commit with feature1")

    # Cut release branch and batch changes
    run_git("checkout", "-b", "release/1.0.0")
    (unreleased_dir / "feature1.yaml").unlink()
    (tmp_path / ".changes" / "v1.0.0.md").write_text("## v1.0.0\n* Feature 1\n")
    run_git("add", ".")
    run_git("commit", "-m", "version: 1.0.0")

    return tmp_path, run_git


def test_clean_release_state_has_no_unreleased_entries(git_repo):
    repo_path, run_git = git_repo
    pr_head = run_git("rev-parse", "HEAD")

    # Release branch is in sync with main
    new_entries = get_new_unreleased_entries_on_main(
        pr_head,
        base_branch="main",
        repo_dir=repo_path,
    )
    assert new_entries == []

    unreleased_on_branch = get_unreleased_entries_on_branch(repo_path / ".changes")
    assert unreleased_on_branch == []


def test_detects_new_unreleased_entry_merged_to_main(git_repo):
    repo_path, run_git = git_repo
    pr_head = run_git("rev-parse", "HEAD")

    # Add a new feature on main while the release PR is open
    run_git("checkout", "main")
    unreleased_dir = repo_path / ".changes" / "unreleased"
    (unreleased_dir / "feature2.yaml").write_text("kind: Feature 2\n")
    run_git("add", ".")
    run_git("commit", "-m", "Add feature2 to main")

    # Switch back to release branch
    run_git("checkout", "release/1.0.0")

    new_entries = get_new_unreleased_entries_on_main(
        pr_head,
        base_branch="main",
        repo_dir=repo_path,
    )
    assert ".changes/unreleased/feature2.yaml" in new_entries


def test_ignores_gitkeep_and_deleted_files(git_repo):
    repo_path, run_git = git_repo
    pr_head = run_git("rev-parse", "HEAD")

    # Advance main with unrelated changes and touch .gitkeep
    run_git("checkout", "main")
    (repo_path / "README.md").write_text("# Updated README\n")
    (repo_path / ".changes" / "unreleased" / ".gitkeep").write_text("# keep\n")
    run_git("add", ".")
    run_git("commit", "-m", "Update README and .gitkeep")

    run_git("checkout", "release/1.0.0")

    new_entries = get_new_unreleased_entries_on_main(
        pr_head,
        base_branch="main",
        repo_dir=repo_path,
    )
    assert new_entries == []


def test_detects_unbatched_entries_on_release_branch(tmp_path: Path):
    changes_dir = tmp_path / ".changes"
    unreleased_dir = changes_dir / "unreleased"
    unreleased_dir.mkdir(parents=True)
    (unreleased_dir / ".gitkeep").touch()

    assert get_unreleased_entries_on_branch(changes_dir) == []

    (unreleased_dir / "stray.yaml").write_text("kind: Bug\n")
    assert get_unreleased_entries_on_branch(changes_dir) == ["stray.yaml"]


def test_main_success(git_repo):
    repo_path, run_git = git_repo
    pr_head = run_git("rev-parse", "HEAD")

    test_args = [
        "check_release_changelog.py",
        "--pr-head",
        pr_head,
        "--base-branch",
        "main",
        "--repo-dir",
        str(repo_path),
    ]

    with patch("sys.argv", test_args):
        assert main() == 0


def test_main_failure_when_main_has_new_unreleased(git_repo, capsys):
    repo_path, run_git = git_repo
    pr_head = run_git("rev-parse", "HEAD")

    # Advance main with new unreleased entry
    run_git("checkout", "main")
    unreleased_dir = repo_path / ".changes" / "unreleased"
    (unreleased_dir / "feature2.yaml").write_text("kind: Feature 2\n")
    run_git("add", ".")
    run_git("commit", "-m", "Add feature2 to main")

    run_git("checkout", "release/1.0.0")
    test_args = [
        "check_release_changelog.py",
        "--pr-head",
        pr_head,
        "--base-branch",
        "main",
        "--repo-dir",
        str(repo_path),
    ]

    with patch("sys.argv", test_args):
        assert main() == 1

    captured = capsys.readouterr()
    assert "Release PR changelog parity check failed" in captured.out
    assert "feature2.yaml" in captured.out
