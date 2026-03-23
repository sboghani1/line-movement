"""Shared git utilities for pushing CSV changes to main."""

import csv
import os
import shutil
import subprocess
import tempfile
import uuid
from typing import List, Optional


def git_push_csv(
    csv_path: str,
    commit_message: str,
    csv_content: Optional[List[List[str]]] = None,
) -> bool:
    """Commit and push a CSV file to main using a temporary git worktree.

    Uses a detached worktree on origin/main so no other local changes are
    staged or committed. Returns True on success, False on failure (warnings
    are printed; callers should treat failures as recoverable).

    Args:
        csv_path:       Relative path to the CSV (e.g. "gh-pages/data/master_sheet.csv").
        commit_message: Git commit message.
        csv_content:    If provided, write these rows into the worktree.
                        If None, copy the file from local disk as-is.
    """
    worktree_path = None
    try:
        subprocess.run(
            ["git", "fetch", "origin", "main"],
            check=True, capture_output=True,
        )

        # git worktree add requires the target path to not exist yet
        worktree_path = os.path.join(tempfile.gettempdir(), f"csv_push_{uuid.uuid4().hex}")
        subprocess.run(
            ["git", "worktree", "add", "--detach", worktree_path, "origin/main"],
            check=True, capture_output=True,
        )

        wt_csv = os.path.join(worktree_path, csv_path)
        os.makedirs(os.path.dirname(wt_csv), exist_ok=True)

        if csv_content is not None:
            with open(wt_csv, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerows(csv_content)
        else:
            shutil.copy2(csv_path, wt_csv)

        # Stage first; diff --cached detects both new and modified files correctly
        subprocess.run(["git", "add", csv_path], check=True, cwd=worktree_path)
        diff = subprocess.run(
            ["git", "diff", "--cached", "--quiet", "--", csv_path],
            cwd=worktree_path, capture_output=True,
        )
        if diff.returncode == 0:
            print("  CSV already up to date on main — nothing to push")
            return True

        subprocess.run(
            ["git", "commit", "-m", commit_message],
            check=True, cwd=worktree_path,
        )
        subprocess.run(
            ["git", "push", "origin", "HEAD:main"],
            check=True, cwd=worktree_path,
        )
        row_count = (len(csv_content) - 1) if csv_content is not None else "?"
        print(f"  Pushed {csv_path} to main ({row_count} rows)")
        return True

    except subprocess.CalledProcessError as e:
        print(f"  Warning: git push failed: {e}")
        return False
    except Exception as e:
        print(f"  Warning: git push failed: {e}")
        return False
    finally:
        if worktree_path:
            subprocess.run(
                ["git", "worktree", "remove", "--force", worktree_path],
                capture_output=True,
            )
