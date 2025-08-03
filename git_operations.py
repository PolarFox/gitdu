"""Git operations module for gitdu."""

from fnmatch import fnmatch
from pathlib import Path

# Configuration constants
DEFAULT_GLOB_PATTERN = "**/*"
MAX_COMMIT_COUNT = 1


class GitOperationError(Exception):
    """Exception raised for Git operation errors."""
    pass


class GitOperations:
    """Handles all Git-related operations."""
    
    def __init__(self, repo):
        self.repo = repo
    
    def get_latest_commit_hash(self, path):
        try:
            commits = list(self.repo.iter_commits(paths=path, max_count=MAX_COMMIT_COUNT))
            if commits:
                return commits[0].hexsha
        except Exception:
            return None
        return None
    
    def get_latest_commit_info(self, path):
        """Get latest commit info (hash, message) for a file."""
        try:
            commits = list(self.repo.iter_commits(paths=path, max_count=MAX_COMMIT_COUNT))
            if commits:
                commit = commits[0]
                return {
                    "hash": commit.hexsha[:8],
                    "message": commit.message.strip().split('\n')[0][:50]
                }
        except Exception:
            return None
        return None
    
    def get_current_head(self):
        """Get current repository HEAD commit hash."""
        try:
            return self.repo.head.commit.hexsha
        except Exception:
            return None
    
    def get_changed_files(self, cached_head, current_head):
        """Get list of files changed between two commits."""
        try:
            if current_head:
                return self.repo.git.diff('--name-only', cached_head, current_head).splitlines()
            else:
                return self.get_tracked_files()
        except Exception as e:
            raise GitOperationError(f"Could not get diff: {e}")
    
    def get_tracked_files(self, glob_pattern=DEFAULT_GLOB_PATTERN):
        """Get all tracked files matching the glob pattern."""
        all_files = self.repo.git.ls_files().splitlines()
        if glob_pattern == DEFAULT_GLOB_PATTERN:
            return all_files
        
        return [f for f in all_files if _matches_glob_pattern(f, glob_pattern)]


def _matches_glob_pattern(filepath, pattern):
    """
    Improved glob pattern matching that properly handles ** patterns.
    """
    if pattern == "**/*":
        return True
    
    # Try pathlib first (good for most patterns)
    if Path(filepath).match(pattern):
        return True
    
    # Handle ** patterns that should match root-level files
    if "**/" in pattern:
        # For patterns like "**/*.py", also try matching without the **/
        root_pattern = pattern.replace("**/", "")
        if Path(filepath).match(root_pattern):
            return True
    
    # Handle patterns that start with ** (like "**suffix")
    if pattern.startswith("**"):
        suffix_pattern = pattern[2:]
        if Path(filepath).match(suffix_pattern) or Path(filepath).match("*" + suffix_pattern):
            return True
    
    return False


def get_latest_commit_hash(repo, path):
    """Legacy function - use GitOperations.get_latest_commit_hash instead."""
    git_ops = GitOperations(repo)
    return git_ops.get_latest_commit_hash(path)


def get_repo_files(repo, glob_pattern=DEFAULT_GLOB_PATTERN):
    """Legacy function - use GitOperations.get_tracked_files instead."""
    git_ops = GitOperations(repo)
    return git_ops.get_tracked_files(glob_pattern)