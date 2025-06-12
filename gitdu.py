# gitdu.py

import os
import sys
import signal
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from fnmatch import fnmatch

from tinydb import TinyDB, Query
from git import Repo
from git.exc import InvalidGitRepositoryError, NoSuchPathError
from textual.app import App, ComposeResult
from textual.widgets import Tree
from rich.progress import Progress, TaskID, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn

CACHE_DB = "gitdu_cache.json"

# Global variable to track if we're in a critical section
_in_critical_section = False
_db_instance = None

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully without corrupting the database."""
    global _in_critical_section, _db_instance
    
    if _in_critical_section and _db_instance:
        print("\n\nReceived interrupt. Finishing current operation to avoid corrupting cache...")
        # Let the current operation complete
        return
    else:
        print("\n\nInterrupted. Cache has been saved.")
        if _db_instance:
            _db_instance.close()
        sys.exit(0)

# Set up signal handler
signal.signal(signal.SIGINT, signal_handler)

def get_latest_commit_hash(repo, path):
    try:
        commits = list(repo.iter_commits(paths=path, max_count=1))
        if commits:
            return commits[0].hexsha
    except Exception:
        return None
    return None


def aggregate_file_stats(repo, path):
    stats = {
        "path": path,
        "commits": 0,
        "insertions": 0,
        "deletions": 0,
        "authors": set(),
        "first_commit": None,
        "last_commit": None,
        "is_dir": False,
        "last_commit_hash": None,
    }
    try:
        for commit in repo.iter_commits(paths=path):
            stats["commits"] += 1
            stats["authors"].add(commit.author.email)
            date = commit.committed_datetime.date().isoformat()
            if not stats["first_commit"] or date < stats["first_commit"]:
                stats["first_commit"] = date
            if not stats["last_commit"] or date > stats["last_commit"]:
                stats["last_commit"] = date
            if hasattr(commit, "stats") and path in commit.stats.files:
                st = commit.stats.files[path]
                stats["insertions"] += st["insertions"]
                stats["deletions"] += st["deletions"]
    except Exception:
        pass
    stats["authors"] = list(stats["authors"])
    stats["last_commit_hash"] = get_latest_commit_hash(repo, path)
    return stats


def aggregate_dir_stats(db, dir_path):
    """Aggregate all files/subdirs under dir_path in TinyDB."""
    q = Query()
    prefix = dir_path.rstrip(os.sep) + os.sep if dir_path else ""
    docs = db.search(q.path.matches(f'^{prefix}.*'))
    agg = {
        "path": dir_path,
        "commits": 0,
        "insertions": 0,
        "deletions": 0,
        "authors": set(),
        "first_commit": None,
        "last_commit": None,
        "is_dir": True,
        "last_commit_hash": None,
    }
    hashes = []
    for doc in docs:
        if doc["path"] == dir_path:  # skip self
            continue
        agg["commits"] += doc.get("commits", 0)
        agg["insertions"] += doc.get("insertions", 0)
        agg["deletions"] += doc.get("deletions", 0)
        agg["authors"].update(doc.get("authors", []))
        if doc.get("first_commit") and (
            not agg["first_commit"] or doc["first_commit"] < agg["first_commit"]
        ):
            agg["first_commit"] = doc["first_commit"]
        if doc.get("last_commit") and (
            not agg["last_commit"] or doc["last_commit"] > agg["last_commit"]
        ):
            agg["last_commit"] = doc["last_commit"]
        if doc.get("last_commit_hash"):
            hashes.append(doc["last_commit_hash"])
    agg["authors"] = list(agg["authors"])
    # Simple synthetic hash: just hash together the latest hashes (not cryptographically strong!)
    if hashes:
        agg["last_commit_hash"] = hashes[0] if len(hashes) == 1 else str(hash(tuple(sorted(hashes))))
    return agg


def get_all_dirs(paths):
    """Return all parent directories for a list of paths."""
    dirs = set()
    for p in paths:
        pth = Path(p)
        for i in range(1, len(pth.parts)):  # Start from 1 to avoid empty tuple
            dir_path = os.path.join(*pth.parts[:i])
            dirs.add(dir_path)
    return dirs


def get_repo_files(repo, glob_pattern="**/*"):
    files = [f for f in repo.git.ls_files().splitlines() if fnmatch(f, glob_pattern)]
    return files


class GitDuCache:
    def __init__(self, repo_path, db_path=CACHE_DB):
        global _db_instance
        self.repo_path = Path(repo_path).resolve()
        self.db_path = db_path
        try:
            self.repo = Repo(str(self.repo_path))
        except InvalidGitRepositoryError:
            print(f"Error: '{self.repo_path}' is not a valid Git repository. Please run this tool inside a Git repository or specify a valid path.")
            sys.exit(1)
        except NoSuchPathError:
            print(f"Error: The path '{self.repo_path}' does not exist. Please provide a valid path to a Git repository.")
            sys.exit(1)
        self.db = TinyDB(db_path)
        _db_instance = self.db  # Store global reference for signal handler

    def check_and_refresh_stale(self, glob_pattern="**/*"):
        """Check for stale cache entries and refresh only what's needed using efficient git diff operations."""
        q = Query()
        
        # Get current repository HEAD
        try:
            current_head = self.repo.head.commit.hexsha
        except Exception:
            # Repository might not have any commits yet
            current_head = None
            
        # Check if we have a cached HEAD commit
        cached_head_entry = self.db.get(q.path == "__repo_head__")
        cached_head = cached_head_entry.get("last_commit_hash") if cached_head_entry else None
        
        if cached_head == current_head:
            print("Repository unchanged since last scan.")
            return False
            
        if not cached_head:
            print("No previous scan found. Need full refresh.")
            return True  # Trigger full refresh
            
        print(f"Repository changed from {cached_head[:8]}...{current_head[:8] if current_head else 'None'}. Checking differences...")
        
        # Get list of changed files using git diff
        try:
            if current_head:
                # Get files changed between cached HEAD and current HEAD
                changed_file_list = self.repo.git.diff('--name-only', cached_head, current_head).splitlines()
            else:
                # Repository was reset or has no commits now
                changed_file_list = get_repo_files(self.repo, glob_pattern)
        except Exception as e:
            print(f"Could not get diff (probably due to history rewrite): {e}")
            print("Falling back to full refresh.")
            return True  # Trigger full refresh
            
        # Filter by glob pattern
        files = get_repo_files(self.repo, glob_pattern)
        files_set = set(files)
        changed_files = [f for f in changed_file_list if f in files_set and fnmatch(f, glob_pattern)]
        
        if not changed_files:
            print("No changes affecting tracked files.")
            # Update the HEAD marker
            self.db.upsert({"path": "__repo_head__", "last_commit_hash": current_head, "is_dir": False}, q.path == "__repo_head__")
            return False
            
        print(f"Found {len(changed_files)} changed files. Updating...")
        
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task("Updating files...", total=len(changed_files))
            for f in changed_files:
                stats = aggregate_file_stats(self.repo, f)
                self.db.upsert(stats, q.path == f)
                progress.update(task, advance=1)

        # Get all directories that need updating (parents of changed files only)
        affected_dirs = set()
        for f in changed_files:
            path_parts = Path(f).parts
            for i in range(1, len(path_parts)):
                dir_path = os.path.join(*path_parts[:i])
                affected_dirs.add(dir_path)

        # Update affected directories in reverse depth order
        if affected_dirs:
            sorted_dirs = sorted(affected_dirs, key=lambda x: (len(Path(x).parts), x), reverse=True)
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("({task.completed}/{task.total})"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
            ) as progress:
                task = progress.add_task("Updating directories...", total=len(sorted_dirs))
                for d in sorted_dirs:
                    agg = aggregate_dir_stats(self.db, d)
                    self.db.upsert(agg, q.path == d)
                    progress.update(task, advance=1)

        # Update the HEAD marker
        self.db.upsert({"path": "__repo_head__", "last_commit_hash": current_head, "is_dir": False}, q.path == "__repo_head__")
        
        print(f"Updated {len(changed_files)} files and {len(affected_dirs)} directories.")
        return True

    def refresh(self, glob_pattern="**/*"):
        """Full refresh with progress tracking and resumable scanning."""
        global _in_critical_section  # Declare at method level
        files = get_repo_files(self.repo, glob_pattern)
        q = Query()
        
        # Find files that need processing (resumable - skip files with current hash)
        files_to_process = []
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
        ) as progress:
            task = progress.add_task("Checking existing cache...", total=len(files))
            
            for f in files:
                latest_hash = get_latest_commit_hash(self.repo, f)
                cached = self.db.get(q.path == f)
                if not cached or cached.get("last_commit_hash") != latest_hash:
                    files_to_process.append(f)
                progress.update(task, advance=1)
        
        print(f"Need to process {len(files_to_process)} files (skipping {len(files) - len(files_to_process)} up-to-date files)")
        
        # Process files with progress bar
        if files_to_process:
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("({task.completed}/{task.total})"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
            ) as progress:
                task = progress.add_task("Processing files...", total=len(files_to_process))
                
                for f in files_to_process:
                    try:
                        _in_critical_section = True
                        stats = aggregate_file_stats(self.repo, f)
                        self.db.upsert(stats, q.path == f)
                        _in_critical_section = False
                    except KeyboardInterrupt:
                        _in_critical_section = False
                        print("\nSafely interrupted. Progress saved.")
                        return
                    progress.update(task, advance=1)
        
        # Update directories
        all_dirs = get_all_dirs(files)
        dirs_to_update = [d for d in sorted(all_dirs) if d != ""]
        
        if dirs_to_update:
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("({task.completed}/{task.total})"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
            ) as progress:
                task = progress.add_task("Processing directories...", total=len(dirs_to_update))
                
                # Sort by reverse depth (deepest first)
                dirs_to_update.sort(key=lambda x: (len(Path(x).parts), x), reverse=True)
                for d in dirs_to_update:
                    try:
                        _in_critical_section = True
                        agg = aggregate_dir_stats(self.db, d)
                        self.db.upsert(agg, q.path == d)
                        _in_critical_section = False
                    except KeyboardInterrupt:
                        _in_critical_section = False
                        print("\nSafely interrupted. Progress saved.")
                        return
                    progress.update(task, advance=1)

        # Store current HEAD commit hash for future incremental updates
        try:
            current_head = self.repo.head.commit.hexsha
            self.db.upsert({"path": "__repo_head__", "last_commit_hash": current_head, "is_dir": False}, q.path == "__repo_head__")
        except Exception:
            # Repository might not have any commits yet
            pass

    def all_stats(self):
        return self.db.all()
    
    def close(self):
        """Properly close the database."""
        if self.db:
            self.db.close()


class GitDuTUI(App):
    def __init__(self, db_path=CACHE_DB):
        super().__init__()
        self.db = TinyDB(db_path)
        # Filter out special entries like "__repo_head__"
        all_stats = self.db.all()
        self.stats = [s for s in all_stats if not s["path"].startswith("__")]
        self.path_stats = {s["path"]: s for s in self.stats}

    def build_tree(self):
        # Build a nested path tree for directory traversal
        tree = {}
        
        # First, initialize all paths and their parents
        for entry in self.stats:
            path = entry["path"]
            parts = Path(path).parts
            
            # Create all parent directory entries
            for i in range(len(parts)):
                if i == 0:
                    current_path = parts[0]
                    parent_path = ""
                else:
                    current_path = os.path.join(*parts[:i+1])
                    parent_path = os.path.join(*parts[:i])
                
                # Initialize tree entries
                tree.setdefault(parent_path, {"children": [], "stats": {}})
                tree.setdefault(current_path, {"children": [], "stats": {}})
                
                # Add child to parent if not already there
                if current_path not in tree[parent_path]["children"]:
                    tree[parent_path]["children"].append(current_path)
        
        # Now populate stats for entries we have in the database
        for entry in self.stats:
            path = entry["path"]
            if path in tree:
                tree[path]["stats"] = entry
        
        return tree

    def compose(self) -> ComposeResult:
        tree_widget = Tree("Repo (sorted by commits)")
        node_map = {"": tree_widget.root}
        path_tree = self.build_tree()

        def add_children(parent_path, parent_node):
            children = path_tree.get(parent_path, {}).get("children", [])
            if not children:
                return
                
            # Sort children by commit count descending, handling missing stats
            children_stats = []
            for child in children:
                child_stats = path_tree.get(child, {}).get("stats", {})
                commit_count = child_stats.get("commits", 0)
                children_stats.append((child, commit_count))
            
            children_stats.sort(key=lambda x: x[1], reverse=True)
            
            for child, _ in children_stats:
                child_entry = path_tree.get(child, {})
                stat = child_entry.get("stats", {})
                
                name = os.path.basename(child) or child
                is_dir = stat.get("is_dir", len(child_entry.get("children", [])) > 0)  # Infer directory if has children
                
                label = f"{name}/" if is_dir else name
                commits = stat.get("commits", 0)
                authors = stat.get("authors", [])
                if commits > 0 or len(authors) > 0:
                    label += f" [c={commits} a={len(authors)}]"
                
                node = parent_node.add(label)
                node_map[child] = node
                
                # Recurse into subdirectories
                if is_dir:
                    add_children(child, node)

        add_children("", tree_widget.root)
        yield tree_widget


def main():
    import argparse
    parser = argparse.ArgumentParser(description="GitDu: ncdu-style activity explorer for Git repos")
    parser.add_argument("repo", nargs="?", default=".", help="Path to the git repository (default: .)")
    parser.add_argument("--refresh", action="store_true", help="Force refresh cache")
    parser.add_argument("--glob", default="**/*", help="Glob pattern for files (default: **/*)")
    args = parser.parse_args()

    cache = GitDuCache(args.repo)
    
    try:
        # Check if we need to refresh: either forced, file doesn't exist, or cache is empty
        need_full_refresh = args.refresh or not os.path.exists(CACHE_DB)
        if not need_full_refresh:
            # Check if cache has data
            stats = cache.all_stats()
            if not stats:
                print("Cache file exists but is empty. Refreshing...")
                need_full_refresh = True
        
        try:
            if need_full_refresh:
                print("Scanning repository. This may take a while...")
                print("Press Ctrl+C to interrupt - you can resume later by running the same command again.")
                cache.refresh(glob_pattern=args.glob)
                print("Done.")
            else:
                print("Using cached data.")
                # Check for stale entries and update only what's needed
                print("Press Ctrl+C to interrupt - you can resume later by running the same command again.")
                updated = cache.check_and_refresh_stale(glob_pattern=args.glob)
                if updated:
                    print("Cache updated.")
                else:
                    print("Cache is up to date.")
        except KeyboardInterrupt:
            print("\n\nScanning interrupted. Progress has been saved.")
            print("Run the same command again to resume from where you left off.")
            cache.close()
            return

        app = GitDuTUI(CACHE_DB)
        app.run()
    finally:
        # Always close the cache properly
        cache.close()


if __name__ == "__main__":
    main()