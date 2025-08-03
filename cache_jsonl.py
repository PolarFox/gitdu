"""Improved cache management with JSONL format for better monitoring."""

import os
import sys
import json
import signal
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
from git import Repo
from git.exc import InvalidGitRepositoryError, NoSuchPathError
from rich.progress import Progress, TaskID, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn

from git_operations import GitOperations, GitOperationError, DEFAULT_GLOB_PATTERN, _matches_glob_pattern
from stats_aggregator import aggregate_all_file_stats_fast, aggregate_file_stats, aggregate_dir_stats, get_all_dirs

# Configuration constants
CACHE_FILE = "gitdu_cache.jsonl"
CACHE_DB = CACHE_FILE  # For compatibility with existing code
REPO_HEAD_MARKER = "__repo_head__"
MAX_LINE_LENGTH = 2000

# Global variables for signal handling
_in_critical_section = False
_cache_instance = None


def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully without corrupting the cache."""
    global _in_critical_section, _cache_instance
    
    if _in_critical_section and _cache_instance:
        print("\n\nReceived interrupt. Finishing current operation to avoid corrupting cache...")
        return
    else:
        print("\n\nInterrupted. Cache has been saved.")
        if _cache_instance:
            _cache_instance.close()
        sys.exit(0)


# Set up signal handler
signal.signal(signal.SIGINT, signal_handler)


class JsonlCache:
    """JSONL-based cache that's easy to monitor and stream."""
    
    def __init__(self, repo_path, cache_file=CACHE_FILE):
        global _cache_instance
        self.repo_path = Path(repo_path).resolve()
        self.cache_file = cache_file
        self.cache_path = Path(cache_file)
        
        try:
            self.repo = Repo(str(self.repo_path))
        except InvalidGitRepositoryError:
            print(f"Error: '{self.repo_path}' is not a valid Git repository.")
            sys.exit(1)
        except NoSuchPathError:
            print(f"Error: The path '{self.repo_path}' does not exist.")
            sys.exit(1)
            
        self.git_ops = GitOperations(self.repo)
        self._data = {}
        self._load_cache()
        _cache_instance = self
    
    def _load_cache(self):
        """Load cache from JSONL file."""
        if not self.cache_path.exists():
            return
            
        try:
            with open(self.cache_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        if 'path' in entry:
                            self._data[entry['path']] = entry
                    except json.JSONDecodeError as e:
                        print(f"Warning: Invalid JSON on line {line_num}: {e}")
        except Exception as e:
            print(f"Warning: Could not load cache: {e}")
    
    def _save_entry(self, entry):
        """Append a single entry to the JSONL file."""
        # Add timestamp for monitoring
        entry['_updated'] = datetime.now().isoformat()
        
        with open(self.cache_file, 'a', encoding='utf-8') as f:
            json.dump(entry, f, separators=(',', ':'))
            f.write('\n')
        
        # Update in-memory cache
        if 'path' in entry:
            self._data[entry['path']] = entry
    
    def _compact_cache(self):
        """Compact the JSONL file by removing duplicates and keeping latest entries."""
        if not self.cache_path.exists():
            return
            
        # Create backup
        backup_path = self.cache_path.with_suffix('.bak')
        if backup_path.exists():
            backup_path.unlink()
        self.cache_path.rename(backup_path)
        
        # Write compacted version
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            for entry in self._data.values():
                json.dump(entry, f, separators=(',', ':'))
                f.write('\n')
        
        # Remove backup on success
        backup_path.unlink()
        print(f"Compacted cache: {len(self._data)} entries")
    
    def get(self, path):
        """Get cached entry for a path."""
        return self._data.get(path)
    
    def upsert(self, entry):
        """Insert or update an entry."""
        self._save_entry(entry)
    
    def all(self):
        """Get all cached entries."""
        return list(self._data.values())
    
    @contextmanager
    def monitor_mode(self):
        """Context manager for live monitoring mode."""
        print(f"Monitoring cache file: {self.cache_file}")
        print("New entries will appear here in real-time...")
        print("=" * 60)
        yield
    
    def check_and_refresh_stale(self, glob_pattern=DEFAULT_GLOB_PATTERN):
        """Check for stale cache entries and refresh only what's needed."""
        current_head = self.git_ops.get_current_head()
        cached_head_entry = self.get(REPO_HEAD_MARKER)
        cached_head = cached_head_entry.get("last_commit_hash") if cached_head_entry else None
        
        if cached_head == current_head:
            print("Repository unchanged since last scan.")
            return False
            
        if not cached_head:
            print("No previous scan found. Need full refresh.")
            return True
            
        print(f"Repository changed from {cached_head[:8]}...{current_head[:8] if current_head else 'None'}. Checking differences...")
        
        try:
            changed_file_list = self.git_ops.get_changed_files(cached_head, current_head)
        except GitOperationError as e:
            print(f"Could not get diff: {e}")
            print("Falling back to full refresh.")
            return True
            
        files = self.git_ops.get_tracked_files(glob_pattern)
        files_set = set(files)
        changed_files = [f for f in changed_file_list if f in files_set and _matches_glob_pattern(f, glob_pattern)]
        
        if not changed_files:
            print("No changes affecting tracked files.")
            self.upsert({"path": REPO_HEAD_MARKER, "last_commit_hash": current_head, "is_dir": False})
            return False
            
        print(f"Found {len(changed_files)} changed files. Updating...")
        
        self._process_files_with_progress(changed_files, "Updating files...", glob_pattern=glob_pattern)
        
        # Update affected directories
        affected_dirs = set()
        for f in changed_files:
            path_parts = Path(f).parts
            for i in range(1, len(path_parts)):
                dir_path = os.path.join(*path_parts[:i])
                affected_dirs.add(dir_path)
        
        if affected_dirs:
            sorted_dirs = sorted(affected_dirs, key=lambda x: (len(Path(x).parts), x), reverse=True)
            self._process_directories_with_progress(sorted_dirs, "Updating directories...")
        
        self.upsert({"path": REPO_HEAD_MARKER, "last_commit_hash": current_head, "is_dir": False})
        
        print(f"Updated {len(changed_files)} files and {len(affected_dirs)} directories.")
        return True
    
    def refresh(self, glob_pattern=DEFAULT_GLOB_PATTERN):
        """Full refresh with progress tracking."""
        global _in_critical_section
        files = self.git_ops.get_tracked_files(glob_pattern)
        
        files_to_process = self._check_files_need_processing(files)
        print(f"Need to process {len(files_to_process)} files (skipping {len(files) - len(files_to_process)} up-to-date files)")
        
        if files_to_process:
            try:
                self._process_files_with_progress(files_to_process, "Processing files...", handle_interrupts=True, glob_pattern=glob_pattern)
            except KeyboardInterrupt:
                print("\nSafely interrupted. Progress saved.")
                return
        
        all_dirs = get_all_dirs(files)
        dirs_to_update = [d for d in sorted(all_dirs) if d != ""]
        
        if dirs_to_update:
            dirs_to_update.sort(key=lambda x: (len(Path(x).parts), x), reverse=True)
            try:
                self._process_directories_with_progress(dirs_to_update, "Processing directories...", handle_interrupts=True)
            except KeyboardInterrupt:
                print("\nSafely interrupted. Progress saved.")
                return
        
        current_head = self.git_ops.get_current_head()
        if current_head:
            self.upsert({"path": REPO_HEAD_MARKER, "last_commit_hash": current_head, "is_dir": False})
        
        # Compact cache after full refresh
        self._compact_cache()
    
    def _create_progress_bar(self):
        """Create a standardized progress bar configuration."""
        return Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        )
    
    def _check_files_need_processing(self, files):
        """Check which files need processing."""
        files_to_process = []
        
        with self._create_progress_bar() as progress:
            task = progress.add_task("Checking existing cache...", total=len(files))
            
            for f in files:
                latest_hash = self.git_ops.get_latest_commit_hash(f)
                cached = self.get(f)
                if not cached or cached.get("last_commit_hash") != latest_hash:
                    files_to_process.append(f)
                progress.update(task, advance=1)
        
        return files_to_process
    
    def _process_files_with_progress(self, files, description, handle_interrupts=False, glob_pattern=DEFAULT_GLOB_PATTERN):
        """Process files with progress bar."""
        global _in_critical_section
        
        print(f"Processing {len(files)} files in batch mode...")
        
        try:
            if handle_interrupts:
                _in_critical_section = True
            
            all_stats = aggregate_all_file_stats_fast(self.repo, files, glob_pattern)
            
            with self._create_progress_bar() as progress:
                task = progress.add_task("Updating cache...", total=len(files))
                
                for f in files:
                    if f in all_stats:
                        self.upsert(all_stats[f])
                    progress.update(task, advance=1)
            
            if handle_interrupts:
                _in_critical_section = False
                
        except KeyboardInterrupt:
            if handle_interrupts:
                _in_critical_section = False
            raise
    
    def _process_directories_with_progress(self, directories, description, handle_interrupts=False):
        """Process directories with progress bar."""
        global _in_critical_section
        
        with self._create_progress_bar() as progress:
            task = progress.add_task(description, total=len(directories))
            
            for d in directories:
                try:
                    if handle_interrupts:
                        _in_critical_section = True
                    
                    # Create a temporary TinyDB-like interface for aggregate_dir_stats
                    class TempDB:
                        def __init__(self, data):
                            self._data = data
                        
                        def search(self, query):
                            # Simple path-based search for aggregate_dir_stats
                            results = []
                            for entry in self._data.values():
                                if query(entry):
                                    results.append(entry)
                            return results
                    
                    temp_db = TempDB(self._data)
                    agg = aggregate_dir_stats(temp_db, d)
                    self.upsert(agg)
                    
                    if handle_interrupts:
                        _in_critical_section = False
                except KeyboardInterrupt:
                    if handle_interrupts:
                        _in_critical_section = False
                    raise
                progress.update(task, advance=1)
    
    def close(self):
        """Close the cache (no-op for JSONL)."""
        pass


# Alias for compatibility
GitDuCache = JsonlCache