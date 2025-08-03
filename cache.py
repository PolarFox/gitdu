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
def get_cache_filename(repo_path):
    """Generate a unique cache filename based on repository path."""
    repo_name = Path(repo_path).name
    return f"gitdu_cache_{repo_name}.jsonl"

CACHE_FILE = "gitdu_cache.jsonl"  # Default fallback
CACHE_DB = CACHE_FILE  # For compatibility with existing code
REPO_HEAD_MARKER = "__repo_head__"
REFRESH_PROGRESS_MARKER = "__refresh_progress__"
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
    
    def __init__(self, repo_path, cache_file=None):
        global _cache_instance
        self.repo_path = Path(repo_path).resolve()
        # Generate unique cache filename if not provided
        if cache_file is None:
            cache_file = get_cache_filename(self.repo_path)
        # Store cache in the target repository directory
        self.cache_file = self.repo_path / cache_file
        self.cache_path = Path(self.cache_file)
        
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
    
    def _remove_entry(self, path):
        """Remove an entry from the cache."""
        if path in self._data:
            del self._data[path]
            # Note: Entry remains in JSONL file until next compaction
    
    def all(self):
        """Get all cached entries."""
        return list(self._data.values())
    
    def has_interrupted_refresh(self):
        """Check if there's an interrupted refresh that can be resumed."""
        progress_state = self.get(REFRESH_PROGRESS_MARKER)
        if not progress_state:
            return False
            
        # Check if repository has changed since the interrupted refresh
        current_head = self.git_ops.get_current_head()
        cached_head_entry = self.get(REPO_HEAD_MARKER)
        cached_head = cached_head_entry.get("last_commit_hash") if cached_head_entry else None
        
        # If repository has changed, the interrupted refresh is no longer valid
        if cached_head and current_head != cached_head:
            return False
            
        return True
    
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
    
    def refresh(self, glob_pattern=DEFAULT_GLOB_PATTERN, resume=False):
        """Full refresh with progress tracking and optional resume capability."""
        global _in_critical_section
        files = self.git_ops.get_tracked_files(glob_pattern)
        
        # Check for existing progress if resuming
        progress_state = None
        if resume:
            progress_state = self.get(REFRESH_PROGRESS_MARKER)
            if progress_state:
                # Check if repository has changed since the interrupted refresh
                current_head = self.git_ops.get_current_head()
                cached_head_entry = self.get(REPO_HEAD_MARKER)
                cached_head = cached_head_entry.get("last_commit_hash") if cached_head_entry else None
                
                if cached_head and current_head != cached_head:
                    print(f"Repository has new commits since interrupt ({cached_head[:8]}...{current_head[:8] if current_head else 'None'})")
                    print("Invalidating old progress and starting fresh refresh...")
                    self._remove_entry(REFRESH_PROGRESS_MARKER)
                    progress_state = None
                else:
                    print(f"Resuming refresh from previous session...")
                    print(f"Progress: {progress_state.get('phase', 'unknown')} phase")
        
        # Determine which files still need processing
        files_to_process = self._check_files_need_processing(files)
        
        if not resume or not progress_state:
            print(f"Need to process {len(files_to_process)} files (skipping {len(files) - len(files_to_process)} up-to-date files)")
        else:
            print(f"Continuing with {len(files_to_process)} remaining files")
        
        # Process files phase
        if files_to_process:
            try:
                # Save progress state at start of files phase 
                self.upsert({
                    "path": REFRESH_PROGRESS_MARKER,
                    "phase": "files",
                    "total_files": len(files),
                    "remaining_files": len(files_to_process),
                    "glob_pattern": glob_pattern,
                    "is_dir": False
                })
                
                self._process_files_with_progress(files_to_process, "Processing files...", handle_interrupts=True, glob_pattern=glob_pattern)
            except KeyboardInterrupt:
                print("\nSafely interrupted. Progress saved.")
                print("Run the same command again to resume from where you left off.")
                return
        
        # Process directories phase
        all_dirs = get_all_dirs(files)
        dirs_to_update = [d for d in sorted(all_dirs) if d != ""]
        
        if dirs_to_update:
            dirs_to_update.sort(key=lambda x: (len(Path(x).parts), x), reverse=True)
            try:
                # Save progress state at start of directories phase
                self.upsert({
                    "path": REFRESH_PROGRESS_MARKER,
                    "phase": "directories", 
                    "total_dirs": len(dirs_to_update),
                    "glob_pattern": glob_pattern,
                    "is_dir": False
                })
                
                self._process_directories_with_progress(dirs_to_update, "Processing directories...", handle_interrupts=True)
            except KeyboardInterrupt:
                print("\nSafely interrupted. Progress saved.")
                print("Run the same command again to resume from where you left off.")
                return
        
        # Mark refresh as complete
        current_head = self.git_ops.get_current_head()
        if current_head:
            self.upsert({"path": REPO_HEAD_MARKER, "last_commit_hash": current_head, "is_dir": False})
        
        # Clean up progress marker
        self._remove_entry(REFRESH_PROGRESS_MARKER)
        
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
        
        if len(files) > 0:
            print(f"Processing {len(files)} files in batch mode...")
            print("This will process git history - please wait...")
        
        try:
            if handle_interrupts:
                _in_critical_section = True
            
            # Show progress during git history processing
            all_stats = aggregate_all_file_stats_fast(self.repo, files, glob_pattern, show_progress=len(files) > 0)
            
            if len(files) > 0:
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