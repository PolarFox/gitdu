"""Statistics aggregation module for gitdu."""

import os
import gc
import psutil
from pathlib import Path
from collections import defaultdict
from tinydb import Query
from rich.progress import Progress, TaskID, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn
from git_operations import GitOperations, _matches_glob_pattern, DEFAULT_GLOB_PATTERN

# Memory configuration for optimization
MAX_MEMORY_GB = 4.0
MEMORY_THRESHOLD = 0.8  # Use up to 80% of max memory before optimization


def get_memory_info():
    """Get current memory usage information."""
    process = psutil.Process()
    memory_info = process.memory_info()
    return {
        "rss_mb": memory_info.rss / 1024 / 1024,
        "vms_mb": memory_info.vms / 1024 / 1024,
        "percent": process.memory_percent(),
        "available_gb": psutil.virtual_memory().available / 1024 / 1024 / 1024
    }


def should_use_memory_optimization():
    """Determine if we have enough memory for aggressive caching."""
    mem_info = get_memory_info()
    return mem_info["available_gb"] >= 2.0  # Need at least 2GB available


def aggregate_all_file_stats_fast(repo, files, glob_pattern=DEFAULT_GLOB_PATTERN, show_progress=True):
    """Ultra-fast batch processing using git log --numstat for maximum speed."""
    import subprocess
    import time
    from collections import defaultdict
    
    if show_progress:
        print("Using ultra-fast git batch processing...")
        start_time = time.time()
    
    files_set = set(files)
    
    # Pre-allocate stats with optimized data structures
    all_stats = {}
    for path in files:
        all_stats[path] = {
            "path": path,
            "commits": 0,
            "insertions": 0,
            "deletions": 0,
            "authors": set(),
            "first_commit": None,
            "last_commit": None,
            "latest_author": None,
            "is_dir": False,
            "last_commit_hash": None,
            "last_commit_message": None,
        }
    
    try:
        # Use git log --numstat which is extremely fast and gives us all the data at once
        cmd = [
            'git', 'log', '--numstat', '--pretty=format:%H|%ae|%ad|%s',
            '--date=short', '--reverse'  # Process in chronological order
        ]
        
        if show_progress:
            print("Running git log --numstat (batch operation)...")
        
        # Run git command in the repository directory
        result = subprocess.run(
            cmd, 
            cwd=str(repo.working_dir),
            capture_output=True, 
            text=True, 
            check=True
        )
        
        if show_progress:
            git_time = time.time() - start_time
            print(f"Git log completed in {git_time:.2f}s, processing data...")
        
        # Parse the output efficiently
        lines = result.stdout.split('\n')
        current_commit = None
        commit_count = 0
        processed_files = 0
        
        if show_progress:
            progress = Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(complete_style="green", finished_style="green"),
                TextColumn("({task.completed} lines processed)"),
                TimeElapsedColumn(),
            )
            progress.start()
            task = progress.add_task("Processing git data...", total=len(lines))
        else:
            progress = None
            task = None
        
        for line_num, line in enumerate(lines):
            if not line.strip():
                continue
                
            if progress and task is not None and line_num % 1000 == 0:
                progress.update(task, completed=line_num)
            
            # Check if this is a commit header line
            if '|' in line and not line[0].isdigit() and '-' not in line[:10]:
                # Format: hash|author_email|date|subject
                parts = line.split('|', 3)
                if len(parts) >= 3:
                    current_commit = {
                        'hash': parts[0],
                        'email': parts[1],
                        'date': parts[2],
                        'message': parts[3] if len(parts) > 3 else ''
                    }
                    commit_count += 1
                continue
            
            # Check if this is a numstat line (insertions/deletions/filename)
            if current_commit and '\t' in line:
                parts = line.split('\t')
                if len(parts) >= 3:
                    try:
                        insertions = int(parts[0]) if parts[0] != '-' else 0
                        deletions = int(parts[1]) if parts[1] != '-' else 0
                        filepath = parts[2]
                        
                        # Check if this file is in our target set and matches glob pattern
                        if (filepath in files_set and 
                            (glob_pattern == DEFAULT_GLOB_PATTERN or _matches_glob_pattern(filepath, glob_pattern))):
                            
                            stats = all_stats[filepath]
                            stats["commits"] += 1
                            stats["insertions"] += insertions
                            stats["deletions"] += deletions
                            stats["authors"].add(current_commit['email'])
                            
                            # Update date range
                            commit_date = current_commit['date']
                            if stats["first_commit"] is None or commit_date < stats["first_commit"]:
                                stats["first_commit"] = commit_date
                            if stats["last_commit"] is None or commit_date > stats["last_commit"]:
                                stats["last_commit"] = commit_date
                                # Most recent commit info (chronological order)
                                stats["last_commit_hash"] = current_commit['hash']
                                stats["latest_author"] = current_commit['email'] 
                                stats["last_commit_message"] = current_commit['message'][:50]
                            
                            processed_files += 1
                    except ValueError:
                        # Skip lines where insertions/deletions can't be parsed as int
                        pass
        
        if progress:
            progress.update(task, completed=len(lines))
            progress.stop()
        
        if show_progress:
            total_time = time.time() - start_time
            avg_per_commit = (total_time * 1000) / max(commit_count, 1)
            print(f"Processed {commit_count} commits in {total_time:.2f}s ({avg_per_commit:.1f}ms per commit)")
                
    except subprocess.CalledProcessError as e:
        if show_progress:
            print(f"Git command failed ({e}), falling back to standard mode...")
        return aggregate_all_file_stats_standard(repo, files, glob_pattern, show_progress)
    except Exception as e:
        if show_progress:
            print(f"Fast processing failed ({e}), falling back to standard mode...")
        return aggregate_all_file_stats_standard(repo, files, glob_pattern, show_progress)
    
    # Convert authors sets to lists
    for stats in all_stats.values():
        stats["authors"] = list(stats["authors"])
    
    return all_stats


def aggregate_all_file_stats_standard(repo, files, glob_pattern=DEFAULT_GLOB_PATTERN, show_progress=True):
    """Fallback standard processing method with lower memory usage."""
    all_stats = {}
    files_set = set(files)
    
    for path in files:
        all_stats[path] = {
            "path": path,
            "commits": 0,
            "insertions": 0,
            "deletions": 0,
            "authors": set(),
            "first_commit": None,
            "last_commit": None,
            "latest_author": None,
            "is_dir": False,
            "last_commit_hash": None,
            "last_commit_message": None,
        }
    
    git_ops = GitOperations(repo)
    
    try:
        if show_progress:
            progress = Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(complete_style="green", finished_style="green"),
                TextColumn("({task.completed} commits processed)"),
                TimeElapsedColumn(),
            )
            progress.start()
            task = progress.add_task("Processing git history (standard mode)...", total=None)
        else:
            progress = None
            task = None
        
        commit_count = 0
        progress_update_interval = 500
        
        for commit in repo.iter_commits():
            commit_count += 1
            
            if progress and task is not None and commit_count % progress_update_interval == 0:
                progress.update(task, completed=commit_count)
            
            commit_date = commit.committed_datetime.date().isoformat()
            commit_email = commit.author.email
            commit_hash = commit.hexsha
            commit_message = commit.message.strip().split('\n')[0][:50]
            
            try:
                changed_files = commit.stats.files.keys()
                relevant_files = files_set.intersection(changed_files)
                
                if glob_pattern != DEFAULT_GLOB_PATTERN:
                    relevant_files = {f for f in relevant_files if _matches_glob_pattern(f, glob_pattern)}
                
                for filepath in relevant_files:
                    stats = all_stats[filepath]
                    stats["commits"] += 1
                    stats["authors"].add(commit_email)
                    
                    if stats["first_commit"] is None or commit_date < stats["first_commit"]:
                        stats["first_commit"] = commit_date
                    if stats["last_commit"] is None or commit_date > stats["last_commit"]:
                        stats["last_commit"] = commit_date
                    
                    file_stats = commit.stats.files.get(filepath)
                    if file_stats:
                        stats["insertions"] += file_stats["insertions"]
                        stats["deletions"] += file_stats["deletions"]
                    
                    if stats["last_commit_hash"] is None:
                        stats["last_commit_hash"] = commit_hash
                        stats["latest_author"] = commit_email
                        stats["last_commit_message"] = commit_message
                            
            except Exception:
                pass
                
        if progress and task is not None:
            progress.update(task, completed=commit_count)
            
        if progress:
            progress.stop()
                
    except Exception:
        if progress:
            progress.stop()
    
    for stats in all_stats.values():
        stats["authors"] = list(stats["authors"])
    
    return all_stats


def aggregate_file_stats(repo, path):
    """Legacy function - kept for compatibility. Use aggregate_all_file_stats_fast for better performance."""
    stats = {
        "path": path,
        "commits": 0,
        "insertions": 0,
        "deletions": 0,
        "authors": set(),
        "first_commit": None,
        "last_commit": None,
        "latest_author": None,
        "is_dir": False,
        "last_commit_hash": None,
    }
    try:
        first_commit = True
        for commit in repo.iter_commits(paths=path):
            stats["commits"] += 1
            stats["authors"].add(commit.author.email)
            date = commit.committed_datetime.date().isoformat()
            if not stats["first_commit"] or date < stats["first_commit"]:
                stats["first_commit"] = date
            if not stats["last_commit"] or date > stats["last_commit"]:
                stats["last_commit"] = date
            # Capture latest author (first commit in iteration is most recent)
            if first_commit:
                stats["latest_author"] = commit.author.email
                first_commit = False
            if hasattr(commit, "stats") and path in commit.stats.files:
                st = commit.stats.files[path]
                stats["insertions"] += st["insertions"]
                stats["deletions"] += st["deletions"]
    except Exception:
        pass
    stats["authors"] = list(stats["authors"])
    git_ops = GitOperations(repo)
    stats["last_commit_hash"] = git_ops.get_latest_commit_hash(path)
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
        "latest_author": None,
        "is_dir": True,
        "last_commit_hash": None,
    }
    hashes = []
    latest_commit_date = None
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
            # Track the latest author based on the most recent commit
            if not latest_commit_date or doc["last_commit"] > latest_commit_date:
                latest_commit_date = doc["last_commit"]
                agg["latest_author"] = doc.get("latest_author")
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