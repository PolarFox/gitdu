#!/usr/bin/env python3
"""Monitor gitdu cache file in real-time."""

import json
import time
import sys
from pathlib import Path
from datetime import datetime


def monitor_jsonl_cache(cache_file="gitdu_cache.jsonl"):
    """Monitor JSONL cache file for changes."""
    cache_path = Path(cache_file)
    
    if not cache_path.exists():
        print(f"Cache file {cache_file} does not exist yet. Waiting...")
        while not cache_path.exists():
            time.sleep(1)
    
    print(f"Monitoring {cache_file}")
    print("=" * 60)
    
    # Start from end of file
    with open(cache_path, 'r') as f:
        f.seek(0, 2)  # Go to end
        
        while True:
            line = f.readline()
            if line:
                try:
                    entry = json.loads(line.strip())
                    timestamp = entry.get('_updated', 'unknown')
                    path = entry.get('path', 'unknown')
                    
                    if path == "__repo_head__":
                        commit_hash = entry.get('last_commit_hash', '')[:8]
                        print(f"[{timestamp}] HEAD updated: {commit_hash}")
                    elif entry.get('is_dir'):
                        commits = entry.get('commits', 0)
                        insertions = entry.get('insertions', 0)
                        deletions = entry.get('deletions', 0)
                        print(f"[{timestamp}] DIR  {path:50} {commits:4}c {insertions:6}+ {deletions:6}-")
                    else:
                        commits = entry.get('commits', 0)
                        insertions = entry.get('insertions', 0)
                        deletions = entry.get('deletions', 0)
                        authors = entry.get('authors', [])
                        print(f"[{timestamp}] FILE {path:50} {commits:4}c {insertions:6}+ {deletions:6}- {len(authors)}a")
                        
                except json.JSONDecodeError:
                    print(f"[{datetime.now().isoformat()}] Invalid JSON: {line.strip()}")
            else:
                time.sleep(0.1)


def analyze_cache(cache_file="gitdu_cache.jsonl"):
    """Analyze the current cache contents."""
    cache_path = Path(cache_file)
    
    if not cache_path.exists():
        print(f"Cache file {cache_file} does not exist.")
        return
    
    files = 0
    dirs = 0
    total_commits = 0
    total_insertions = 0
    total_deletions = 0
    authors = set()
    
    print(f"Analyzing {cache_file}")
    print("=" * 60)
    
    with open(cache_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            try:
                entry = json.loads(line)
                path = entry.get('path', '')
                
                if path == "__repo_head__":
                    continue
                    
                if entry.get('is_dir'):
                    dirs += 1
                else:
                    files += 1
                
                total_commits += entry.get('commits', 0)
                total_insertions += entry.get('insertions', 0)
                total_deletions += entry.get('deletions', 0)
                authors.update(entry.get('authors', []))
                
            except json.JSONDecodeError:
                continue
    
    print(f"Files: {files}")
    print(f"Directories: {dirs}")
    print(f"Total commits: {total_commits}")
    print(f"Total insertions: {total_insertions}")
    print(f"Total deletions: {total_deletions}")
    print(f"Unique authors: {len(authors)}")
    print(f"Authors: {', '.join(sorted(authors))}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1]
        cache_file = sys.argv[2] if len(sys.argv) > 2 else "gitdu_cache.jsonl"
        
        if command == "monitor":
            try:
                monitor_jsonl_cache(cache_file)
            except KeyboardInterrupt:
                print("\nMonitoring stopped.")
        elif command == "analyze":
            analyze_cache(cache_file)
        else:
            print("Usage: python monitor_cache.py [monitor|analyze] [cache_file]")
    else:
        print("Usage: python monitor_cache.py [monitor|analyze] [cache_file]")
        print("  monitor: Watch cache file for real-time updates")
        print("  analyze: Show summary statistics of cache contents")