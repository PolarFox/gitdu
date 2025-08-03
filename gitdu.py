#!/usr/bin/env python3
"""
GitDu: ncdu-style activity explorer for Git repos

Main entry point for the gitdu application.
"""

import os
import argparse
from cache import GitDuCache, CACHE_DB
from tui import GitDuTUI
from lazy_tui import LazyGitDuTUI
from git_operations import DEFAULT_GLOB_PATTERN


def main():
    parser = argparse.ArgumentParser(description="GitDu: ncdu-style activity explorer for Git repos")
    parser.add_argument("repo", nargs="?", default=".", help="Path to the git repository (default: .)")
    parser.add_argument("--refresh", action="store_true", help="Force refresh cache")
    parser.add_argument("--resume", action="store_true", help="Resume interrupted refresh (auto-detected if no new commits)")
    parser.add_argument("--lazy", action="store_true", help="Use lazy loading mode for huge repositories")
    parser.add_argument("--glob", default=DEFAULT_GLOB_PATTERN, help=f"Glob pattern for files (default: {DEFAULT_GLOB_PATTERN})")
    args = parser.parse_args()

    # Auto-detect huge repositories and suggest lazy mode
    if not args.lazy:
        try:
            print("Analyzing repository size...", end="", flush=True)
            from git import Repo
            repo = Repo(args.repo)
            tracked_files = repo.git.ls_files().splitlines()
            print(f" found {len(tracked_files)} files.")
            if len(tracked_files) > 5000:
                print(f"Detected large repository with {len(tracked_files)} files.")
                print("Consider using --lazy flag for better performance on huge repositories.")
                print("Continuing with normal mode...")
        except Exception:
            print(" failed to analyze.")
            pass

    # Use lazy loading mode for huge repositories
    if args.lazy:
        print("Using lazy loading mode for huge repositories...")
        print("Press Ctrl+C to stop startup if needed...")
        try:
            app = LazyGitDuTUI(args.repo)
            app.run()
        except KeyboardInterrupt:
            print("\nApplication interrupted by user.")
        return

    cache = GitDuCache(args.repo)
    
    try:
        # Check for interrupted refresh first
        has_interrupted = cache.has_interrupted_refresh()
        
        if args.resume or has_interrupted:
            if has_interrupted:
                if not args.resume:
                    print("Detected interrupted refresh. Resuming automatically...")
                    print("(Use --resume flag to explicitly resume, or --refresh to start over)")
                print("Resuming repository scan from previous interruption...")
                print("Press Ctrl+C to interrupt - you can resume later by running the same command again.")
                print()  # Add spacing before progress bars
                cache.refresh(glob_pattern=args.glob, resume=True)
                print("\nRefresh completed.")
            else:
                print("No interrupted refresh found. Nothing to resume.")
                print("Use --refresh to start a new scan.")
        else:
            # Check if we need to refresh: either forced, file doesn't exist, or cache is empty
            cache_path = cache.cache_path
            need_full_refresh = args.refresh or not cache_path.exists()
            if not need_full_refresh:
                # Check if cache has data
                stats = cache.all()
                if not stats:
                    print("Cache file exists but is empty. Refreshing...")
                    need_full_refresh = True
            
            if need_full_refresh:
                print("Scanning repository. This may take a while...")
                print("Press Ctrl+C to interrupt - you can resume later by running the same command again.")
                print()  # Add spacing before progress bars
                cache.refresh(glob_pattern=args.glob)
                print("\nDone.")
            else:
                print("Using cached data.")
                # Check for stale entries and update only what's needed
                print("Press Ctrl+C to interrupt - you can resume later by running the same command again.")
                print()  # Add spacing before progress bars
                updated = cache.check_and_refresh_stale(glob_pattern=args.glob)
                if updated:
                    print("\nCache updated.")
                else:
                    print("\nCache is up to date.")
    except KeyboardInterrupt:
        print("\n\nScanning interrupted. Progress has been saved.")
        print("Run the same command again to resume from where you left off.")
        cache.close()
        return

    app = GitDuTUI(args.repo)
    app.run()
    
    # Close the cache properly
    cache.close()


if __name__ == "__main__":
    main()