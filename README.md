# GitDu

A vibe coding testbed project - an **ncdu-style activity explorer for Git repositories**.

## What is this?

This was an experimental project to create a terminal UI tool that helps you explore Git repository activity, similar to how `ncdu` helps you explore disk usage. Instead of showing file sizes, GitDu shows Git activity metrics like commits, changes, and author activity.

## Features

- **Interactive Terminal UI**: Navigate through your repository structure with a tree-based interface
- **Git Activity Metrics**: View commits, changes, total modifications, and author counts per file/directory
- **Multiple Sort Modes**: Sort by commits, latest changes, total changes, or number of authors
- **Smart Caching**: JSONL-based incremental cache system with resume capability
- **Lazy Loading Mode**: Handle huge repositories (5000+ files) with on-demand loading
- **Resume Capability**: Interrupt and resume long repository scans
- **Glob Pattern Support**: Filter files using custom glob patterns

## Architecture

The project consists of several key components:

- `gitdu.py` - Main entry point and CLI argument handling
- `tui.py` - Textual-based terminal user interface
- `lazy_tui.py` - Lazy-loading UI for huge repositories
- `cache.py` - JSONL-based cache system with interruption handling
- `git_operations.py` - Git repository operations and statistics
- `stats_aggregator.py` - File and directory statistics aggregation
- `monitor_cache.py` - Cache monitoring utilities

## Usage

```bash
# Basic usage - analyze current directory
python gitdu.py

# Analyze specific repository
python gitdu.py /path/to/repo

# Force refresh cache
python gitdu.py --refresh

# Resume interrupted scan
python gitdu.py --resume

# Use lazy loading for huge repos
python gitdu.py --lazy

# Custom file patterns
python gitdu.py --glob "*.py"
```

## Dependencies

- **GitPython** - Git repository operations
- **textual** - Terminal UI framework
- **tinydb** - Database operations
- **psutil** - System monitoring

## Key Features Implemented

### Smart Repository Analysis
- Auto-detects repository size and suggests lazy mode for large repos
- Handles interrupted scans gracefully with resume capability
- Incremental cache updates for changed files only

### Flexible UI Modes
- **Normal Mode**: Full repository analysis with complete caching
- **Lazy Mode**: On-demand loading for repositories with 5000+ files
- **Resume Mode**: Continue from where you left off after interruption

### Advanced Caching
- JSONL format for monitoring and debugging
- Atomic operations to prevent corruption
- Progress tracking and resumption
- Stale entry detection and selective refresh

## Development Notes

This was a "vibe coding" project - experimental development focused on:
- Exploring terminal UI frameworks (Textual)
- Implementing efficient Git repository analysis
- Building robust caching systems
- Handling large-scale repository data
- Creating intuitive navigation interfaces

The codebase demonstrates patterns for:
- Signal handling and graceful interruption
- Progress tracking for long operations
- Lazy loading and memory management
- File system monitoring and caching strategies