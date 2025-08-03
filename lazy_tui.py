"""Lazy-loading Text UI module for gitdu."""

import os
from pathlib import Path
from lazy_cache import LazyCache
from textual.app import App, ComposeResult
from textual.widgets import Tree
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


class LazyGitDuTUI(App):
    """TUI with lazy loading for huge repositories."""
    
    def __init__(self, repo_path=".", max_display_items=200):
        super().__init__()
        self.cache = LazyCache(repo_path)
        self.max_display_items = max_display_items
        
    def compose(self) -> ComposeResult:
        tree_widget = Tree("Repo (lazy loaded)")
        self._build_lazy_tree(tree_widget)
        # Expand the root node to show children
        tree_widget.root.expand()
        yield tree_widget
    
    def _build_lazy_tree(self, tree_widget):
        """Build tree with lazy loading."""
        # Get root level items and add them directly
        root_items = self.cache.get_directory_files("", limit=self.max_display_items)
        
        for child in root_items:
            is_dir = self._is_directory(child)
            name = os.path.basename(child) or child
            
            if is_dir:
                # For directories, show simple info
                label = f"{name}/"
            else:
                # For files, get actual stats
                try:
                    stats = self.cache.get_stats_for_display(child, is_dir=False)
                    label = self._format_file_label(name, stats)
                except Exception as e:
                    # Fallback if stats fail
                    label = name
            
            node = tree_widget.root.add(label)
            
            # Add children for directories (limited depth)
            if is_dir:
                self._add_directory_children(child, node, depth=1)
    
    def _add_directory_children(self, dir_path, parent_node, depth=0):
        """Add children for a directory node."""
        if depth > 2:  # Limit depth
            return
            
        try:
            children = self.cache.get_directory_files(dir_path, limit=20)
            children.sort()
            
            for child in children:
                is_dir = self._is_directory(child)
                name = os.path.basename(child) or child
                
                if is_dir:
                    label = f"{name}/"
                    node = parent_node.add(label)
                    # Recursively add children for directories
                    if depth < 2:
                        self._add_directory_children(child, node, depth + 1)
                else:
                    try:
                        stats = self.cache.get_stats_for_display(child, is_dir=False)
                        label = self._format_file_label(name, stats)
                    except Exception:
                        label = name
                    parent_node.add(label)
        except Exception as e:
            # If we can't get children, add a placeholder
            parent_node.add(f"[Error loading: {e}]")
    
    def _is_directory(self, path):
        """Check if a path represents a directory."""
        return self.cache.is_directory(path)
    
    def _format_file_label(self, name, stats):
        """Format file label with stats info."""
        commits = stats.get("commits", 0)
        authors = stats.get("authors", [])
        latest_author = stats.get("latest_author", "")
        insertions = stats.get("insertions", 0)
        deletions = stats.get("deletions", 0)
        last_commit = stats.get("last_commit", "")
        last_commit_hash = stats.get("last_commit_hash", "")
        last_commit_message = stats.get("last_commit_message", "")
        
        if commits == 0:
            return name
        
        parts = []
        
        # Simplified display for lazy loading
        # Column 1: Commit count
        if commits > 0:
            if commits >= 20:
                commit_color = "[bold red]"
            elif commits >= 5:
                commit_color = "[bold green]"
            else:
                commit_color = "[dim green]"
            commit_part = f"{commit_color}{commits:>3}[/]"
        else:
            commit_part = "   "
        parts.append(commit_part)
        
        # Column 2: Latest author
        if latest_author:
            author_name = latest_author.split('@')[0] if '@' in latest_author else latest_author
            if len(author_name) > 8:
                author_name = author_name[:7] + "…"
            author_part = f"[cyan]{author_name:>8}[/]"
        else:
            author_part = "        "
        parts.append(author_part)
        
        # Column 3: Commit info with arrow
        if last_commit_hash and last_commit_message:
            short_hash = last_commit_hash[:8]
            if len(last_commit_message) > 30:
                short_message = last_commit_message[:27] + "..."
            else:
                short_message = last_commit_message
            commit_info = f"[dim blue]→[/] [cyan]{short_hash}[/] [dim]{short_message}[/]"
        else:
            commit_info = ""
        parts.append(commit_info)
        
        formatted_info = " ".join(parts)
        return f"{name:<25} {formatted_info}"