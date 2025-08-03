"""Text UI module for gitdu."""

import os
from pathlib import Path
from cache import JsonlCache
from textual.app import App, ComposeResult
from textual.widgets import Tree
from textual.binding import Binding

# Configuration constants
CACHE_DB = "gitdu_cache.jsonl"


class GitDuTUI(App):
    BINDINGS = [
        Binding("s", "cycle_sort", "Cycle Sort"),
        Binding("q", "quit", "Quit"),
    ]
    
    SORT_MODES = [
        ("commits", "Commits"),
        ("last_commit", "Latest Change"),
        ("changes", "Total Changes"),
        ("authors", "Authors"),
    ]
    
    def __init__(self, db_path=None):
        super().__init__()
        self.sort_mode_index = 1  # Default to "Latest Change"
        # Extract repo path from cache file path
        if isinstance(db_path, str) and db_path.endswith('.jsonl'):
            repo_path = os.path.dirname(db_path)
            cache_filename = os.path.basename(db_path)
            self.cache = JsonlCache(repo_path, cache_filename)
        else:
            # Use default cache with unique naming
            repo_path = db_path or "."
            self.cache = JsonlCache(repo_path)
        # Filter out special entries like repository head marker
        all_stats = self.cache.all()
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

    def action_cycle_sort(self):
        """Cycle through sort modes."""
        self.sort_mode_index = (self.sort_mode_index + 1) % len(self.SORT_MODES)
        self.refresh_tree()
    
    def get_sort_key(self, child_stats):
        """Get sort key based on current sort mode."""
        sort_mode, _ = self.SORT_MODES[self.sort_mode_index]
        
        if sort_mode == "commits":
            return child_stats.get("commits", 0)
        elif sort_mode == "last_commit":
            last_commit = child_stats.get("last_commit")
            return last_commit if last_commit is not None else ""
        elif sort_mode == "changes":
            return child_stats.get("insertions", 0) + child_stats.get("deletions", 0)
        elif sort_mode == "authors":
            return len(child_stats.get("authors", []))
        else:
            return 0
    
    def refresh_tree(self):
        """Refresh the tree with current sort mode."""
        tree_widget = self.query_one(Tree)
        tree_widget.clear()
        sort_mode, sort_name = self.SORT_MODES[self.sort_mode_index]
        tree_widget.label = f"Repo (sorted by {sort_name}) - Press 's' to change sort"
        self._populate_tree(tree_widget)
        # Force complete refresh of the widget
        self.refresh()

    def compose(self) -> ComposeResult:
        sort_mode, sort_name = self.SORT_MODES[self.sort_mode_index]
        tree_widget = Tree(f"Repo (sorted by {sort_name}) - Press 's' to change sort")
        self._populate_tree(tree_widget)
        yield tree_widget
    
    def _populate_tree(self, tree_widget):
        node_map = {"": tree_widget.root}
        path_tree = self.build_tree()

        def add_children(parent_path, parent_node):
            children = path_tree.get(parent_path, {}).get("children", [])
            if not children:
                return
                
            # Sort children based on current sort mode
            children_stats = []
            for child in children:
                child_stats = path_tree.get(child, {}).get("stats", {})
                sort_key = self.get_sort_key(child_stats)
                children_stats.append((child, sort_key))
            
            children_stats.sort(key=lambda x: x[1], reverse=True)
            
            for child, _ in children_stats:
                child_entry = path_tree.get(child, {})
                stat = child_entry.get("stats", {})
                
                name = os.path.basename(child) or child
                is_dir = stat.get("is_dir", len(child_entry.get("children", [])) > 0)  # Infer directory if has children
                
                label = f"{name}/" if is_dir else name
                commits = stat.get("commits", 0)
                authors = stat.get("authors", [])
                latest_author = stat.get("latest_author", "")
                insertions = stat.get("insertions", 0)
                deletions = stat.get("deletions", 0)
                first_commit = stat.get("first_commit", "")
                last_commit = stat.get("last_commit", "")
                
                if commits > 0 or len(authors) > 0:
                    # Create formatted columns with more generous spacing
                    parts = []
                    
                    # Column 1: Commit count with bar (width ~20 chars)
                    commit_bar_length = min(commits // 5 + 1, 12) if commits > 0 else 0
                    if commit_bar_length > 0:
                        commit_intensity = "█" * commit_bar_length
                        if commits >= 50:
                            commit_color = "[bold red]"
                        elif commits >= 20:
                            commit_color = "[bold yellow]"
                        elif commits >= 5:
                            commit_color = "[bold green]"
                        else:
                            commit_color = "[dim green]"
                        
                        # Format commit count with K/M for large numbers
                        if commits >= 1000000:
                            commit_display = f"{commits/1000000:.1f}M"
                        elif commits >= 1000:
                            commit_display = f"{commits/1000:.1f}K"
                        else:
                            commit_display = str(commits)
                        
                        commit_part = f"{commit_color}{commit_intensity:<12}[/] {commit_display:>6}"
                    else:
                        commit_part = f"{'':>20}"
                    parts.append(commit_part)
                    
                    # Column 2: Latest author + diversity (width ~25 chars)  
                    author_count = len(authors)
                    if latest_author:
                        # Extract username from email (take part before @)
                        author_name = latest_author.split('@')[0] if '@' in latest_author else latest_author
                        # Allow longer author names
                        if len(author_name) > 12:
                            author_name = author_name[:11] + "…"
                        
                        # Add diversity indicator
                        if author_count > 1:
                            diversity_indicator = f"+{author_count-1}" if author_count <= 4 else f"+{author_count-1}👥"
                            author_part = f"[cyan]{author_name}[/] [dim blue]{diversity_indicator}[/]"
                        else:
                            author_part = f"[cyan]{author_name}[/]"
                        author_part = f"{author_part:<25}"
                    elif author_count > 0:
                        # Fallback to just showing diversity
                        author_symbols = "👤" * min(author_count, 3)
                        if author_count > 3:
                            author_symbols += f"+{author_count-3}"
                        author_part = f"[blue]{author_symbols:<15}[/]"
                    else:
                        author_part = f"{'':>25}"
                    parts.append(author_part)
                    
                    # Column 3: Changes with visualization (width ~25 chars)
                    total_changes = insertions + deletions
                    if total_changes > 0:
                        if insertions > deletions:
                            change_symbol = "📈"  # Growing
                            change_color = "[green]"
                        elif deletions > insertions:
                            change_symbol = "📉"  # Shrinking
                            change_color = "[red]"
                        else:
                            change_symbol = "⚖️"   # Balanced
                            change_color = "[yellow]"
                        
                        # Size indicator
                        if total_changes >= 1000000:
                            size_indicator = "🌋"  # Massive
                        elif total_changes >= 100000:
                            size_indicator = "🔥"  # Very large
                        elif total_changes >= 10000:
                            size_indicator = "⭐"  # Large
                        elif total_changes >= 1000:
                            size_indicator = "💫"  # Medium
                        else:
                            size_indicator = "•"   # Small
                        
                        # Format numbers with K/M suffixes for readability
                        def format_number(n):
                            if n >= 1000000:
                                return f"{n/1000000:.1f}M"
                            elif n >= 1000:
                                return f"{n/1000:.1f}K"
                            else:
                                return str(n)
                        
                        ins_str = format_number(insertions)
                        del_str = format_number(deletions)
                        changes_text = f"+{ins_str}/-{del_str}"
                        change_part = f"{change_color}{change_symbol}{size_indicator}[/] {changes_text:>15}"
                    else:
                        change_part = f"{'':>25}"
                    parts.append(change_part)
                    
                    # Column 4: Date with recency indicator (width ~15 chars)
                    if last_commit:
                        from datetime import datetime, date
                        try:
                            last_date = datetime.fromisoformat(last_commit).date()
                            today = date.today()
                            days_ago = (today - last_date).days
                            
                            # Format as "[number]d ago" with emoji
                            if days_ago == 0:
                                date_str = "today"
                                recency_symbol = "🔥"  # Hot
                                recency_color = "[bold red]"
                            elif days_ago == 1:
                                date_str = "1d ago"
                                recency_symbol = "🔥"  # Hot
                                recency_color = "[bold red]"
                            elif days_ago <= 7:
                                date_str = f"{days_ago}d ago"
                                recency_symbol = "🔥"  # Hot
                                recency_color = "[bold red]"
                            elif days_ago <= 30:
                                date_str = f"{days_ago}d ago"
                                recency_symbol = "🌟"  # Recent
                                recency_color = "[yellow]"
                            elif days_ago <= 90:
                                date_str = f"{days_ago}d ago"
                                recency_symbol = "📅"  # Moderate
                                recency_color = "[blue]"
                            else:
                                date_str = f"{days_ago}d ago"
                                recency_symbol = "🗂️"   # Old
                                recency_color = "[dim]"
                            
                            recency_part = f"{recency_color}{recency_symbol}[/] {date_str:>8}"
                        except:
                            # Fallback to just showing shortened date
                            recency_part = f"[dim]{last_commit[:10]}[/]"
                    else:
                        recency_part = f"{'':>15}"
                    parts.append(recency_part)
                    
                    # Column 5: Latest commit info for files only (not directories)
                    if not is_dir:
                        last_commit_hash = stat.get("last_commit_hash", "")
                        last_commit_message = stat.get("last_commit_message", "")
                        
                        if last_commit_hash and last_commit_message:
                            short_hash = last_commit_hash[:8]
                            # Allow longer commit messages with more space
                            if len(last_commit_message) > 60:
                                short_message = last_commit_message[:57] + "..."
                            else:
                                short_message = last_commit_message
                            commit_info = f"[dim blue]→[/] [cyan]{short_hash}[/] [dim]{short_message}[/]"
                        else:
                            commit_info = ""
                        parts.append(commit_info)
                    
                    # Join all parts with more generous spacing
                    formatted_info = "  ".join(parts)
                    label = f"{label:<40} {formatted_info}"
                
                node = parent_node.add(label)
                node_map[child] = node
                
                # Recurse into subdirectories
                if is_dir:
                    add_children(child, node)

        add_children("", tree_widget.root)