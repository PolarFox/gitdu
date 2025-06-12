#!/usr/bin/env python3

from gitdu import GitDuTUI
from tinydb import TinyDB

# Create TUI instance
tui = GitDuTUI('gitdu_cache.json')

print(f"Loaded {len(tui.stats)} stats entries")
print("Sample stats:")
for i, stat in enumerate(tui.stats[:3]):
    print(f"  {i+1}. {stat}")

print("\nBuilding tree...")
tree = tui.build_tree()

print(f"Tree has {len(tree)} nodes")
print("Tree structure:")
for path, data in tree.items():
    children = data.get('children', [])
    stats = data.get('stats', {})
    print(f"  '{path}' -> children: {children}, has_stats: {bool(stats)}")

print("\nRoot children:")
root_children = tree.get('', {}).get('children', [])
print(f"Root has {len(root_children)} children: {root_children}")

print("\nTesting compose method...")
try:
    widgets = list(tui.compose())
    print(f"Compose returned {len(widgets)} widgets")
    for i, widget in enumerate(widgets):
        print(f"  Widget {i+1}: {type(widget).__name__}")
        if hasattr(widget, 'root') and widget.root:
            print(f"    Root label: {widget.root.label}")
            print(f"    Root children: {len(widget.root.children)}")
            for j, child in enumerate(widget.root.children):
                print(f"      Child {j+1}: {child.label}")
except Exception as e:
    print(f"Error in compose: {e}")
    import traceback
    traceback.print_exc() 