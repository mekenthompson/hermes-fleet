#!/usr/bin/env python3
"""Fail the image build when the baked Honcho plugin needs Agent core APIs that are missing.

The plugin is copied from plastic-labs/honcho and imports Agent modules at load
time (``from agent.skill_commands import ...``). If the Agent core in the parent
image does not provide a name, the memory provider would fail to import at
runtime. This check resolves every module-level absolute ``from <core module>
import <name>`` in the plugin against the core source tree by AST, without
importing anything, and exits non-zero listing each missing module or name.
Imports inside functions (lazy, optional seams) are not load-time requirements
and are ignored. Optional provider hooks the plugin defines (``on_profile_*``,
``read_config_fields``) are called by core only when present, so they need no
check here.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

CORE_ROOTS = ("agent", "hermes_cli", "tools", "hermes_constants", "hermes_state_common")


def _module_file(core: Path, module: str) -> Path | None:
    base = core.joinpath(*module.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def _defined_names(path: Path) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names.update(t.id for t in node.targets if isinstance(t, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            names.update((a.asname or a.name).split(".")[0] for a in node.names)
    return names


def find_missing(plugin: Path, core: Path) -> list[str]:
    missing: list[str] = []
    for source in sorted(plugin.glob("*.py")):
        tree = ast.parse(source.read_text(encoding="utf-8"))
        for node in tree.body:
            if not (isinstance(node, ast.ImportFrom) and node.level == 0 and node.module):
                continue
            if node.module.split(".")[0] not in CORE_ROOTS:
                continue
            target = _module_file(core, node.module)
            if target is None:
                missing.append(f"{source.name}:{node.lineno} module {node.module} not found in core")
                continue
            defined = _defined_names(target)
            missing.extend(
                f"{source.name}:{node.lineno} {node.module}.{alias.name} not defined in core"
                for alias in node.names
                if alias.name not in defined and alias.name != "*"
            )
    return missing


def main(argv: list[str]) -> int:
    plugin = Path(argv[1] if len(argv) > 1 else "/opt/hermes/plugins/memory/honcho")
    core = Path(argv[2] if len(argv) > 2 else "/opt/hermes")
    if not plugin.is_dir() or not core.is_dir():
        print(f"honcho plugin core-seam check: missing directory {plugin} or {core}", file=sys.stderr)
        return 2
    missing = find_missing(plugin, core)
    if missing:
        print("honcho plugin requires Agent core APIs that are missing; review the plugin pin or the core pin:", file=sys.stderr)
        for line in missing:
            print(f"  {line}", file=sys.stderr)
        return 1
    print("honcho plugin core seams: all module-level Agent imports resolve")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
