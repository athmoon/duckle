#!/usr/bin/env python3
"""Query the code graph built by scripts/codegraph.py.

Every answer carries file:line, because the graph LOCATES and the source
DECIDES. Nothing here should ever be quoted as behaviour without opening the
cited lines - a regex-built index can be wrong about what a function does and
is only reliable about where it is.

    cg.py def <name>          where a symbol is defined
    cg.py find <regex>        symbols whose name matches
    cg.py callers <name>      who calls it (name-matched, see caveat)
    cg.py calls <name>        what it calls
    cg.py file <path-substr>  symbols in matching files
    cg.py near <file> <line>  the definition enclosing a line
    cg.py crate <name>        files and symbol counts for a crate
    cg.py tests <regex>       test functions matching
    cg.py stat                graph size

Add --limit N (default 40).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT = os.environ.get("DUCKLE_CODEGRAPH") or os.path.join(
    os.path.expanduser("~"),
    ".claude", "projects", "C--Users-Sourav-Roy-Documents-duckle", "memory", "codegraph",
)


def load(dirname: str, name: str):
    p = os.path.join(dirname, name)
    if not os.path.exists(p):
        sys.exit(f"no graph at {p} - run: python scripts/codegraph.py --out {dirname}")
    with open(p, encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def show(s: dict) -> str:
    flags = "".join("[" + f + "]" for f in s.get("x", []))
    doc = ("  // " + s["d"]) if s.get("d") else ""
    return f"{s['f']}:{s['l']}  {s['k']:6s} {s['n']}{flags}{doc}"


def main() -> int:
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("cmd", nargs="?", default="stat")
    ap.add_argument("args", nargs="*")
    ap.add_argument("--limit", type=int, default=40)
    ap.add_argument("--graph", default=DEFAULT)
    a = ap.parse_args()
    g, lim = a.graph, a.limit

    def cap(rows):
        rows = list(rows)
        for r in rows[:lim]:
            print(r)
        if len(rows) > lim:
            print(f"... {len(rows) - lim} more (--limit)")
        if not rows:
            print("(nothing)")

    if a.cmd == "def":
        want = a.args[0]
        cap(show(s) for s in load(g, "symbols.jsonl") if s["n"] == want)

    elif a.cmd == "find":
        pat = re.compile(a.args[0], re.I)
        cap(show(s) for s in load(g, "symbols.jsonl") if pat.search(s["n"]))

    elif a.cmd == "callers":
        want = a.args[0]
        seen = set()
        rows = []
        for e in load(g, "edges.jsonl"):
            if e["b"] == want and (e["a"], e["f"]) not in seen:
                seen.add((e["a"], e["f"]))
                rows.append(f"{e['f']}:{e['l']}  {e['a']} -> {want}")
        cap(rows)

    elif a.cmd == "calls":
        want = a.args[0]
        seen = set()
        rows = []
        for e in load(g, "edges.jsonl"):
            if e["a"] == want and e["b"] not in seen:
                seen.add(e["b"])
                rows.append(f"{e['f']}:{e['l']}  {want} -> {e['b']}")
        cap(rows)

    elif a.cmd == "file":
        sub = a.args[0].replace("\\", "/")
        cap(show(s) for s in load(g, "symbols.jsonl") if sub in s["f"])

    elif a.cmd == "near":
        path, line = a.args[0].replace("\\", "/"), int(a.args[1])
        best = None
        for s in load(g, "symbols.jsonl"):
            if s["f"].endswith(path) and s["l"] <= line:
                if best is None or s["l"] > best["l"]:
                    best = s
        print(show(best) if best else "(nothing)")

    elif a.cmd == "crate":
        want = a.args[0]
        rows = [
            f"{r['f']}  {r['lines']:6d} lines  {r['syms']:4d} symbols"
            for r in load(g, "files.jsonl")
            if want in r["c"]
        ]
        cap(sorted(rows, key=lambda s: -int(s.split()[1])))

    elif a.cmd == "tests":
        pat = re.compile(a.args[0] if a.args else ".", re.I)
        cap(
            show(s)
            for s in load(g, "symbols.jsonl")
            if "test" in s.get("x", []) and pat.search(s["n"])
        )

    else:
        syms = list(load(g, "symbols.jsonl"))
        edges = sum(1 for _ in load(g, "edges.jsonl"))
        files = list(load(g, "files.jsonl"))
        kinds: dict[str, int] = {}
        for s in syms:
            kinds[s["k"]] = kinds.get(s["k"], 0) + 1
        print(f"graph {g}")
        print(f"  {len(files)} files, {sum(f['lines'] for f in files):,} lines")
        print(f"  {len(syms)} symbols, {edges} edges")
        print("  " + ", ".join(f"{k}={v}" for k, v in sorted(kinds.items())))
    return 0


if __name__ == "__main__":
    sys.exit(main())
