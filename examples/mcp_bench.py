#!/usr/bin/env python3
"""OntoBricks MCP benchmark — measure per-question, per-round, and per-tool timings.

Runs a list of questions against the MCP server and captures:
  - total_ms, rounds, LLM time, MCP time
  - prompt and completion token counts per round
  - per-tool call time and raw result size
  - token ratio (prompt:completion) — key indicator for context bloat

Usage
-----
    # Baseline (no optimizations)
    python3 mcp_bench.py --baseline --output before.json

    # Optimized (all opts on — default)
    python3 mcp_bench.py --domain <name> --output after.json

    # Compare results
    python3 -c "
    import json
    b=json.load(open('before.json')); a=json.load(open('after.json'))
    b_avg=sum(r['total_ms'] for r in b['results'] if not r['error'])/len(b['results'])
    a_avg=sum(r['total_ms'] for r in a['results'] if not r['error'])/len(a['results'])
    print(f'avg ms: {b_avg:.0f} → {a_avg:.0f}  ({(a_avg-b_avg)/b_avg*100:+.0f}%)')
    "

Environment variables
---------------------
    OBX_MCP_URL          MCP server URL
    OBX_LLM_ENDPOINT     Databricks serving endpoint (default: databricks-claude-haiku-4-5)
    OBX_PROFILE          Databricks CLI profile
    DATABRICKS_HOST      Databricks workspace host
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

import sys
sys.path.insert(0, os.path.dirname(__file__))
from mcp_client import (
    MCPClient, DatabricksLLM, tools_to_openai, get_token,
    warm_domain, compress_result, build_system_prompt,
    MCP_URL, ENDPOINT, PROFILE, DBRX_HOST,
)

# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class ToolCall:
    name: str
    call_ms: float
    raw_chars: int    # size before compression
    round_idx: int

@dataclass
class QuestionResult:
    question: str
    total_ms: float          = 0.0
    rounds:   int            = 0
    llm_ms:   float          = 0.0
    mcp_ms:   float          = 0.0
    prompt_tokens:     int   = 0
    completion_tokens: int   = 0
    answer_chars:      int   = 0
    tool_calls: list[ToolCall] = field(default_factory=list)
    error: Optional[str]     = None

    @property
    def unique_tools(self) -> list[str]:
        seen, out = set(), []
        for tc in self.tool_calls:
            if tc.name not in seen:
                seen.add(tc.name); out.append(tc.name)
        return out


# ── Instrumented agent loop ───────────────────────────────────────────────────

def run_question(
    question: str,
    mcp: MCPClient,
    llm: DatabricksLLM,
    tools_raw: list[dict],
    *,
    domain: Optional[str]  = None,
    graphql_schema: Optional[str] = None,
    compress: bool = True,
    opt_tools: bool = True,
) -> QuestionResult:
    result = QuestionResult(question=question)
    tools_oi = tools_to_openai(tools_raw) if opt_tools else [
        {"type": "function", "function": {
            "name": t["name"],
            "description": next(
                (l for l in t.get("description","").splitlines() if l.strip()), ""
            )[:200],
            "parameters": t.get("inputSchema", {"type":"object","properties":{}}),
        }} for t in tools_raw
    ]
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": build_system_prompt(domain, graphql_schema)},
        {"role": "user",   "content": question},
    ]

    t_total = time.perf_counter()
    try:
        for rnd in range(12):
            t_llm = time.perf_counter()
            resp = llm.chat(messages, tools=tools_oi)
            result.llm_ms += (time.perf_counter() - t_llm) * 1000
            result.rounds += 1

            choice = resp["choices"][0]
            msg = choice["message"]
            finish = choice.get("finish_reason", "stop")
            tc_list = msg.get("tool_calls") or []
            usage = resp.get("usage", {})
            result.prompt_tokens     += usage.get("prompt_tokens", 0)
            result.completion_tokens += usage.get("completion_tokens", 0)

            if finish == "stop" or not tc_list:
                raw = msg.get("content") or ""
                if isinstance(raw, list):
                    text = "\n".join(
                        b.get("text","") for b in raw
                        if isinstance(b,dict) and b.get("type")=="text"
                    ).strip()
                else:
                    text = str(raw)
                result.answer_chars = len(text)
                break

            raw_c = msg.get("content")
            clean_c: Any = (
                [b for b in raw_c if isinstance(b,dict) and b.get("type")=="text"] or None
                if isinstance(raw_c, list) else raw_c
            )
            messages.append({"role":"assistant","content":clean_c,"tool_calls":tc_list})

            for tc in tc_list:
                fn = tc["function"]
                args = json.loads(fn.get("arguments","{}") or "{}")
                t_mcp = time.perf_counter()
                raw_result = mcp.call_tool(fn["name"], args)
                result.mcp_ms += (time.perf_counter() - t_mcp) * 1000
                compressed = compress_result(fn["name"], raw_result, question=question, enabled=compress)
                result.tool_calls.append(ToolCall(
                    name=fn["name"],
                    call_ms=(time.perf_counter() - t_mcp) * 1000,
                    raw_chars=len(raw_result),
                    round_idx=rnd,
                ))
                messages.append({"role":"tool","tool_call_id":tc["id"],"content":compressed})
        else:
            result.error = "max rounds (12) reached"
    except Exception as exc:
        result.error = str(exc)

    result.total_ms = (time.perf_counter() - t_total) * 1000
    return result


# ── Report ────────────────────────────────────────────────────────────────────

def _tool_stats(results: list[QuestionResult]) -> dict[str, dict]:
    stats: dict[str, dict] = {}
    for r in results:
        for tc in r.tool_calls:
            s = stats.setdefault(tc.name, {"calls":0,"total_ms":0,"max_ms":0,"raw_chars":0})
            s["calls"] += 1; s["total_ms"] += tc.call_ms
            s["max_ms"] = max(s["max_ms"], tc.call_ms); s["raw_chars"] += tc.raw_chars
    for s in stats.values():
        s["avg_ms"] = s["total_ms"] / s["calls"] if s["calls"] else 0
    return stats


def print_report(results: list[QuestionResult], endpoint: str, opts: str) -> None:
    ok = [r for r in results if not r.error]
    print(f"\n{'═'*90}")
    print(f" BENCHMARK  endpoint={endpoint}  opts={opts}")
    print(f"{'═'*90}")
    print(f"  questions: {len(results)}  success: {len(ok)}  failed: {len(results)-len(ok)}")
    if ok:
        tots = [r.total_ms for r in ok]
        print(f"  total_ms :  min={min(tots):.0f}  avg={sum(tots)/len(tots):.0f}  max={max(tots):.0f}")
        print(f"  rounds   :  avg={sum(r.rounds for r in ok)/len(ok):.1f}")
        p = sum(r.prompt_tokens for r in ok); c = sum(r.completion_tokens for r in ok)
        print(f"  tokens   :  prompt={p:,}  completion={c:,}  ratio={p//max(c,1)}:1")
        lm = sum(r.llm_ms for r in ok); mm = sum(r.mcp_ms for r in ok)
        print(f"  llm_ms   :  total={lm:.0f}  avg/q={lm/len(ok):.0f}")
        print(f"  mcp_ms   :  total={mm:.0f}  avg/q={mm/len(ok):.0f}")
    print()

    print(f"{'#':>2}  {'ms':>7}  {'rds':>3}  {'p_tok':>6}  {'c_tok':>5}  {'tools':<40}  status")
    print("─" * 90)
    for i, r in enumerate(results, 1):
        status = f"✗ {r.error[:35]}" if r.error else "✓"
        print(f"{i:>2}  {r.total_ms:>7.0f}  {r.rounds:>3}  "
              f"{r.prompt_tokens:>6}  {r.completion_tokens:>5}  "
              f"{','.join(r.unique_tools)[:40]:<40}  {status}")
    print()

    tstats = _tool_stats(ok)
    if tstats:
        print("── Tool breakdown ──")
        print(f"  {'tool':<35}  {'calls':>5}  {'avg_ms':>7}  {'max_ms':>7}  {'total_ms':>9}  {'avg_raw':>8}")
        print("  " + "─"*75)
        for name, s in sorted(tstats.items(), key=lambda x: -x[1]["total_ms"]):
            avg_raw = s["raw_chars"] // s["calls"]
            print(f"  {name:<35}  {s['calls']:>5}  {s['avg_ms']:>7.0f}  "
                  f"{s['max_ms']:>7.0f}  {s['total_ms']:>9.0f}  {avg_raw:>7} ch")
    print()

    if ok:
        lm = sum(r.llm_ms for r in ok); mm = sum(r.mcp_ms for r in ok)
        tot = sum(r.total_ms for r in ok)
        print("── Optimization hints ──")
        print(f"  time split: LLM {lm/tot*100:.0f}%  MCP {mm/tot*100:.0f}%  overhead {(tot-lm-mm)/tot*100:.0f}%")
        p = sum(r.prompt_tokens for r in ok); c = sum(r.completion_tokens for r in ok)
        if p // max(c,1) > 15:
            print(f"  ⚠ token ratio {p//max(c,1)}:1 — result compression would help")
        avg_rounds = sum(r.rounds for r in ok) / len(ok)
        if avg_rounds > 4:
            print(f"  ⚠ avg {avg_rounds:.1f} rounds — domain pre-selection or schema preload would reduce rounds")
        slowest = max(tstats.items(), key=lambda x: x[1]["avg_ms"])[0] if tstats else "-"
        print(f"  slowest tool: {slowest}")


# ── CLI ───────────────────────────────────────────────────────────────────────

DEFAULT_QUESTIONS = [
    "What entity types exist in this domain and how many instances of each?",
    "Show me 5 representative entities with their key attributes.",
]

def main() -> None:
    ap = argparse.ArgumentParser(description="OntoBricks MCP benchmark")
    ap.add_argument("--questions", nargs="+", help="Questions to run (default: built-in set)")
    ap.add_argument("--runs",     type=int, default=1)
    ap.add_argument("--endpoint", default=ENDPOINT)
    ap.add_argument("--profile",  default=PROFILE)
    ap.add_argument("--host",     default=DBRX_HOST)
    ap.add_argument("--domain",   default=None,  help="Pre-select domain (OPT2+3)")
    ap.add_argument("--output",   default=None,  help="Save JSON to FILE")
    ap.add_argument("--baseline", action="store_true", help="Disable all optimizations")
    ap.add_argument("--no-preload-schema", dest="preload_schema", action="store_false", default=True)
    ap.add_argument("--no-compress",       dest="compress",       action="store_false", default=True)
    ap.add_argument("--no-opt-tools",      dest="opt_tools",      action="store_false", default=True)
    args = ap.parse_args()

    if args.baseline:
        args.domain = None; args.preload_schema = False
        args.compress = False; args.opt_tools = False

    opts = " | ".join(filter(None, [
        "OPT1:tool-desc"    if args.opt_tools      else "",
        "OPT2:domain-presel" if args.domain         else "",
        "OPT3:schema-preload" if args.preload_schema else "",
        "OPT4+5:compress"   if args.compress        else "",
    ])) or "NONE (baseline)"

    questions = args.questions or DEFAULT_QUESTIONS
    total = len(questions) * args.runs

    print(f"  questions: {len(questions)} × {args.runs} = {total}")
    print(f"  endpoint : {args.endpoint}  domain: {args.domain or '(auto)'}")
    print(f"  opts     : {opts}\n")

    if not args.host:
        sys.exit("ERROR: set DATABRICKS_HOST env var or pass --host")
    if not MCP_URL:
        sys.exit("ERROR: set OBX_MCP_URL env var")

    token = get_token(args.profile)
    mcp = MCPClient(MCP_URL, token)
    try: mcp.initialize()
    except Exception: pass
    tools = mcp.list_tools()
    print(f"MCP: {len(tools)} tools")

    schema: Optional[str] = None
    if args.domain:
        schema = warm_domain(mcp, args.domain, preload_schema=args.preload_schema)
        print(f"Domain '{args.domain}' warmed (schema {len(schema or '')} chars)")

    llm = DatabricksLLM(args.host, args.endpoint, token)
    all_results: list[QuestionResult] = []
    done = 0

    try:
        for _ in range(args.runs):
            for q in questions:
                done += 1
                print(f"[{done:>2}/{total}] {q[:65]} ...", end=" ", flush=True)
                r = run_question(q, mcp, llm, tools,
                                 domain=args.domain,
                                 graphql_schema=schema if args.preload_schema else None,
                                 compress=args.compress, opt_tools=args.opt_tools)
                print(f"✗ {r.error[:35]}" if r.error else f"✓ {r.total_ms:.0f}ms / {r.rounds} rds")
                all_results.append(r)
    finally:
        mcp.close(); llm.close()

    print_report(all_results, args.endpoint, opts)

    if args.output:
        data = {
            "endpoint": args.endpoint, "opts": opts,
            "results": [
                {**{k: v for k, v in asdict(r).items() if k != "tool_calls"},
                 "tool_calls": [asdict(tc) for tc in r.tool_calls],
                 "unique_tools": r.unique_tools}
                for r in all_results
            ],
            "tool_stats": _tool_stats([r for r in all_results if not r.error]),
        }
        json.dump(data, open(args.output, "w"), indent=2)
        print(f"JSON saved to {args.output}")


if __name__ == "__main__":
    main()
