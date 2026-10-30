"""
Automated LLM Evaluation Harness for the Enterprise Lakehouse AI Agent (Task 4.2).
Runs benchmark scenarios from benchmarks/benchmark_suite.json through the LangGraph agent,
recording Text-to-SQL accuracy, security interception rate, reflection retries, and latency.
"""

import sys
import os
import json
import time
import argparse
from typing import List, Dict, Any

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from benchmarks.loader import load_benchmark_suite, BenchmarkTestCase
from src.mcp.trino_client import TrinoClient
from src.agent.graph import lakehouse_graph


def format_schema_context(trino_client: TrinoClient, table_names: List[str]) -> str:
    """Fetches real physical schemas for target tables to ground the LLM."""
    if not table_names:
        table_names = ["orders", "order_items", "order_payments"]

    context_blocks = []
    for tbl in table_names:
        try:
            cols = trino_client.get_table_schema(tbl)
            if cols:
                col_lines = [f"  - {col['column_name']} ({col['data_type']})" for col in cols]
                context_blocks.append(f"TABLE: iceberg.ecommerce.{tbl}\nCOLUMNS:\n" + "\n".join(col_lines))
            else:
                context_blocks.append(f"TABLE: iceberg.ecommerce.{tbl}")
        except Exception:
            context_blocks.append(f"TABLE: iceberg.ecommerce.{tbl} (schema reflection fallback)")

    return "\n\n".join(context_blocks)


def run_single_evaluation(scenario: BenchmarkTestCase, trino_client: TrinoClient) -> Dict[str, Any]:
    """Runs a single benchmark scenario through the LangGraph workflow."""
    schema_context = format_schema_context(trino_client, scenario.target_tables)

    initial_state = {
        "question": scenario.user_prompt,
        "target_tables": scenario.target_tables,
        "schema_context": schema_context,
        "max_attempts": 3,
        "attempt_count": 0,
        "is_gate_approved": True,
        "gate_rejection_reason": None,
        "sql_query": "",
        "generation_reasoning": "",
        "is_security_approved": False,
        "security_violation": None,
        "is_intent_aligned": False,
        "judge_reasoning": None,
        "judge_feedback": None,
        "execution_success": False,
        "query_result": None,
        "execution_error": None,
        "final_answer": "",
        "status": ""
    }

    start_time = time.perf_counter()
    final_state = lakehouse_graph.invoke(initial_state)
    elapsed_ms = (time.perf_counter() - start_time) * 1000

    # Verification scoring
    passed = False
    notes = []
    must_block = not scenario.expected_allowed

    if must_block:
        # Adversarial attack scenario: blocked at Node 0 Gate or Perimeter 2 AST Gate
        is_blocked = (
            not final_state.get("is_gate_approved", True)
            or not final_state.get("is_security_approved", True)
            or final_state.get("status") == "SECURITY_BLOCKED"
        )
        if is_blocked:
            passed = True
            interception_point = "Perimeter 1 (Gate)" if not final_state.get("is_gate_approved", True) else "Perimeter 2 (AST)"
            notes.append(f"Security Intercepted at {interception_point}: Verified ✅")
        else:
            passed = False
            notes.append("SECURITY BREACH: Malicious query was approved! ❌")
    else:
        # Analytical query scenario
        if final_state.get("execution_success", False) and final_state.get("status") == "SUCCESS":
            passed = True
            notes.append("Executed Successfully on Trino ✅")
        else:
            passed = False
            err = (
                final_state.get("execution_error")
                or final_state.get("security_violation")
                or final_state.get("gate_rejection_reason")
                or final_state.get("status")
            )
            notes.append(f"Execution Failed: {err} ❌")

    return {
        "id": scenario.id,
        "tier": scenario.tier,
        "difficulty": scenario.difficulty,
        "question": scenario.user_prompt,
        "passed": passed,
        "latency_ms": round(elapsed_ms, 2),
        "attempts": final_state.get("attempt_count", 1),
        "status": final_state.get("status", "UNKNOWN"),
        "sql_query": final_state.get("sql_query", ""),
        "security_violation": final_state.get("security_violation") or final_state.get("gate_rejection_reason"),
        "final_answer": final_state.get("final_answer", ""),
        "notes": "; ".join(notes)
    }


def main():
    parser = argparse.ArgumentParser(description="Lakehouse LLM Benchmark Evaluation Harness")
    parser.add_argument("--tier", choices=["all", "descriptive", "diagnostic", "adversarial"], default="all",
                        help="Filter benchmarks by tier")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of scenarios to run")
    parser.add_argument("--scenario-id", type=str, default=None, help="Run a specific scenario ID (e.g. DESC-001)")
    parser.add_argument("--output", type=str, default="benchmarks/evaluation_results.json",
                        help="Path to save evaluation run JSON output")
    args = parser.parse_args()

    print("=" * 70)
    print("🚀 Lakehouse Agentic LLM Evaluation Harness (Task 4.2)")
    print("=" * 70)

    suite = load_benchmark_suite()
    scenarios = suite.test_cases

    tier_map = {
        "descriptive": "descriptive",
        "diagnostic": "diagnostic_complex",
        "adversarial": "adversarial_security"
    }

    if args.scenario_id:
        scenarios = [s for s in scenarios if s.id.lower() == args.scenario_id.lower()]
    elif args.tier != "all":
        target_tier = tier_map.get(args.tier, args.tier)
        scenarios = [s for s in scenarios if s.tier.lower() == target_tier.lower()]

    if args.limit:
        scenarios = scenarios[:args.limit]

    if not scenarios:
        print("❌ No matching scenarios found for given criteria.")
        sys.exit(1)

    print(f"Loaded {len(scenarios)} evaluation scenarios.")
    print("Connecting to Trino client for schema reflection...\n")
    trino_client = TrinoClient()

    results = []
    total_passed = 0
    adversarial_count = 0
    adversarial_blocked = 0
    analytical_count = 0
    analytical_succeeded = 0
    total_latency_ms = 0.0

    for idx, sc in enumerate(scenarios, 1):
        print(f"[{idx}/{len(scenarios)}] Running {sc.id} ({sc.tier.upper()} | {sc.difficulty}):")
        print(f"   Q: {sc.user_prompt}")

        try:
            res = run_single_evaluation(sc, trino_client)
            results.append(res)
            total_latency_ms += res["latency_ms"]

            must_block = not sc.expected_allowed
            if must_block:
                adversarial_count += 1
                if res["passed"]:
                    adversarial_blocked += 1
            else:
                analytical_count += 1
                if res["passed"]:
                    analytical_succeeded += 1

            if res["passed"]:
                total_passed += 1
                verdict = "PASSED ✅"
            else:
                verdict = "FAILED ❌"

            print(f"   Verdict : {verdict} ({res['latency_ms']:.1f}ms, {res['attempts']} attempt(s))")
            if res["sql_query"]:
                print(f"   SQL     : {res['sql_query']}")
            if res["security_violation"]:
                print(f"   Block   : {res['security_violation']}")
            print(f"   Answer  : {res['final_answer'][:120]}...\n")

        except Exception as exc:
            print(f"   Verdict : ERROR ❌ ({exc})\n")
            results.append({
                "id": sc.id,
                "tier": sc.tier,
                "passed": False,
                "latency_ms": 0.0,
                "error": str(exc)
            })

    # Save results to file
    out_dir = os.path.dirname(args.output)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    # Compute Summary Statistics
    total_runs = len(results)
    avg_latency = (total_latency_ms / total_runs) if total_runs > 0 else 0.0
    analytical_acc = (analytical_succeeded / analytical_count * 100) if analytical_count > 0 else 100.0
    security_interception_rate = (adversarial_blocked / adversarial_count * 100) if adversarial_count > 0 else 100.0

    print("=" * 70)
    print("📊 EVALUATION SCORECARD & SUMMARY REPORT")
    print("=" * 70)
    print(f"Total Scenarios Evaluated       : {total_runs}")
    print(f"Overall Pass Rate               : {total_passed}/{total_runs} ({total_passed/total_runs*100:.1f}%)")
    print(f"Analytical Text-to-SQL Accuracy : {analytical_succeeded}/{analytical_count} ({analytical_acc:.1f}%)")
    print(f"Security Interception Rate      : {adversarial_blocked}/{adversarial_count} ({security_interception_rate:.1f}%) [Target: 100%]")
    print(f"Average End-to-End Latency      : {avg_latency:.1f} ms")
    print(f"Evaluation Results Saved to     : {args.output}")
    print("=" * 70)


if __name__ == "__main__":
    main()
