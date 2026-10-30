"""
benchmarks/loader.py
================================================================
Enterprise Agentic Streaming Lakehouse - Benchmark Loader
Task 4.1: Pydantic Data Models and Suite Loader
================================================================
"""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field


class EvaluationCriteria(BaseModel):
    """Specific pass/fail assertions for a test case."""
    min_rows: Optional[int] = None
    max_rows: Optional[int] = None
    required_columns: Optional[List[str]] = None
    must_contain_where_filter: Optional[bool] = None
    must_join: Optional[bool] = None
    has_having_clause: Optional[bool] = None
    must_be_blocked: Optional[bool] = None
    error_type: Optional[str] = None
    must_be_clamped: Optional[bool] = None
    clamped_to: Optional[int] = None


class BenchmarkTestCase(BaseModel):
    """Individual benchmark test scenario."""
    id: str = Field(description="Unique scenario ID, e.g., DESC-001, DIAG-001, ADV-001")
    tier: str = Field(description="descriptive | diagnostic_complex | adversarial_security")
    category: str = Field(description="Subcategory of the analytical or security question")
    difficulty: str = Field(description="easy | medium | hard | critical")
    user_prompt: str = Field(description="Natural language question presented to the agent")
    target_tables: List[str] = Field(default_factory=list)
    gold_sql: Optional[str] = Field(default=None, description="Reference ANSI Trino SQL")
    expected_allowed: bool = Field(description="True if query should succeed, False if security should block it")
    expected_result_type: Optional[str] = None
    expected_security_error: Optional[str] = None
    prohibited_tokens: Optional[List[str]] = None
    circuit_breaker_clamped: Optional[bool] = None
    max_enforced_limit: Optional[int] = None
    evaluation_criteria: Optional[Dict[str, Any]] = Field(default_factory=dict)


class BenchmarkSuite(BaseModel):
    """Container for the entire benchmark evaluation dataset."""
    suite_metadata: Dict[str, Any]
    test_cases: List[BenchmarkTestCase]

    def get_tier(self, tier: str) -> List[BenchmarkTestCase]:
        """Filter test cases by tier (descriptive, diagnostic_complex, adversarial_security)."""
        return [tc for tc in self.test_cases if tc.tier.lower() == tier.lower()]

    def get_case(self, case_id: str) -> Optional[BenchmarkTestCase]:
        """Fetch a specific test scenario by ID."""
        for tc in self.test_cases:
            if tc.id.upper() == case_id.upper():
                return tc
        return None

    def summary(self) -> Dict[str, Any]:
        """Return counts and statistics about the benchmark suite."""
        return {
            "total_cases": len(self.test_cases),
            "descriptive": len(self.get_tier("descriptive")),
            "diagnostic_complex": len(self.get_tier("diagnostic_complex")),
            "adversarial_security": len(self.get_tier("adversarial_security")),
            "difficulties": {
                diff: sum(1 for tc in self.test_cases if tc.difficulty == diff)
                for diff in ["easy", "medium", "hard", "critical"]
            },
        }


def load_benchmark_suite(file_path: Optional[str] = None) -> BenchmarkSuite:
    """
    Loads and validates the benchmark suite JSON file.
    
    Args:
        file_path: Path to benchmark_suite.json. Defaults to standard repository path.
        
    Returns:
        BenchmarkSuite Pydantic instance.
    """
    if file_path is None:
        file_path = str(Path(__file__).parent / "benchmark_suite.json")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return BenchmarkSuite(**data)

