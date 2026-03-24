"""Evaluation dataset and regression harness for ChatBI."""

from .cases import EvalCase, build_eval_cases
from .harness import run_evaluation, write_markdown_report

__all__ = ["EvalCase", "build_eval_cases", "run_evaluation", "write_markdown_report"]
