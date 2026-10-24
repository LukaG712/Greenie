"""Automated code review for mission validation.

Extends the existing auto_review with structured findings for agent-generated
code changes, including deterministic checks and AI-assisted review.
"""

import json
import logging
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

from hub.mission.models import ValidationResult, ValidationSeverity

logger = logging.getLogger(__name__)


@dataclass
class ReviewFinding:
    """A single code review finding."""
    severity: str = ValidationSeverity.INFO.value
    category: str = ""  # security, correctness, style, test, docs
    file_path: Optional[str] = None
    line_number: Optional[int] = None
    explanation: str = ""
    suggested_correction: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def run_deterministic_checks(
    repo_path: str,
    test_commands: Optional[list[str]] = None,
) -> list[ReviewFinding]:
    """Run deterministic checks: build, test, lint, type check.

    Returns a list of findings from failed checks.
    """
    findings = []
    path = Path(repo_path)

    # Test commands
    commands = test_commands or []
    if path.joinpath("pytest.ini").exists() or path.joinpath("pyproject.toml").exists():
        if not commands:
            commands = ["pytest"]

    for cmd in commands:
        result = _run_command(cmd, cwd=repo_path, timeout=120)
        if result["returncode"] != 0:
            findings.append(ReviewFinding(
                severity=ValidationSeverity.CRITICAL.value,
                category="test",
                explanation=f"Command failed: {cmd}\n{result['stderr'][:500]}",
                suggested_correction="Fix the failing tests or build errors",
            ))

    # Lint check (ruff)
    if _command_exists("ruff"):
        result = _run_command("ruff check .", cwd=repo_path, timeout=60)
        if result["returncode"] != 0 and result["stdout"]:
            findings.append(ReviewFinding(
                severity=ValidationSeverity.MINOR.value,
                category="style",
                explanation=f"Lint issues found:\n{result['stdout'][:500]}",
                suggested_correction="Run 'ruff check --fix .' to auto-fix",
            ))

    # Type check (mypy)
    if _command_exists("mypy"):
        result = _run_command("mypy .", cwd=repo_path, timeout=120)
        if result["returncode"] != 0 and result["stdout"]:
            findings.append(ReviewFinding(
                severity=ValidationSeverity.MAJOR.value,
                category="correctness",
                explanation=f"Type errors found:\n{result['stdout'][:500]}",
                suggested_correction="Fix type annotations to match expected types",
            ))

    return findings


def review_code_change(
    diff_text: str,
    file_path: Optional[str] = None,
) -> list[ReviewFinding]:
    """Review a code diff for common issues.

    Performs static analysis on the diff text to find potential problems.
    """
    findings = []

    # Security checks
    security_patterns = [
        (r"eval\s*\(", "Use of eval() is a security risk", "Use ast.literal_eval() or a safer alternative"),
        (r"exec\s*\(", "Use of exec() is a security risk", "Avoid exec(); use a safer alternative"),
        (r"os\.system\s*\(", "Use of os.system() is a security risk", "Use subprocess.run() with a list of arguments"),
        (r"pickle\.loads?\s*\(", "Use of pickle is a security risk", "Use json or a safer serialization format"),
        (r"subprocess\.call\s*\(.*shell\s*=\s*True", "Shell injection risk", "Use a list of arguments instead of shell=True"),
    ]

    for pattern, explanation, correction in security_patterns:
        import re
        if re.search(pattern, diff_text, re.IGNORECASE):
            findings.append(ReviewFinding(
                severity=ValidationSeverity.CRITICAL.value,
                category="security",
                file_path=file_path,
                explanation=explanation,
                suggested_correction=correction,
            ))

    # Common correctness issues
    correctness_patterns = [
        (r"print\s*\(", "Debug print statement left in code", "Remove or replace with proper logging"),
        (r"TODO|FIXME|HACK|XXX", "Incomplete code marker found", "Address the TODO/FIXME before merging"),
        (r"pass\s*$", "Empty pass statement", "Ensure the function has proper implementation"),
    ]

    for pattern, explanation, correction in correctness_patterns:
        import re
        if re.search(pattern, diff_text):
            findings.append(ReviewFinding(
                severity=ValidationSeverity.MINOR.value,
                category="correctness",
                file_path=file_path,
                explanation=explanation,
                suggested_correction=correction,
            ))

    return findings


def review_mission_changes(
    mission_id: str,
    repo_path: str,
    test_commands: Optional[list[str]] = None,
) -> list[ValidationResult]:
    """Run a full review pipeline for a mission's changes.

    Combines deterministic checks with static analysis.
    """
    all_findings: list[ReviewFinding] = []

    # Deterministic checks
    all_findings.extend(run_deterministic_checks(repo_path, test_commands))

    # Git diff review (if in a git repo)
    try:
        result = _run_command("git diff HEAD", cwd=repo_path, timeout=30)
        if result["returncode"] == 0 and result["stdout"].strip():
            diff_findings = review_code_change(result["stdout"])
            all_findings.extend(diff_findings)
    except Exception:
        pass  # Not a git repo or git not available

    # Convert to ValidationResult
    results = []
    for finding in all_findings:
        vr = ValidationResult(
            mission_id=mission_id,
            severity=finding.severity,
            file_path=finding.file_path,
            line_number=finding.line_number,
            explanation=f"[{finding.category}] {finding.explanation}",
            suggested_correction=finding.suggested_correction,
        )
        results.append(vr)

    return results


def _run_command(cmd: str, cwd: str, timeout: int = 60) -> dict[str, Any]:
    """Run a shell command and capture output."""
    import shlex
    try:
        args = shlex.split(cmd)
        result = subprocess.run(
            args,
            shell=False,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return {
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    except subprocess.TimeoutExpired:
        return {
            "returncode": -1,
            "stdout": "",
            "stderr": f"Command timed out after {timeout}s: {cmd}",
        }
    except Exception as e:
        return {
            "returncode": -1,
            "stdout": "",
            "stderr": str(e),
        }


def _command_exists(cmd: str) -> bool:
    """Check if a command exists in PATH."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "show", cmd],
            capture_output=True,
            timeout=10,
        )
        if result.returncode == 0:
            return True
        # Also check as standalone command
        import shutil
        return shutil.which(cmd) is not None
    except Exception:
        return False
