"""Phase 12: the repository must not contain a real secret, ever.

A one-off grep during review proves the repository was clean ONCE. This is the
same check as a test, so it is re-run on every commit and a leaked credential
fails the build instead of reaching a public remote - which is the only place
where finding it still matters, because a secret pushed to GitHub is a secret
that has to be rotated whether or not the commit is reverted afterwards.

Scanning the whole tree rather than a curated file list is deliberate: the
files most likely to leak a credential are the ones nobody thought to list.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]

#: Directories never scanned: build output, virtualenvs, and generated data.
#: data/ is excluded because it is regenerated synthetic content that is also
#: gitignored - it is not part of the repository.
SKIP_DIRS = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "node_modules",
    "build",
    "dist",
    "data",
    "logs",
    ".idea",
}

#: Extensions worth scanning. A binary file cannot be reviewed for a secret
#: by regex anyway, and including them produces noise rather than findings.
SCAN_SUFFIXES = {
    ".py",
    ".sql",
    ".sh",
    ".yml",
    ".yaml",
    ".toml",
    ".cfg",
    ".ini",
    ".md",
    ".json",
    ".env",
    ".txt",
    ".conf",
    ".dockerfile",
    ".example",
    "",
}

#: Files scanned by name whatever their extension. .env.example is the single
#: most important file in this scan: it is the template every developer copies,
#: so a real value in it propagates to every machine that runs the project.
ALWAYS_SCAN = {".env.example", "docker-compose.yml", "Makefile", "Dockerfile"}

#: Values that are obviously placeholders. Every one of these appears in a
#: file whose whole purpose is to show the SHAPE of a credential.
PLACEHOLDER_MARKERS = (
    "local_dev_only_change_me",
    "change_me",
    "changeme",
    "your_",
    "<",
    "xxx",
    "placeholder",
    "example",
    "dummy",
    "fake",
    "test_only",
    "replace_me",
    "not_a_secret",
    "not_a_real",
    "${",
    "$(",
    "os.environ",
    "getenv",
    "secretstr",
    "alias=",
    "field(",
)

#: Patterns for credentials that are unmistakably real if they appear at all.
#: These have no false-positive-tolerant form: an AWS key id IS an AWS key id.
HIGH_CONFIDENCE = {
    "aws_access_key_id": re.compile(r"\b(AKIA|ASIA)[0-9A-Z]{16}\b"),
    "private_key_block": re.compile(r"-----BEGIN (RSA |EC |OPENSSH |PGP )?PRIVATE KEY-----"),
    "github_token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{36,}\b"),
    "slack_token": re.compile(r"\bxox[abprs]-[0-9A-Za-z-]{10,}\b"),
    "google_api_key": re.compile(r"\bAIza[0-9A-Za-z_\-]{35}\b"),
    "stripe_key": re.compile(r"\b[sr]k_(live|test)_[0-9A-Za-z]{20,}\b"),
    "jwt": re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\."),
}

#: Assignments that MIGHT be a secret. Judged against PLACEHOLDER_MARKERS,
#: because "PASSWORD = 'local_dev_only_change_me'" is documentation and
#: "PASSWORD = 'Tr0ub4dor&3'" is an incident.
ASSIGNMENT = re.compile(
    r"""(?ix)
    \b(pass(word|wd)?|secret|api[_-]?key|access[_-]?key|auth[_-]?token|
       token|credential|private[_-]?key)
    \s*[:=]\s*
    ["']([^"'\n]{6,})["']
    """
)

#: A DSN with an inline password: postgresql://user:pa55w0rd@host/db
DSN_WITH_PASSWORD = re.compile(r"(?i)\b[a-z0-9+]{2,12}://[^\s:/@]+:([^\s:/@]{4,})@")


def _files_to_scan() -> list[Path]:
    found = []
    for path in REPO_ROOT.rglob("*"):
        if not path.is_file():
            continue
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.name not in ALWAYS_SCAN and path.suffix.lower() not in SCAN_SUFFIXES:
            continue
        # This file is a catalogue of secret PATTERNS; scanning it finds
        # itself and nothing else.
        if path.name == Path(__file__).name:
            continue
        found.append(path)
    return found


def _looks_like_a_placeholder(value: str) -> bool:
    lowered = value.lower()
    return any(marker in lowered for marker in PLACEHOLDER_MARKERS)


class TestNoSecretsCommitted:
    def test_the_scanner_actually_sees_the_repository(self) -> None:
        """Guard against the whole suite passing because it scanned nothing.

        A secret scanner with a broken path is indistinguishable from a clean
        repository, and it is the most common way this kind of test becomes
        decorative.
        """
        files = _files_to_scan()
        assert len(files) > 50, f"only {len(files)} files scanned - the walk is broken"
        names = {f.name for f in files}
        assert {"pyproject.toml", ".env.example"} <= names

    def test_no_high_confidence_credentials(self) -> None:
        """Provider-shaped keys. There is no benign reason for one to be here."""
        findings = []
        for path in _files_to_scan():
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            for label, pattern in HIGH_CONFIDENCE.items():
                for match in pattern.finditer(text):
                    line = text.count("\n", 0, match.start()) + 1
                    findings.append(f"{path.relative_to(REPO_ROOT)}:{line} {label}")
        assert not findings, "provider credentials found:\n" + "\n".join(findings)

    def test_no_non_placeholder_credential_assignments(self) -> None:
        """Assignments that are not obviously a placeholder.

        The rule this enforces is the one from the specification: credentials
        come from the environment, and the only literals in the repository are
        SHAPES showing what to put there.
        """
        findings = []
        for path in _files_to_scan():
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            for match in ASSIGNMENT.finditer(text):
                value = match.group(3)
                if _looks_like_a_placeholder(value) or _looks_like_a_placeholder(match.group(0)):
                    continue
                line = text.count("\n", 0, match.start()) + 1
                findings.append(f"{path.relative_to(REPO_ROOT)}:{line} -> {match.group(0)[:80]}")
        assert not findings, (
            "credential-shaped literals that are not recognisable placeholders:\n"
            + "\n".join(findings)
        )

    def test_no_connection_string_carries_an_inline_password(self) -> None:
        """A DSN is the most common accidental leak: it looks like a URL."""
        findings = []
        for path in _files_to_scan():
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            for match in DSN_WITH_PASSWORD.finditer(text):
                if _looks_like_a_placeholder(match.group(0)):
                    continue
                line = text.count("\n", 0, match.start()) + 1
                findings.append(f"{path.relative_to(REPO_ROOT)}:{line} -> {match.group(0)[:60]}")
        assert not findings, "DSNs with inline passwords:\n" + "\n".join(findings)

    def test_dotenv_is_ignored_and_untracked(self) -> None:
        """.env holds the real values, so it must never reach a commit.

        The requirement is that it is IGNORED, not that it is absent. An
        earlier version of this test asserted absence, which contradicted the
        project's own documented first step - `cp .env.example .env` - so
        every developer who followed the README failed the suite on setup. A
        test that fails when you follow the instructions is a test that gets
        deleted, taking the real check with it.

        What actually matters is checked instead, and more strictly than
        before: the pattern is present in .gitignore, and git itself confirms
        the file is ignored and untracked. Asking git rather than pattern
        matching catches a negation later in the file (`!.env`) that a
        substring check would miss.
        """
        gitignore = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        patterns = {line.strip() for line in gitignore}
        assert ".env" in patterns, ".gitignore does not exclude .env"
        assert "*.pem" in patterns and "*.key" in patterns

        git_exe = shutil.which("git")
        if not (REPO_ROOT / ".git").exists() or git_exe is None:
            pytest.skip("not a git checkout - the pattern assertions above still ran")

        def git(*args: str) -> subprocess.CompletedProcess[str]:
            # Absolute path from shutil.which, and every argument below is a
            # string literal in this file - no user or file input reaches it.
            return subprocess.run(  # noqa: S603
                [git_exe, *args], cwd=REPO_ROOT, capture_output=True, text=True, check=False
            )

        tracked = git("ls-files", "--error-unmatch", ".env")
        assert tracked.returncode != 0, ".env is TRACKED by git - it must never be committed"

        if (REPO_ROOT / ".env").exists():
            ignored = git("check-ignore", "-q", ".env")
            assert ignored.returncode == 0, (
                ".env exists but git does not consider it ignored - check for a "
                "later negation pattern in .gitignore"
            )

    def test_the_example_env_declares_every_required_setting(self) -> None:
        """The safe local-development path the specification asks for.

        If credentials are required but unavailable, the project must offer a
        clean configuration mechanism rather than stopping. .env.example IS
        that mechanism, so it has to stay complete - a required setting missing
        from it turns "copy this file" into "read the source to find out what
        else it wants".
        """
        example = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
        declared = {
            line.split("=", 1)[0].strip()
            for line in example.splitlines()
            if "=" in line and not line.lstrip().startswith("#")
        }
        required = {
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "POSTGRES_HOST",
            "POSTGRES_PORT",
            "WH_DB",
            "CMS_DB",
            "WH_ETL_USER",
            "WH_ETL_PASSWORD",
            "CMS_READER_USER",
            "CMS_READER_PASSWORD",
        }
        assert required <= declared, f"missing from .env.example: {sorted(required - declared)}"

    def test_every_example_credential_is_visibly_a_placeholder(self) -> None:
        """A plausible-looking password in .env.example gets copied verbatim."""
        example = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
        offenders = []
        for line in example.splitlines():
            if "=" not in line or line.lstrip().startswith("#"):
                continue
            key, _, value = line.partition("=")
            if not re.search(r"(?i)pass(word|wd)?|secret|token|key$", key):
                continue
            if value.strip() and not _looks_like_a_placeholder(value):
                offenders.append(line.strip())
        assert not offenders, (
            "these .env.example values do not read as placeholders and would be "
            f"copied into production as-is: {offenders}"
        )
