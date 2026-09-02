"""Regression tests for the Lakebase permission bootstrap script."""

import os
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "scripts" / "bootstrap" / "lakebase-perms.sh"


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content)
    path.chmod(0o755)


@pytest.mark.parametrize("credential_payload", ["not-json", "[]", '{"token":null}'])
def test_invalid_credential_json_reports_diagnostics(
    tmp_path: Path, credential_payload: str
) -> None:
    """A successful CLI call with invalid JSON must not fail silently."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_executable(
        bin_dir / "databricks",
        """#!/usr/bin/env bash
if [[ "$1 $2" == "current-user me" ]]; then
    printf '%s\n' '{"userName":"reviewer@example.com"}'
elif [[ "$1 $2" == "api get" ]]; then
    printf '%s\n' '{"endpoints":[{"name":"projects/test/branches/production/endpoints/primary","status":{"hosts":{"host":"test.database.cloud.databricks.com"}}}]}'
elif [[ "$1 $2" == "postgres generate-database-credential" ]]; then
    printf '%s\n' "$CREDENTIAL_PAYLOAD"
else
    exit 2
fi
""",
    )
    _write_executable(bin_dir / "psql", "#!/usr/bin/env bash\nexit 0\n")
    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["CREDENTIAL_PAYLOAD"] = credential_payload

    result = subprocess.run(
        [
            "bash",
            str(SCRIPT),
            "-i",
            "test",
            "-b",
            "production",
            "-d",
            "registry",
            "-s",
            "registry",
            "-a",
            "",
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "CLI succeeded but response was not valid JSON" in result.stderr
    assert "Lakebase diagnostics" in result.stderr
