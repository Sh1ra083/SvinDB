#!/usr/bin/env python3
"""Изолированные проверки контракта tools/build_release_linux.sh."""

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("build_release_linux.sh")


class BuildReleaseLinuxTests(unittest.TestCase):
    def setUp(self):
        self.work = Path(tempfile.mkdtemp(prefix="svindb-build-release-test-"))
        self.root = self.work / "repo"
        (self.root / "tools").mkdir(parents=True)
        (self.root / "crates/svindb-server").mkdir(parents=True)
        shutil.copy2(SCRIPT, self.root / "tools/build_release_linux.sh")
        (self.root / "tools/verify_oci_binary.py").write_text(
            "#!/usr/bin/env python3\n", encoding="utf-8"
        )
        # Пакетирование здесь не является предметом теста: заглушка фиксирует,
        # что до него дошли только после успешной проверки загруженного образа.
        (self.root / "tools/package_release.sh").write_text(
            "#!/bin/sh\nprintf '%s\\n' package-called > \"$PACKAGE_MARKER\"\n",
            encoding="utf-8",
        )
        (self.root / "tools/package_release.sh").chmod(0o755)
        (self.root / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
        self.path = self.work / "bin"
        self.path.mkdir()
        self.log = self.work / "docker.log"
        self.marker = self.work / "package.marker"
        self._write_docker_mock()

    def tearDown(self):
        shutil.rmtree(self.work)

    def _write_docker_mock(self):
        docker = self.path / "docker"
        docker.write_text(
            """#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$DOCKER_LOG"
case "$1" in
  buildx) exit 0 ;;
  inspect)
    case "$*" in
      *revision*) printf '%s\\n' "$FAKE_REVISION" ;;
      *version*) printf '%s\\n' "$FAKE_VERSION" ;;
      *features*) printf '%s\\n' "$FAKE_FEATURES" ;;
    esac
    ;;
  create) printf 'container-id\\n' ;;
  cp) target=""; for arg do target="$arg"; done; : > "$target" ;;
  rm) exit 0 ;;
  *) echo "unexpected docker command: $*" >&2; exit 97 ;;
esac
""",
            encoding="utf-8",
        )
        docker.chmod(0o755)
        date = self.path / "date"
        date.write_text("#!/bin/sh\nprintf '2026-01-01T00:00:00Z\\n'\n", encoding="utf-8")
        date.chmod(0o755)

    def _run(self, version, *, labels=None):
        (self.root / "crates/svindb-server/Cargo.toml").write_text(
            f'[package]\nversion = "{version}"\n', encoding="utf-8"
        )
        labels = labels or {
            "FAKE_REVISION": "abcdef1234567",
            "FAKE_VERSION": version,
            "FAKE_FEATURES": "",
        }
        env = os.environ.copy()
        env.update(
            PATH=f"{self.path}:{env['PATH']}",
            DOCKER_LOG=str(self.log),
            PACKAGE_MARKER=str(self.marker),
            SVINDB_BUILD_REVISION="abcdef1234567",
            SOURCE_DATE_EPOCH="1700000000",
            **labels,
        )
        output = self.work / "out"
        result = subprocess.run(
            [str(self.root / "tools/build_release_linux.sh"), str(output)],
            text=True, capture_output=True, env=env,
        )
        return result, output

    def test_prerelease_is_parsed_and_loaded_image_is_verified_before_cp(self):
        result, _ = self._run("3.0.0-alpha.1")
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.log.read_text(encoding="utf-8").splitlines()
        load = next(i for i, call in enumerate(calls) if " --load " in f" {call} ")
        self.assertIn("--build-arg SVINDB_BUILD_VERSION=3.0.0-alpha.1", calls[load])
        inspect_indexes = [i for i, call in enumerate(calls) if call.startswith("inspect ")]
        cp_index = next(i for i, call in enumerate(calls) if call.startswith("cp "))
        self.assertTrue(all(i < cp_index for i in inspect_indexes))
        self.assertTrue(self.marker.exists())

    def test_invalid_version_is_rejected_before_docker(self):
        result, _ = self._run("3.0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("версия svindb-server не разобралась", result.stderr)
        self.assertFalse(self.log.exists())
        self.assertFalse(self.marker.exists())

    def test_mismatched_loaded_image_labels_cannot_package_wrong_existing_tag(self):
        result, _ = self._run(
            "3.0.0-alpha.1",
            labels={
                "FAKE_REVISION": "stale0000000",
                "FAKE_VERSION": "3.0.0",
                "FAKE_FEATURES": "",
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("метки загруженного Docker-образа", result.stderr)
        calls = self.log.read_text(encoding="utf-8").splitlines()
        self.assertTrue(any("--load" in call for call in calls))
        self.assertFalse(any(call.startswith("cp ") for call in calls))
        self.assertFalse(self.marker.exists())


if __name__ == "__main__":
    unittest.main()
