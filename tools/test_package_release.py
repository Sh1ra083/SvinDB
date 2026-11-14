#!/usr/bin/env python3
"""Isolated release archive contents and reproducibility contract."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest
import zipfile


SCRIPT = Path(__file__).with_name("package_release.sh")


class PackageReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="svindb-package-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "repo"
        (self.root / "tools").mkdir(parents=True)
        shutil.copy2(SCRIPT, self.root / "tools/package_release.sh")
        self.product = {
            "README.md", "README.ru.md", "LICENSE-APACHE-2.0", "SECURITY.md",
            "CONTRIBUTING.md", "CONTRIBUTING.ru.md", "CHANGELOG.md", "docs/API.md",
            "docs/ARCHITECTURE.md", "docs/SCHEMAS.md",
            "docs/images/architecture.svg", "docs/images/architecture.ru.svg",
            "docs/images/segment-anatomy.svg", "docs/images/segment-anatomy.ru.svg",
            "docs/images/nested-slots.svg", "docs/images/nested-slots.ru.svg",
        }
        self.forbidden = {
            "docs/BENCHMARK.md", "docs/BENCHMARK.ru.md", "docs/benchmark-results/run.json",
            "docs/tmp-plan.md", "docs/.DS_Store", "docs/images/.DS_Store",
            "docs/images/unreviewed.svg", ".agents/notes.md", ".codex/config.toml",
            "vendor/source.rs", "crates/svindb-bench/baseline-macos-arm64.json",
            "README-internal.md", "CHANGELOG-draft.md", "SECURITY-local.md",
        }
        for relative in self.product | self.forbidden | {"packaging/README-linux-x86_64.md"}:
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(relative + "\n", encoding="utf-8")
        self.binary = self.root / "fake-binary"
        self.binary.write_bytes(b"release binary fixture\n")
        self.binary.chmod(0o755)
        self.bin_dir = Path(self.temp.name) / "bin"
        self.bin_dir.mkdir()
        file = self.bin_dir / "file"
        file.write_text("#!/bin/sh\nprintf 'ELF 64-bit executable, x86-64\\n'\n")
        file.chmod(0o755)

    def package(self, destination):
        env = {
            **os.environ, "PATH": f"{self.bin_dir}:{os.environ['PATH']}",
            "SOURCE_DATE_EPOCH": "1700000000", "SVINDB_BUILD_REVISION": "abc123",
        }
        return subprocess.run(
            ["sh", str(self.root / "tools/package_release.sh"), "3.0.0-alpha.4",
             "linux-x86_64", str(self.binary), str(destination)],
            env=env, text=True, capture_output=True,
        )

    def test_exact_product_allowlist_and_checksums_in_both_formats(self):
        output = Path(self.temp.name) / "out"
        result = self.package(output)
        self.assertEqual(result.returncode, 0, result.stderr)
        name = "svindb-v3.0.0-alpha.4-linux-x86_64"
        expected = self.product | {"svindb", "QUICKSTART.md", "BUILD-INFO.json", "SHA256SUMS"}
        with tarfile.open(output / (name + ".tar.gz")) as archive:
            files = {
                str(Path(member.name).relative_to(name)): archive.extractfile(member).read()
                for member in archive.getmembers() if member.isfile()
            }
            mode = archive.getmember(name + "/svindb").mode
        self.assertEqual(set(files), expected)
        self.assertEqual(mode, 0o755)
        self.assertEqual(json.loads(files["BUILD-INFO.json"])["version"], "3.0.0-alpha.4")
        for line in files["SHA256SUMS"].decode().splitlines():
            digest, relative = line.split(maxsplit=1)
            self.assertEqual(hashlib.sha256(files[relative]).hexdigest(), digest)
        with zipfile.ZipFile(output / (name + ".zip")) as archive:
            zipped = {str(Path(path).relative_to(name)): archive.read(path) for path in archive.namelist()}
            self.assertEqual((archive.getinfo(name + "/svindb").external_attr >> 16) & 0o777, 0o755)
        self.assertEqual(zipped, files)

    def test_missing_required_document_prevents_any_archive(self):
        (self.root / "docs/API.md").unlink()
        output = Path(self.temp.name) / "missing-document"
        result = self.package(output)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("обязательный файл поставки отсутствует", result.stderr)
        self.assertFalse(output.exists())

    def test_archives_are_reproducible_and_existing_outputs_are_not_overwritten(self):
        outputs = [Path(self.temp.name) / name for name in ("first", "second")]
        for output in outputs:
            result = self.package(output)
            self.assertEqual(result.returncode, 0, result.stderr)
        for first in outputs[0].iterdir():
            self.assertEqual(first.read_bytes(), (outputs[1] / first.name).read_bytes())
        result = self.package(outputs[0])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("перезапись запрещена", result.stderr)


if __name__ == "__main__":
    unittest.main()
