import hashlib
import json
import sys
import tarfile
import tempfile
import unittest
from io import BytesIO
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from verify_oci_binary import main


def descriptor(data, media_type="application/octet-stream"):
    return {"mediaType": media_type, "digest": "sha256:" + hashlib.sha256(data).hexdigest(), "size": len(data)}


class VerifyOciBinaryTests(unittest.TestCase):
    def make_archive(self, root, binary=b"binary", platforms=None, tamper=None,
                     config_platform=("linux", "amd64"), nested=False, unsafe_path=None):
        layer_io = BytesIO()
        with tarfile.open(fileobj=layer_io, mode="w") as layer:
            info = tarfile.TarInfo(unsafe_path or "usr/local/bin/svindb")
            info.size = len(binary)
            layer.addfile(info, BytesIO(binary))
        layer = layer_io.getvalue()
        config = json.dumps({"os": config_platform[0], "architecture": config_platform[1]}).encode()
        manifest = json.dumps({"schemaVersion": 2, "config": descriptor(config), "layers": [descriptor(layer)]}).encode()
        blobs = {hashlib.sha256(x).hexdigest(): x for x in (config, layer, manifest)}
        platforms = platforms or [{"os": "linux", "architecture": "amd64"}]
        entries = [dict(descriptor(manifest), platform=p) for p in platforms]
        if nested:
            nested_index = json.dumps({"schemaVersion": 2, "manifests": entries}).encode()
            blobs[hashlib.sha256(nested_index).hexdigest()] = nested_index
            entries = [dict(descriptor(nested_index))]
        index = {"schemaVersion": 2, "manifests": entries}
        archive = root / "image.oci.tar"
        with tarfile.open(archive, "w") as out:
            for digest, data in blobs.items():
                info = tarfile.TarInfo("blobs/sha256/" + digest)
                info.size = len(data)
                out.addfile(info, BytesIO(data))
            data = json.dumps(index).encode()
            info = tarfile.TarInfo("index.json")
            info.size = len(data)
            out.addfile(info, BytesIO(data))
        if tamper:
            with open(archive, "r+b") as handle:
                handle.seek(tamper)
                handle.write(b"X")
        return archive

    def test_matching_binary_and_platform(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = self.make_archive(root, platforms=[{"os": "unknown", "architecture": "unknown"}, {"os": "linux", "architecture": "arm64"}, {"os": "linux", "architecture": "amd64"}])
            binary = root / "svindb"
            binary.write_bytes(b"binary")
            main(archive, binary)

    def test_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = self.make_archive(root)
            binary = root / "svindb"
            binary.write_bytes(b"wrong")
            with self.assertRaises(ValueError):
                main(archive, binary)

    def test_tampered_blob_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = self.make_archive(root, tamper=512)
            binary = root / "svindb"
            binary.write_bytes(b"binary")
            with self.assertRaises(ValueError):
                main(archive, binary)

    def test_ambiguous_platform_manifests_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = self.make_archive(root, platforms=[{"os": "linux", "architecture": "amd64"}, {"os": "linux", "architecture": "amd64"}])
            binary = root / "svindb"
            binary.write_bytes(b"binary")
            with self.assertRaises(ValueError):
                main(archive, binary)

    def test_nested_index_and_config_platform_are_resolved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = self.make_archive(root, nested=True)
            binary = root / "svindb"
            binary.write_bytes(b"binary")
            main(archive, binary)

    def test_mismatched_config_platform_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = self.make_archive(root, config_platform=("linux", "arm64"))
            binary = root / "svindb"
            binary.write_bytes(b"binary")
            with self.assertRaises(ValueError):
                main(archive, binary)

    def test_malicious_layer_path_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = self.make_archive(root, unsafe_path="../../usr/local/bin/svindb")
            binary = root / "svindb"
            binary.write_bytes(b"binary")
            with self.assertRaises(ValueError):
                main(archive, binary)


if __name__ == "__main__":
    unittest.main()
