#!/usr/bin/env python3
"""Проверяет, что бинарник взят из linux/amd64 OCI-образа с теми же слоями."""

import hashlib
import json
import sys
import tarfile
from io import BytesIO
from pathlib import PurePosixPath


def fail(message):
    raise ValueError(message)


def blob(blobs, descriptor):
    digest = descriptor.get("digest", "")
    if not digest.startswith("sha256:"):
        fail(f"неподдерживаемый digest OCI: {digest}")
    data = blobs.get(digest[7:])
    if data is None or hashlib.sha256(data).hexdigest() != digest[7:]:
        fail(f"digest OCI blob не совпадает: {digest}")
    return data


def main(archive, binary):
    with tarfile.open(archive, "r:") as outer:
        members = {}
        for member in outer.getmembers():
            if member.name in members:
                fail(f"дублирующийся путь в OCI archive: {member.name}")
            if member.name.startswith("/") or ".." in PurePosixPath(member.name).parts:
                fail(f"небезопасный путь в OCI archive: {member.name}")
            if member.isfile():
                members[member.name] = outer.extractfile(member).read()
    blobs = {name.rsplit("/", 1)[-1]: data for name, data in members.items()
             if name.startswith("blobs/sha256/")}
    try:
        index = json.loads(members["index.json"])
    except (KeyError, json.JSONDecodeError) as exc:
        fail(f"некорректный index.json OCI: {exc}")
    def resolve(descriptors, depth=0, seen=None):
        if depth > 8:
            fail("вложенные OCI index превышают допустимую глубину")
        seen = set() if seen is None else seen
        result = []
        for descriptor in descriptors:
            digest = descriptor.get("digest", "")
            if digest in seen:
                fail("цикл во вложенных OCI index")
            data = blob(blobs, descriptor)
            try:
                document = json.loads(data)
            except json.JSONDecodeError as exc:
                fail(f"некорректный OCI manifest/index: {exc}")
            if "config" in document and "layers" in document:
                result.append((descriptor, document))
            elif "manifests" in document:
                result.extend(resolve(document["manifests"], depth + 1, seen | {digest}))
            else:
                fail("OCI descriptor не является manifest или index")
        return result

    candidates = []
    for descriptor, manifest in resolve(index.get("manifests", [])):
        platform = descriptor.get("platform")
        if platform and (platform.get("os"), platform.get("architecture")) != ("linux", "amd64"):
            continue
        config = blob(blobs, manifest.get("config", {}))
        try:
            config_document = json.loads(config)
        except json.JSONDecodeError as exc:
            fail(f"некорректный config OCI: {exc}")
        if (config_document.get("os"), config_document.get("architecture")) == ("linux", "amd64"):
            candidates.append(manifest)
    if len(candidates) != 1:
        fail("OCI-архив должен содержать ровно один image config linux/amd64")
    manifest = candidates[0]
    found = []
    for descriptor in manifest.get("layers", []):
        data = blob(blobs, descriptor)
        try:
            with tarfile.open(fileobj=BytesIO(data), mode="r:*") as layer:
                for member in layer:
                    if member.name.startswith("/") or ".." in PurePosixPath(member.name).parts:
                        fail(f"небезопасный путь в OCI layer: {member.name}")
                    if member.name.rsplit("/", 1)[-1].startswith(".wh."):
                        fail("whiteout в OCI layer не поддерживается")
                    if PurePosixPath(member.name) == PurePosixPath("usr/local/bin/svindb"):
                        if not member.isfile():
                            fail("usr/local/bin/svindb в OCI layer не является файлом")
                        found.append(layer.extractfile(member).read())
        except (tarfile.TarError, EOFError) as exc:
            fail(f"неподдерживаемый формат OCI layer: {exc}")
    if len(found) != 1:
        fail("в OCI layers должен быть ровно один usr/local/bin/svindb")
    with open(binary, "rb") as handle:
        actual = handle.read()
    if hashlib.sha256(found[0]).digest() != hashlib.sha256(actual).digest():
        fail("OCI binary не совпадает с извлечённым бинарником")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"использование: {sys.argv[0]} <oci.tar> <бинарник>", file=sys.stderr)
        raise SystemExit(2)
    try:
        main(sys.argv[1], sys.argv[2])
    except (OSError, ValueError) as exc:
        print(f"проверка OCI не пройдена: {exc}", file=sys.stderr)
        raise SystemExit(1)
