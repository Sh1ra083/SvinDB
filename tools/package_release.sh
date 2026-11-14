#!/bin/sh
# Собирает один платформенный архив из уже проверенного release-бинарника.
# Содержимое здесь является публичным договором поставки и проверяется CI.

set -eu

if [ "$#" -ne 4 ]; then
    echo "использование: $0 <версия без v> <macos-arm64|linux-x86_64> <бинарник> <каталог результата>" >&2
    exit 2
fi

version=$1
platform=$2
binary=$3
output=$4
epoch=${SOURCE_DATE_EPOCH:-0}
revision=${SVINDB_BUILD_REVISION:-unknown}
features=${SVINDB_FEATURES:-default}

# Значение попадает в JSON-манифест. Ограничиваем его синтаксисом списка Cargo
# features, чтобы внешний env не мог сделать BUILD-INFO.json невалидным.
case "$features" in
    '' ) features=default ;;
    *[!A-Za-z0-9_.+,-]* )
        echo "SVINDB_FEATURES содержит недопустимые символы: $features" >&2
        exit 2
        ;;
esac

case "$epoch" in
    ''|*[!0-9]*)
        echo "SOURCE_DATE_EPOCH обязан быть целым числом: $epoch" >&2
        exit 2
        ;;
esac

if ! printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$'; then
    echo "версия должна иметь вид 1.2.3 без префикса v: $version" >&2
    exit 2
fi

case "$platform" in
    macos-arm64|linux-x86_64) ;;
    *)
        echo "неподдерживаемая платформа релиза: $platform" >&2
        exit 2
        ;;
esac

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if [ ! -f "$binary" ] || [ ! -x "$binary" ]; then
    echo "release-бинарник не найден или не исполняемый: $binary" >&2
    exit 1
fi

kind=$(file -b "$binary")
case "$platform:$kind" in
    macos-arm64:*Mach-O*arm64*) ;;
    linux-x86_64:*ELF*x86-64*) ;;
    *)
        echo "бинарник не соответствует платформе $platform: $kind" >&2
        exit 1
        ;;
esac

for required in README.md LICENSE-APACHE-2.0 docs/API.md docs/ARCHITECTURE.md docs/SCHEMAS.md; do
    if [ ! -f "$root/$required" ]; then
        echo "обязательный файл поставки отсутствует: $required" >&2
        exit 1
    fi
done

mkdir -p "$output"
output=$(CDPATH= cd -- "$output" && pwd)
name="svindb-v${version}-${platform}"
tarball="$output/$name.tar.gz"
zipball="$output/$name.zip"
if [ -e "$tarball" ] || [ -e "$zipball" ]; then
    echo "архив этой версии уже существует; перезапись запрещена: $name" >&2
    exit 1
fi

stage=$(mktemp -d "${TMPDIR:-/tmp}/svindb-package.XXXXXX")
verify=$(mktemp -d "${TMPDIR:-/tmp}/svindb-package-check.XXXXXX")
cleanup() {
    rm -rf -- "$stage" "$verify"
}
trap cleanup EXIT HUP INT TERM

bundle="$stage/$name"
mkdir -p "$bundle"
install -m 0755 "$binary" "$bundle/svindb"
# Положительный список: новые отчёты и служебные файлы не становятся
# публичной поставкой автоматически при появлении в docs/.
for document in "$root"/README.md "$root"/README.ru.md \
    "$root"/LICENSE-APACHE-2.0 "$root"/SECURITY.md \
    "$root"/CONTRIBUTING.md "$root"/CONTRIBUTING.ru.md "$root"/CHANGELOG.md \
    "$root"/docs/API.md "$root"/docs/ARCHITECTURE.md "$root"/docs/SCHEMAS.md \
    "$root"/docs/images/architecture.svg "$root"/docs/images/architecture.ru.svg \
    "$root"/docs/images/segment-anatomy.svg "$root"/docs/images/segment-anatomy.ru.svg \
    "$root"/docs/images/nested-slots.svg "$root"/docs/images/nested-slots.ru.svg; do
    [ -f "$document" ] || continue
    relative=${document#"$root"/}
    mkdir -p "$bundle/$(dirname "$relative")"
    install -m 0644 "$document" "$bundle/$relative"
done
install -m 0644 "$root/packaging/README-$platform.md" "$bundle/QUICKSTART.md"
printf '{"version":"%s","revision":"%s","target":"%s","features":"%s","source_date_epoch":%s}\n' \
    "$version" "$revision" "$platform" "$features" "$epoch" > "$bundle/BUILD-INFO.json"

# Finder metadata и extended attributes не относятся к продукту и не должны
# делать два архива из одного дерева побайтово разными.
find "$bundle" -type f -name .DS_Store -delete
if command -v xattr >/dev/null 2>&1; then
    xattr -cr "$bundle"
fi

checksum() {
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1"
    else
        sha256sum "$1"
    fi
}

(
    cd "$bundle"
    find . -type f ! -name SHA256SUMS -print | LC_ALL=C sort | while IFS= read -r path; do
        checksum "${path#./}"
    done
) > "$bundle/SHA256SUMS"

# Python stdlib одинаково доступна на Linux/macOS: GNU tar и touch -d
# отсутствуют в штатной macOS. Оба формата имеют фиксированные timestamps,
# порядок и права; gzip не сохраняет имя временного файла.
python3 - "$stage" "$name" "$tarball" "$zipball" "$epoch" <<'PYARCHIVE'
import gzip
import stat
import sys
import tarfile
import zipfile
from pathlib import Path

stage, name, tarball, zipball, epoch = sys.argv[1:]
epoch = int(epoch)
root = Path(stage) / name
paths = [root, *sorted(root.rglob("*"))]
with open(tarball, "wb") as destination:
    with gzip.GzipFile(filename="", mode="wb", fileobj=destination, mtime=epoch) as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
            for path in paths:
                info = archive.gettarinfo(str(path), str(path.relative_to(stage)))
                info.uid = info.gid = 0
                info.uname = info.gname = ""
                info.mtime = epoch
                info.mode = 0o755 if path.is_dir() or path.name == "svindb" else 0o644
                if path.is_file():
                    with path.open("rb") as source:
                        archive.addfile(info, source)
                else:
                    archive.addfile(info)
with zipfile.ZipFile(zipball, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
    for path in paths:
        if not path.is_file():
            continue
        info = zipfile.ZipInfo(str(path.relative_to(stage)), date_time=(1980, 1, 1, 0, 0, 0))
        info.create_system = 3
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = (stat.S_IFREG | (0o755 if path.name == "svindb" else 0o644)) << 16
        archive.writestr(info, path.read_bytes())
PYARCHIVE

tar -xzf "$tarball" -C "$verify"
(
    cd "$verify/$name"
    if command -v shasum >/dev/null 2>&1; then
        shasum -a 256 -c SHA256SUMS
    else
        sha256sum -c SHA256SUMS
    fi
)
if command -v unzip >/dev/null 2>&1; then
    unzip -tqq "$zipball"
else
    python3 - "$zipball" <<'PY'
import sys
import zipfile

with zipfile.ZipFile(sys.argv[1]) as archive:
    bad = archive.testzip()
if bad is not None:
    raise SystemExit(f"повреждённый файл в zip: {bad}")
PY
fi

for required in svindb README.md LICENSE-APACHE-2.0 BUILD-INFO.json SHA256SUMS docs/API.md; do
    if [ ! -f "$verify/$name/$required" ]; then
        echo "архив не содержит обязательный файл: $required" >&2
        exit 1
    fi
done

echo "$tarball"
echo "$zipball"
