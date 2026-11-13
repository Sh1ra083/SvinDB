#!/bin/sh
# Воспроизводимая сборка Linux x86_64 binary и OCI archive.

set -eu

if [ "$#" -ne 1 ]; then
    echo "использование: $0 <каталог результата>" >&2
    exit 2
fi

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
output=$1
revision=${SVINDB_BUILD_REVISION:-$(git -C "$root" rev-parse HEAD)}
epoch=${SOURCE_DATE_EPOCH:-$(git -C "$root" show -s --format=%ct HEAD)}
features=${SVINDB_FEATURES:-}
version=$(sed -n 's/^version = "\([^"]*\)"$/\1/p' "$root/crates/svindb-server/Cargo.toml" | head -n 1)

case "$revision" in
    ''|*[!0-9a-fA-F]*)
        echo "SVINDB_BUILD_REVISION обязан быть hex-идентификатором: $revision" >&2
        exit 2
        ;;
esac
if [ "${#revision}" -lt 7 ]; then
    echo "SVINDB_BUILD_REVISION короче 7 символов: $revision" >&2
    exit 2
fi
if ! printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$'; then
    echo "версия svindb-server не разобралась: $version" >&2
    exit 2
fi
case "$epoch" in
    ''|*[!0-9]*)
        echo "SOURCE_DATE_EPOCH обязан быть целым числом: $epoch" >&2
        exit 2
        ;;
esac

rfc3339=$(date -u -d "@$epoch" '+%Y-%m-%dT%H:%M:%SZ')
image="svindb:${version}-${revision}"
oci="$output/svindb-v${version}-linux-x86_64.oci.tar"

mkdir -p "$output"
output=$(CDPATH= cd -- "$output" && pwd)

docker buildx build \
    --platform linux/amd64 \
    --build-arg "SVINDB_BUILD_REVISION=$revision" \
    --build-arg "SVINDB_BUILD_VERSION=$version" \
    --build-arg "SVINDB_FEATURES=$features" \
    --build-arg "SOURCE_DATE_EPOCH=$epoch" \
    --build-arg "SOURCE_DATE_RFC3339=$rfc3339" \
    --output "type=oci,dest=$oci,rewrite-timestamp=true" \
    --tag "$image" \
    "$root"

# OCI-вывод нельзя использовать через `docker cp`: явно загружаем тот же
# исходный код с теми же аргументами в tag перед извлечением. Это исключает
# использование устаревшего локального tag, оставшегося от другой сборки.
docker buildx build \
    --platform linux/amd64 \
    --build-arg "SVINDB_BUILD_REVISION=$revision" \
    --build-arg "SVINDB_BUILD_VERSION=$version" \
    --build-arg "SVINDB_FEATURES=$features" \
    --build-arg "SOURCE_DATE_EPOCH=$epoch" \
    --build-arg "SOURCE_DATE_RFC3339=$rfc3339" \
    --load \
    --tag "$image" \
    "$root"

label_revision=$(docker inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$image")
label_version=$(docker inspect --format '{{index .Config.Labels "org.opencontainers.image.version"}}' "$image")
label_features=$(docker inspect --format '{{index .Config.Labels "org.opencontainers.image.features"}}' "$image")
if [ "$label_revision" != "$revision" ] || [ "$label_version" != "$version" ] || [ "$label_features" != "$features" ]; then
    echo "метки загруженного Docker-образа не соответствуют сборке: revision=$label_revision version=$label_version features=$label_features" >&2
    exit 1
fi

container=$(docker create "$image")
trap 'docker rm -f "$container" >/dev/null 2>&1 || true' EXIT HUP INT TERM
docker cp "$container:/usr/local/bin/svindb" "$output/svindb-v${version}-linux-x86_64"
chmod 0755 "$output/svindb-v${version}-linux-x86_64"
python3 "$root/tools/verify_oci_binary.py" "$oci" "$output/svindb-v${version}-linux-x86_64"

SVINDB_BUILD_REVISION=$revision SOURCE_DATE_EPOCH=$epoch \
    "$root/tools/package_release.sh" \
    "$version" linux-x86_64 "$output/svindb-v${version}-linux-x86_64" "$output"
