#!/bin/bash
set -eux

md5name () {
    local base=${1##*/}
    local ext=${base##*.}
    local dir=${1%/*}
    printf '%s' "${base%.$ext}" | gmd5sum |
    awk -v dir="$dir" -v base="$base" '{ printf("%s_%s\n", $1, base) }'
}

mkdir -p target
cp cloudkarafka-mgmt.linux target/cloudkarafka-mgmt.linux
cp -r static target/static

while IFS= read -r -d '' pathname; do
    test -f "$pathname" || continue
    hashed=$(md5name "$pathname")
    cp "$pathname" "${pathname%/*}/$hashed"
    find target/static -type f -name '*.html' | xargs \
      sed -i '' -e "s/${pathname##*/}/$hashed/g"
done < <(find target/static/js -type f -name '*.js' -print0)

tar -czf cloudkarafka-mgmt.tar.gz target
