#!/bin/bash
set -eux

md5name () {
    local base=${1##*/}
    local ext=${base##*.}
    local dir=${1%/*}
    printf '%s' "${base%.$ext}" | md5sum |
    awk -v dir="$dir" -v base="$base" '{ printf("%s_%s\n", $1, base) }'
}

TEMP_DIR=target
mkdir -p "$TEMP_DIR"
cp -r static "$TEMP_DIR/static"
cp -r templates "$TEMP_DIR/templates"
rm "$TEMP_DIR/templates/templates.go"

while IFS= read -r -d '' pathname; do
    test -f "$pathname" || continue
    hashed=$(md5name "$pathname")
    cp "$pathname" "${pathname%/*}/$hashed"
    find "$TEMP_DIR"/templates/{layouts,pages} -type f -name '*.html' | xargs \
      sed -i -e "s/${pathname##*/}/$hashed/g"
done < <(find "$TEMP_DIR/static" -regextype sed -regex '.*\.\(css\|js\)$' -type f -print0)


