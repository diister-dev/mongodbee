#!/usr/bin/env bash
set -euo pipefail
name="$(node -p "require('./package.json').name")"
version="$(node -p "require('./package.json').version")"
case "$version" in
  *-*) tag="$(printf '%s' "${version#*-}" | cut -d. -f1)" ;;
  *)   tag="latest" ;;
esac
if [ "$(npm view "$name@$version" version 2>/dev/null || true)" = "$version" ]; then
  echo "$name@$version is already published: nothing to stage"
  exit 0
fi
echo "staging $name@$version under dist-tag '$tag'"
log="$(mktemp)"
if npm stage publish --access public --tag "$tag" 2>&1 | tee "$log"; then
  echo "Staged. Release it with: npm stage list && npm stage approve <id>"
elif grep -q "E409" "$log"; then
  echo "$name@$version is already staged: approve it with npm stage list && npm stage approve <id>"
else
  exit 1
fi
