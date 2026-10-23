#!/usr/bin/env bash
set -euo pipefail
name="$(node -p "require('./jsr.json').name")"
version="$(node -p "require('./jsr.json').version")"
published() {
  curl -fsS --retry 3 "https://jsr.io/$name/meta.json" | node -e '
    let s = ""; process.stdin.on("data", (c) => (s += c)).on("end", () => process.exit(JSON.parse(s).versions?.[process.argv[1]] ? 0 : 1));
  ' "$version"
}
if published; then
  echo "$name@$version is already on JSR: nothing to publish"
  exit 0
fi
for attempt in 1 2 3; do
  if npx jsr publish; then exit 0; fi
  if published; then echo "$name@$version reached JSR despite the error"; exit 0; fi
  echo "JSR publish failed (attempt $attempt), retrying in $((attempt * 20)) s"
  sleep $((attempt * 20))
done
exit 1
