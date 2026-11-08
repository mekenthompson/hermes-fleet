#!/usr/bin/env bash
# Download one pinned GitHub release asset and verify it before use.
#
#   fetch-pinned-release-asset.sh OWNER/REPO ASSET_ID SIZE SHA256 FALLBACK_URL OUTPUT
#
# Primary route: GitHub's REST asset endpoint
#   https://api.github.com/repos/OWNER/REPO/releases/assets/ASSET_ID
# with Accept: application/octet-stream (and GH_TOKEN when set). Fallback:
# the release's browser download URL on github.com. Both routes are official
# GitHub endpoints and both retry; github.com/.../releases/download can return
# HTTP 504 for long stretches while the API keeps serving the same bytes.
#
# Fail-closed: OUTPUT is written only after the downloaded file has exactly
# the pinned byte size and sha256. A mismatch on either route, or neither
# route answering, exits non-zero and leaves no OUTPUT behind.
set -euo pipefail

if [ "$#" -ne 6 ]; then
  echo "usage: $0 OWNER/REPO ASSET_ID SIZE SHA256 FALLBACK_URL OUTPUT" >&2
  exit 2
fi
repo=$1 asset_id=$2 size=$3 sha256=$4 fallback_url=$5 output=$6

case "$repo" in */*) ;; *) echo "bad repo: $repo" >&2; exit 2 ;; esac
[[ "$asset_id" =~ ^[0-9]+$ ]] || { echo "bad asset id: $asset_id" >&2; exit 2; }
[[ "$size" =~ ^[0-9]+$ ]] || { echo "bad size: $size" >&2; exit 2; }
[[ "$sha256" =~ ^[0-9a-f]{64}$ ]] || { echo "bad sha256: $sha256" >&2; exit 2; }
case "$fallback_url" in
  "https://github.com/$repo/releases/download/"*) ;;
  *) echo "fallback must be a github.com release URL of $repo: $fallback_url" >&2; exit 2 ;;
esac

api_base=${GITHUB_API_URL:-https://api.github.com}
api_url="$api_base/repos/$repo/releases/assets/$asset_id"
partial="$output.partial"
rm -f "$output" "$partial"

verify() {
  local got_size got_sha
  got_size=$(stat -c %s "$partial")
  got_sha=$(sha256sum "$partial" | awk '{print $1}')
  if [ "$got_size" != "$size" ] || [ "$got_sha" != "$sha256" ]; then
    echo "::warning::$1 returned $got_size bytes sha256 $got_sha; pinned $size bytes sha256 $sha256" >&2
    return 1
  fi
}

fetch() {
  # $1 label, rest curl args
  local label=$1; shift
  rm -f "$partial"
  if curl --fail --location --silent --show-error \
       --retry 5 --retry-all-errors --retry-delay 5 --retry-max-time 300 \
       --connect-timeout 20 --max-time 600 \
       --output "$partial" "$@"; then
    if verify "$label"; then
      mv "$partial" "$output"
      echo "Fetched $repo asset $asset_id via $label: $size bytes, sha256 $sha256"
      return 0
    fi
  else
    echo "::warning::$label download of $repo asset $asset_id failed" >&2
  fi
  rm -f "$partial"
  return 1
}

auth=()
if [ -n "${GH_TOKEN:-}" ]; then
  auth=(--header "Authorization: Bearer $GH_TOKEN")
fi

fetch "GitHub REST asset API" \
  --header "Accept: application/octet-stream" \
  --header "X-GitHub-Api-Version: 2022-11-28" \
  "${auth[@]}" "$api_url" && exit 0
fetch "github.com release download" "$fallback_url" && exit 0

echo "::error::Could not fetch a verified copy of $repo asset $asset_id from either route" >&2
exit 1
