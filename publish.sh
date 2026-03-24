#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cargo_toml="${repo_root}/Cargo.toml"
cargo_lock="${repo_root}/Cargo.lock"

current_version() {
  awk '
    /^\[package\]$/ { in_package = 1; next }
    /^\[/ { in_package = 0 }
    in_package && /^version = "/ {
      gsub(/^version = "/, "", $0)
      gsub(/"$/, "", $0)
      print
      exit
    }
  ' "${cargo_toml}"
}

increment_version() {
  local version="$1"
  local -a parts=()

  IFS='.' read -r -a parts <<< "${version}"
  if [[ "${#parts[@]}" -eq 0 ]]; then
    echo "invalid version: ${version}" >&2
    exit 1
  fi

  for part in "${parts[@]}"; do
    if [[ ! "${part}" =~ ^[0-9]+$ ]]; then
      echo "version must contain only numeric dot-separated components: ${version}" >&2
      exit 1
    fi
  done

  local last_index=$(( ${#parts[@]} - 1 ))
  parts[${last_index}]=$(( 10#${parts[${last_index}]} + 1 ))

  local next_version="${parts[0]}"
  local i
  for (( i = 1; i < ${#parts[@]}; i++ )); do
    next_version="${next_version}.${parts[i]}"
  done

  printf '%s\n' "${next_version}"
}

update_version() {
  local new_version="$1"
  local temp_file
  temp_file="$(mktemp)"

  awk -v new_version="${new_version}" '
    BEGIN { in_package = 0; updated = 0 }
    /^\[package\]$/ { in_package = 1; print; next }
    /^\[/ { in_package = 0 }
    in_package && !updated && /^version = "/ {
      print "version = \"" new_version "\""
      updated = 1
      next
    }
    { print }
    END {
      if (!updated) {
        exit 1
      }
    }
  ' "${cargo_toml}" > "${temp_file}"

  mv "${temp_file}" "${cargo_toml}"
}

validate_version() {
  local version="$1"

  if [[ ! "${version}" =~ ^[0-9]+(\.[0-9]+)+$ ]]; then
    echo "version must look like 0.1.1: ${version}" >&2
    exit 1
  fi
}

main() {
  cd "${repo_root}"

  local old_version
  old_version="$(current_version)"
  if [[ -z "${old_version}" ]]; then
    echo "failed to read version from Cargo.toml" >&2
    exit 1
  fi

  local new_version
  if [[ $# -ge 1 ]]; then
    new_version="$1"
    validate_version "${new_version}"
  else
    new_version="$(increment_version "${old_version}")"
  fi

  if [[ "${new_version}" == "${old_version}" ]]; then
    echo "new version matches current version: ${new_version}" >&2
    exit 1
  fi

  if git rev-parse -q --verify "refs/tags/v${new_version}" >/dev/null 2>&1; then
    echo "tag already exists: v${new_version}" >&2
    exit 1
  fi

  update_version "${new_version}"
  cargo check >/dev/null

  if [[ ! -f "${cargo_lock}" ]]; then
    echo "Cargo.lock was not generated" >&2
    exit 1
  fi

  git add Cargo.toml Cargo.lock
  git commit -m "release ${new_version}" -- Cargo.toml Cargo.lock
  git tag "v${new_version}"
  git push origin "v${new_version}"
}

main "$@"
