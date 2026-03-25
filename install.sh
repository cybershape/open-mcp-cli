#!/usr/bin/env bash

set -euo pipefail

readonly REPOSITORY="${REPOSITORY:-tiejunhu/ones-mcp-cli}"
readonly BINARY_NAME="omc"
readonly RELEASES_BASE="https://github.com/${REPOSITORY}/releases"

log() {
  printf '%s\n' "$*"
}

fatal() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || fatal "missing required command: $1"
}

download_file() {
  local url="$1"
  local output="$2"
  local error_output

  if command -v curl >/dev/null 2>&1; then
    if error_output="$(curl -fsSL --retry 3 --output "$output" "$url" 2>&1)"; then
      return
    fi

    fatal "failed to download ${url}: ${error_output}"
  fi

  if command -v wget >/dev/null 2>&1; then
    if error_output="$(wget -O "$output" "$url" 2>&1)"; then
      return
    fi

    fatal "failed to download ${url}: ${error_output}"
  fi

  fatal "install requires curl or wget"
}

normalize_version() {
  local version="$1"
  if [[ -z "$version" ]]; then
    return
  fi

  if [[ "$version" == v* ]]; then
    printf '%s\n' "$version"
    return
  fi

  printf 'v%s\n' "$version"
}

latest_release_url() {
  printf '%s/latest\n' "$RELEASES_BASE"
}

release_asset_url() {
  local version="$1"
  local asset_name="$2"
  printf '%s/download/%s/%s\n' "$RELEASES_BASE" "$version" "$asset_name"
}

detect_target() {
  local os
  local arch

  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os" in
    Linux)
      os="unknown-linux-gnu"
      ;;
    Darwin)
      os="apple-darwin"
      ;;
    *)
      fatal "unsupported operating system: $os"
      ;;
  esac

  case "$arch" in
    x86_64 | amd64)
      arch="x86_64"
      ;;
    arm64 | aarch64)
      arch="aarch64"
      ;;
    *)
      fatal "unsupported architecture: $arch"
      ;;
  esac

  printf '%s-%s\n' "$arch" "$os"
}

resolve_latest_version() {
  local url
  local final_url
  local response

  url="$(latest_release_url)"

  if command -v curl >/dev/null 2>&1; then
    if final_url="$(curl -fsSL -o /dev/null -w '%{url_effective}' "$url" 2>&1)"; then
      :
    else
      fatal "failed to resolve latest release from ${url}: ${final_url}"
    fi
  elif command -v wget >/dev/null 2>&1; then
    if response="$(wget -S --spider --max-redirect=20 "$url" 2>&1)"; then
      final_url="$(printf '%s\n' "$response" | awk '/^Location: / { print $2 }' | tail -n 1)"
      final_url="${final_url%$'\r'}"
      final_url="${final_url% \[following\]}"
      [[ -n "$final_url" ]] || fatal "failed to resolve latest release from ${url}: missing redirect target"
    else
      fatal "failed to resolve latest release from ${url}: ${response}"
    fi
  else
    fatal "install requires curl or wget"
  fi

  [[ "$final_url" == "${RELEASES_BASE}/tag/"* ]] || fatal "failed to resolve release tag from ${url}: ${final_url}"
  printf '%s\n' "${final_url##*/}"
}

default_install_dir() {
  if [[ -n "${INSTALL_DIR:-}" ]]; then
    printf '%s\n' "${INSTALL_DIR}"
    return
  fi

  if [[ "$(id -u)" -eq 0 ]]; then
    if [[ -d "/opt/homebrew/bin" ]]; then
      printf '/opt/homebrew/bin\n'
      return
    fi

    printf '/usr/local/bin\n'
    return
  fi

  if [[ -d "/opt/homebrew/bin" && -w "/opt/homebrew/bin" ]]; then
    printf '/opt/homebrew/bin\n'
    return
  fi

  if [[ -d "/usr/local/bin" && -w "/usr/local/bin" ]]; then
    printf '/usr/local/bin\n'
    return
  fi

  [[ -n "${HOME:-}" ]] || fatal "HOME is not set; specify INSTALL_DIR explicitly"
  printf '%s/.local/bin\n' "${HOME}"
}

install_binary() {
  local source="$1"
  local destination="$2"

  if command -v install >/dev/null 2>&1; then
    install -m 0755 "$source" "$destination"
    return
  fi

  cp "$source" "$destination"
  chmod 0755 "$destination"
}

path_contains_dir() {
  local dir="$1"
  local entry

  IFS=':' read -r -a path_entries <<< "${PATH:-}"
  for entry in "${path_entries[@]}"; do
    if [[ "$entry" == "$dir" ]]; then
      return 0
    fi
  done

  return 1
}

main() {
  need_cmd tar
  need_cmd mktemp
  need_cmd uname

  local version="${VERSION:-}"
  version="$(normalize_version "$version")"

  local target
  target="$(detect_target)"

  local install_dir
  install_dir="$(default_install_dir)"
  mkdir -p "$install_dir"

  if [[ ! -w "$install_dir" ]]; then
    fatal "install directory is not writable: $install_dir; rerun with sudo or set INSTALL_DIR"
  fi

  local release_tag
  if [[ -n "$version" ]]; then
    release_tag="$version"
  else
    release_tag="$(resolve_latest_version)"
  fi
  [[ -n "$release_tag" ]] || fatal "failed to resolve release tag"

  local asset_name
  asset_name="${BINARY_NAME}-${release_tag}-${target}.tar.gz"

  local download_url
  download_url="$(release_asset_url "$release_tag" "$asset_name")"

  temp_dir="$(mktemp -d)"
  trap 'rm -rf -- "$temp_dir"' EXIT

  local archive_path
  archive_path="${temp_dir}/${asset_name}"

  log "Downloading ${asset_name}"
  download_file "$download_url" "$archive_path"

  tar -xzf "$archive_path" -C "$temp_dir"

  local extracted_binary
  extracted_binary="${temp_dir}/${BINARY_NAME}"
  [[ -f "$extracted_binary" ]] || fatal "archive did not contain ${BINARY_NAME}"

  install_binary "$extracted_binary" "${install_dir}/${BINARY_NAME}"

  log "Installed ${BINARY_NAME} ${release_tag} to ${install_dir}/${BINARY_NAME}"
  if ! path_contains_dir "$install_dir"; then
    log "Warning: ${install_dir} is not in PATH"
    log "Add it to your shell profile before running ${BINARY_NAME} without the full path"
  fi
}

main "$@"
