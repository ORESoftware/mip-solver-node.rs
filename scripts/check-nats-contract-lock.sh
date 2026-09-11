#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)
default_root=$(cd -P "${script_dir}/.." && pwd)
repo_root=${NATS_CONTRACT_ROOT:-$default_root}
lock_relative=${NATS_CONTRACT_LOCK_FILE:-vendor/nats-subject-defs/contract.lock}
lock_file="${repo_root}/${lock_relative}"

fail() {
  printf 'nats-contract-lock: %s\n' "$*" >&2
  exit 1
}

hash_sha256() {
  local file=$1
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -- "$file" | awk '{print $1}'
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 -- "$file" | awk '{print $1}'
    return
  fi
  fail 'neither sha256sum nor shasum is available'
}

[[ -f "$lock_file" ]] || fail "missing lock file: ${lock_relative}"
[[ ! -L "$lock_file" ]] || fail "lock file must not be a symbolic link: ${lock_relative}"

format=''
source_repository=''
source_ref=''
source_commit=''
source_path=''
source_blob_sha1=''
content_sha256=''
contract_fingerprint=''
rust_toolchain=''
vendored_path=''
seen='|'
line_number=0

while IFS= read -r line || [[ -n "$line" ]]; do
  line_number=$((line_number + 1))
  [[ -n "$line" ]] || fail "blank line at ${lock_relative}:${line_number}"
  [[ "$line" != *$'\r'* ]] || fail "carriage return at ${lock_relative}:${line_number}"
  [[ "$line" =~ ^[a-z0-9_]+=[A-Za-z0-9._:/-]+$ ]] || fail "invalid lock syntax at ${lock_relative}:${line_number}"

  key=${line%%=*}
  value=${line#*=}
  case "$seen" in
    *"|${key}|"*) fail "duplicate key '${key}' at ${lock_relative}:${line_number}" ;;
  esac
  seen="${seen}${key}|"

  case "$key" in
    format) format=$value ;;
    source_repository) source_repository=$value ;;
    source_ref) source_ref=$value ;;
    source_commit) source_commit=$value ;;
    source_path) source_path=$value ;;
    source_blob_sha1) source_blob_sha1=$value ;;
    content_sha256) content_sha256=$value ;;
    contract_fingerprint) contract_fingerprint=$value ;;
    rust_toolchain) rust_toolchain=$value ;;
    vendored_path) vendored_path=$value ;;
    *) fail "unknown key '${key}' at ${lock_relative}:${line_number}" ;;
  esac
done < "$lock_file"

for required in \
  format source_repository source_ref source_commit source_path source_blob_sha1 \
  content_sha256 contract_fingerprint rust_toolchain vendored_path; do
  case "$seen" in
    *"|${required}|"*) ;;
    *) fail "missing required key '${required}' in ${lock_relative}" ;;
  esac
done

[[ "$format" == 'ores.nats-contract-lock.v1' ]] || fail "unsupported lock format: ${format}"
[[ "$source_repository" == 'ORESoftware/k8s-libs-and-shared-defs' ]] || fail "unexpected source repository: ${source_repository}"
[[ "$source_ref" == 'main' ]] || fail "source ref must remain main"
[[ "$source_path" == 'nats/subject-defs/generated/rust/src/lib.rs' ]] || fail "unexpected canonical source path: ${source_path}"
[[ "$vendored_path" == 'vendor/nats-subject-defs/src/lib.rs' ]] || fail "unexpected vendored path: ${vendored_path}"
[[ "$source_commit" =~ ^[0-9a-f]{40}$ ]] || fail 'source_commit must be a lowercase 40-character Git SHA'
[[ "$source_blob_sha1" =~ ^[0-9a-f]{40}$ ]] || fail 'source_blob_sha1 must be a lowercase 40-character Git blob SHA'
[[ "$content_sha256" =~ ^[0-9a-f]{64}$ ]] || fail 'content_sha256 must be a lowercase 64-character SHA-256 digest'
[[ "$contract_fingerprint" =~ ^sha256:[0-9a-f]{64}$ ]] || fail 'contract_fingerprint must be sha256:<64 lowercase hex characters>'
[[ "$rust_toolchain" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail 'rust_toolchain must be an exact semantic version'

binding_file="${repo_root}/${vendored_path}"
[[ -f "$binding_file" ]] || fail "missing vendored binding: ${vendored_path}"
[[ ! -L "$binding_file" ]] || fail "vendored binding must not be a symbolic link: ${vendored_path}"

actual_sha256=$(hash_sha256 "$binding_file")
[[ "$actual_sha256" == "$content_sha256" ]] || fail "content SHA-256 mismatch: expected ${content_sha256}, got ${actual_sha256}"

command -v git >/dev/null 2>&1 || fail 'git is required to verify the Git blob identity'
actual_blob_sha1=$(git hash-object -- "$binding_file")
[[ "$actual_blob_sha1" == "$source_blob_sha1" ]] || fail "Git blob mismatch: expected ${source_blob_sha1}, got ${actual_blob_sha1}"

expected_fingerprint_line="pub const NATS_CONTRACT_FINGERPRINT: &str = \"${contract_fingerprint}\";"
grep -Fxq -- "$expected_fingerprint_line" "$binding_file" || fail 'embedded NATS contract fingerprint does not match the lock'

printf 'nats-contract-lock: ok source=%s@%s blob=%s sha256=%s toolchain=%s\n' \
  "$source_repository" "$source_commit" "$source_blob_sha1" "$content_sha256" "$rust_toolchain"
