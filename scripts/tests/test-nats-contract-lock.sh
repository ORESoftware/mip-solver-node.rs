#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -P "${script_dir}/../.." && pwd)
checker="${repo_root}/scripts/check-nats-contract-lock.sh"
expected_fingerprint='sha256:2b00ab7021f06527d59d3446927332784657f7f3004af0462eda93f005c1687e'
expected_commit='e90e48f1a14c6e9aa7b6d591bb50d81e61482974'
passed=0

fail() {
  printf 'test-nats-contract-lock: %s\n' "$*" >&2
  exit 1
}

hash_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -- "$1" | awk '{print $1}'
  else
    shasum -a 256 -- "$1" | awk '{print $1}'
  fi
}

new_root() {
  local root
  root=$(mktemp -d "${TMPDIR:-/tmp}/nats-contract-lock.XXXXXX")
  mkdir -p "${root}/vendor/nats-subject-defs/src"
  printf '%s\n' "$root"
}

write_binding() {
  local root=$1
  local fingerprint=${2:-$expected_fingerprint}
  cat > "${root}/vendor/nats-subject-defs/src/lib.rs" <<EOF
// generated fixture
#![allow(clippy::needless_return)]
pub const NATS_CONTRACT_FINGERPRINT: &str = "${fingerprint}";
pub const MIP_SOLVER_REQUESTS_SUBJECT: &str = "dd.remote.mip_solver.requests";
EOF
}

write_lock() {
  local root=$1
  local source_repository=${2:-ORESoftware/k8s-libs-and-shared-defs}
  local source_ref=${3:-main}
  local source_commit=${4:-$expected_commit}
  local source_path=${5:-nats/subject-defs/generated/rust/src/lib.rs}
  local source_blob_sha1=${6:-$(git hash-object -- "${root}/vendor/nats-subject-defs/src/lib.rs")}
  local content_sha256=${7:-$(hash_sha256 "${root}/vendor/nats-subject-defs/src/lib.rs")}
  local contract_fingerprint=${8:-$expected_fingerprint}
  local rust_toolchain=${9:-1.85.0}
  local vendored_path=${10:-vendor/nats-subject-defs/src/lib.rs}
  local extra=${11:-}

  cat > "${root}/vendor/nats-subject-defs/contract.lock" <<EOF
format=ores.nats-contract-lock.v1
source_repository=${source_repository}
source_ref=${source_ref}
source_commit=${source_commit}
source_path=${source_path}
source_blob_sha1=${source_blob_sha1}
content_sha256=${content_sha256}
contract_fingerprint=${contract_fingerprint}
rust_toolchain=${rust_toolchain}
vendored_path=${vendored_path}
EOF
  if [[ -n "$extra" ]]; then
    printf '%b' "$extra" >> "${root}/vendor/nats-subject-defs/contract.lock"
  fi
}

valid_root() {
  local root
  root=$(new_root)
  write_binding "$root"
  write_lock "$root"
  printf '%s\n' "$root"
}

expect_ok() {
  local name=$1
  local root=$2
  NATS_CONTRACT_ROOT="$root" "$checker" > "${root}/result.log" 2>&1 || {
    cat "${root}/result.log" >&2
    fail "expected success: ${name}"
  }
  grep -Fq 'nats-contract-lock: ok' "${root}/result.log" || fail "missing success receipt: ${name}"
  passed=$((passed + 1))
}

expect_fail() {
  local name=$1
  local root=$2
  local message=$3
  if NATS_CONTRACT_ROOT="$root" "$checker" > "${root}/result.log" 2>&1; then
    cat "${root}/result.log" >&2
    fail "expected failure: ${name}"
  fi
  grep -Fq -- "$message" "${root}/result.log" || {
    cat "${root}/result.log" >&2
    fail "wrong failure for ${name}; expected '${message}'"
  }
  passed=$((passed + 1))
}

root=$(valid_root)
expect_ok 'valid lock' "$root"

root=$(new_root)
write_binding "$root"
expect_fail 'missing lock' "$root" 'missing lock file'

root=$(valid_root)
printf '\n' >> "${root}/vendor/nats-subject-defs/contract.lock"
expect_fail 'blank line' "$root" 'blank line'

root=$(valid_root)
printf 'source_ref=main\n' >> "${root}/vendor/nats-subject-defs/contract.lock"
expect_fail 'duplicate key' "$root" "duplicate key 'source_ref'"

root=$(valid_root)
printf 'unreviewed_key=value\n' >> "${root}/vendor/nats-subject-defs/contract.lock"
expect_fail 'unknown key' "$root" "unknown key 'unreviewed_key'"

root=$(new_root)
write_binding "$root"
write_lock "$root" 'other-owner/other-repo'
expect_fail 'wrong source repository' "$root" 'unexpected source repository'

root=$(new_root)
write_binding "$root"
write_lock "$root" 'ORESoftware/k8s-libs-and-shared-defs' 'feature-branch'
expect_fail 'mutable alternate source ref' "$root" 'source ref must remain main'

root=$(new_root)
write_binding "$root"
write_lock "$root" 'ORESoftware/k8s-libs-and-shared-defs' 'main' 'main'
expect_fail 'non-commit source identity' "$root" 'source_commit must be a lowercase 40-character Git SHA'

root=$(new_root)
write_binding "$root"
write_lock "$root" 'ORESoftware/k8s-libs-and-shared-defs' 'main' "$expected_commit" 'generated/rust/src/lib.rs'
expect_fail 'wrong canonical path' "$root" 'unexpected canonical source path'

root=$(new_root)
write_binding "$root"
write_lock "$root" 'ORESoftware/k8s-libs-and-shared-defs' 'main' "$expected_commit" 'nats/subject-defs/generated/rust/src/lib.rs' '' '' "$expected_fingerprint" '1.85.0' '../outside.rs'
expect_fail 'wrong vendored path' "$root" 'unexpected vendored path'

root=$(new_root)
write_binding "$root"
write_lock "$root" 'ORESoftware/k8s-libs-and-shared-defs' 'main' "$expected_commit" 'nats/subject-defs/generated/rust/src/lib.rs' '' 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
expect_fail 'content digest mismatch' "$root" 'content SHA-256 mismatch'

root=$(new_root)
write_binding "$root"
write_lock "$root" 'ORESoftware/k8s-libs-and-shared-defs' 'main' "$expected_commit" 'nats/subject-defs/generated/rust/src/lib.rs' 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
expect_fail 'Git blob mismatch' "$root" 'Git blob mismatch'

root=$(new_root)
write_binding "$root"
write_lock "$root" 'ORESoftware/k8s-libs-and-shared-defs' 'main' "$expected_commit" 'nats/subject-defs/generated/rust/src/lib.rs' '' '' 'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
expect_fail 'embedded fingerprint mismatch' "$root" 'embedded NATS contract fingerprint does not match the lock'

root=$(new_root)
write_binding "$root"
write_lock "$root" 'ORESoftware/k8s-libs-and-shared-defs' 'main' "$expected_commit" 'nats/subject-defs/generated/rust/src/lib.rs' '' '' "$expected_fingerprint" 'stable'
expect_fail 'unpinned Rust toolchain' "$root" 'rust_toolchain must be an exact semantic version'

root=$(valid_root)
printf 'tampered\n' >> "${root}/vendor/nats-subject-defs/src/lib.rs"
expect_fail 'tampered binding' "$root" 'content SHA-256 mismatch'

root=$(valid_root)
cp "${root}/vendor/nats-subject-defs/contract.lock" "${root}/reviewed.lock"
: > "${root}/vendor/nats-subject-defs/contract.lock"
ln -s "${root}/reviewed.lock" "${root}/linked.lock"
NATS_CONTRACT_LOCK_FILE=linked.lock expect_fail 'symlink lock' "$root" 'lock file must not be a symbolic link'

root=$(new_root)
write_binding "$root"
write_lock "$root"
cp "${root}/vendor/nats-subject-defs/src/lib.rs" "${root}/binding.rs"
: > "${root}/vendor/nats-subject-defs/src/lib.rs"
ln -s "${root}/binding.rs" "${root}/linked-binding.rs"
write_lock "$root" 'ORESoftware/k8s-libs-and-shared-defs' 'main' "$expected_commit" 'nats/subject-defs/generated/rust/src/lib.rs' '' '' "$expected_fingerprint" '1.85.0' 'linked-binding.rs'
expect_fail 'symlink or alternate binding path' "$root" 'unexpected vendored path'

root=$(new_root)
write_binding "$root"
write_lock "$root"
printf 'format=ores.nats-contract-lock.v1\r\n' > "${root}/vendor/nats-subject-defs/contract.lock"
expect_fail 'CRLF ambiguity' "$root" 'carriage return'

root=$(new_root)
write_binding "$root"
write_lock "$root"
{
  printf 'vendored_path=vendor/nats-subject-defs/src/lib.rs\n'
  printf 'rust_toolchain=1.85.0\n'
  printf 'contract_fingerprint=%s\n' "$expected_fingerprint"
  printf 'content_sha256=%s\n' "$(hash_sha256 "${root}/vendor/nats-subject-defs/src/lib.rs")"
  printf 'source_blob_sha1=%s\n' "$(git hash-object -- "${root}/vendor/nats-subject-defs/src/lib.rs")"
  printf 'source_path=nats/subject-defs/generated/rust/src/lib.rs\n'
  printf 'source_commit=%s\n' "$expected_commit"
  printf 'source_ref=main\n'
  printf 'source_repository=ORESoftware/k8s-libs-and-shared-defs\n'
  printf 'format=ores.nats-contract-lock.v1\n'
} > "${root}/vendor/nats-subject-defs/contract.lock"
expect_ok 'order-independent lock' "$root"

printf 'test-nats-contract-lock: %d tests passed\n' "$passed"
