#!/usr/bin/env bash
# Build this branch's Vector for one target: run the tests that come with the branch's patches,
# build a release binary, check it, and package it as upstream's release tarball is laid out.
#
# Usage:
#   ./eiti/build.sh <target>     # test, build and package one target
#   ./eiti/build.sh --version    # print the upstream version this branch is (Cargo.toml)
#   ./eiti/build.sh --targets    # print the targets this builds, one per line
#   ./eiti/build.sh --help
#
# <target> is x86_64-unknown-linux-gnu or aarch64-unknown-linux-gnu, and the script runs on a Linux
# machine of that architecture: it runs the patched crate's tests and the binary it built, which a
# cross build could not. .github/workflows/eiti-release.yml runs it once per target and publishes
# the tarballs as a release of this repository (eiti/README.md).
#
# Output: ./target/eiti/vector-<version>-<target>.tar.gz, laid out as upstream's release tarball
# (vector-<target>/bin/vector), with Vector's licence files and this branch's README.
#
# Needs git, rustup (the toolchain rust-toolchain.toml pins installs itself), protoc and
# perl + make (the vendored OpenSSL and jemalloc builds), uv (cargo-zigbuild and zig run through
# uvx), and binutils' objdump. cargo-nextest is pinned here and installed under target/ on first use.
set -euo pipefail

# The glibc the binary may require at most: upstream's own floor for its gnu release builds.
# cargo-zigbuild links against exactly this version, whatever glibc the build machine has.
GLIBC_FLOOR="2.28"
CARGO_ZIGBUILD_VERSION="0.23.4"
ZIG_VERSION="0.17.0"
# The test runner upstream runs its suite with, at the version upstream pins for this release
# (scripts/environment/prepare.sh).
CARGO_NEXTEST_VERSION="0.9.95"

TARGETS=(x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu)

# The components the build holds: the ones the hosts that run this build configure. A config
# naming a component outside this set fails `vector validate` on the built binary.
FEATURES=(
	api api-client
	sources-file sources-journald sources-exec sources-host_metrics sources-internal_metrics
	sources-http_server
	transforms-remap transforms-route transforms-aggregate transforms-filter
	transforms-log_to_metric transforms-metric_to_log
	sinks-aws_s3 sinks-gcp sinks-console sinks-prometheus
	vrl/stdlib
	tikv-jemallocator
)

# The tests that come with this branch's patches (eiti/README.md names the patch each belongs to),
# by the name nextest gives them. Each must exist and pass before anything is built: a patch that
# still applies after a move to a new upstream version, and no longer does what it is for, fails
# here.
PATCH_TESTS=(
	"variants::disk_v2::tests::acknowledgements::replayed_records_do_not_acknowledge_records_read_after_reopen"
	"variants::disk_v2::tests::record::bytes_reported_flushed_have_landed_in_the_file"
	"variants::disk_v2::tests::record::a_record_written_past_the_write_buffer_has_landed_when_it_is_reported_flushed"
)
PATCHED_CRATE="vector-buffers"

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_DIR}"

# The upstream version this branch is: the `vector` package's own, which the binary reports.
VERSION="$(awk -F'"' '/^\[package\]/ { in_package = 1; next } /^\[/ { in_package = 0 } in_package && /^version *=/ { print $2; exit }' Cargo.toml)"
if [ -z "${VERSION}" ]; then
	echo "ERROR: Cargo.toml states no [package] version" >&2
	exit 1
fi

case "${1:-}" in
	--version)
		echo "${VERSION}"
		exit 0
		;;
	--targets)
		printf '%s\n' "${TARGETS[@]}"
		exit 0
		;;
	-h | --help)
		# The header comment, whatever its length.
		awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"
		exit 0
		;;
esac

TARGET=""
for known in "${TARGETS[@]}"; do
	if [ "${known}" = "${1:-}" ]; then TARGET="${known}"; fi
done
if [ -z "${TARGET}" ]; then
	echo "ERROR: expected a target (${TARGETS[*]}), --version, --targets or --help" >&2
	exit 2
fi

if [ "$(uname -s)" != "Linux" ] || [ "$(uname -m)" != "${TARGET%%-*}" ]; then
	echo "ERROR: ${TARGET} is built on a Linux machine of its architecture; this is $(uname -s) $(uname -m)" >&2
	exit 1
fi
for tool in git cargo protoc perl make uvx objdump; do
	if ! command -v "${tool}" >/dev/null 2>&1; then
		echo "ERROR: ${tool} not found" >&2
		exit 1
	fi
done

# A build is of a commit: with a file changed or added, the tarball would not be the tree its tag
# names.
if [ -n "$(git status --porcelain)" ]; then
	echo "ERROR: the checkout has uncommitted changes; a build is of a commit" >&2
	git status --short >&2
	exit 1
fi

OUT_DIR="${REPO_DIR}/target/eiti"
mkdir -p "${OUT_DIR}"

# attempt <command…> — run a command that reaches the network up to three times, a few seconds apart.
attempt() {
	local try
	for try in 1 2 3; do
		"$@" && return 0
		[ "${try}" -lt 3 ] && { echo "    attempt ${try} of '$1' failed; retrying" >&2; sleep $((try * 10)); }
	done
	return 1
}

# The patches' tests, run as upstream runs its suite: under nextest, with the repository's own
# .config/nextest.toml (each test in its own process, retried up to three times, ended after two
# minutes). One invocation per test, each refusing to pass when its name matches no test: a test
# renamed or dropped in a move to a new upstream version fails the build instead of being skipped.
TOOLS_DIR="${OUT_DIR}/tools/cargo-nextest-${CARGO_NEXTEST_VERSION}"
if [ ! -x "${TOOLS_DIR}/bin/cargo-nextest" ]; then
	echo ">>> Installing cargo-nextest ${CARGO_NEXTEST_VERSION}"
	attempt cargo install cargo-nextest --version "${CARGO_NEXTEST_VERSION}" --locked --root "${TOOLS_DIR}"
fi
for patch_test in "${PATCH_TESTS[@]}"; do
	echo ">>> Testing ${patch_test}"
	"${TOOLS_DIR}/bin/cargo-nextest" nextest run --package "${PATCHED_CRATE}" --no-tests fail \
		--status-level pass -E "test(=${patch_test})"
done

# Upstream's release profile, with thin LTO in place of fat. Linking the `vector` crate under fat
# LTO takes more than 9 GB of memory on Linux, and a standard GitHub runner has 8 GB; under thin
# LTO the build peaks near 4 GB. The two differ in how far the optimizer inlines across crates,
# not in what the program does.
export CARGO_PROFILE_RELEASE_LTO="thin"

echo ">>> Building vector ${VERSION} for ${TARGET} (glibc ${GLIBC_FLOOR})"
features="$(IFS=,; echo "${FEATURES[*]}")"
# --locked: the dependency versions are this commit's Cargo.lock, never a newer resolution.
uvx --from "cargo-zigbuild==${CARGO_ZIGBUILD_VERSION}" --with "ziglang==${ZIG_VERSION}" \
	cargo-zigbuild zigbuild --release --locked --no-default-features --features "${features}" \
	--bin vector --target "${TARGET}.${GLIBC_FLOOR}"
BIN="${REPO_DIR}/target/${TARGET}/release/vector"

# The binary runs here and reports the upstream version: a host that tells a change of version
# from a rebuild of the same one reads that field.
reported="$("${BIN}" --version | awk '{print $2}')"
if [ "${reported}" != "${VERSION}" ]; then
	echo "ERROR: the built binary reports version '${reported}', not ${VERSION}" >&2
	exit 1
fi

# No symbol needs a glibc newer than the floor, so the binary starts wherever upstream's does.
# (`|| true`: a binary with no versioned glibc symbol leaves `newest` empty for the check below.)
newest="$(objdump -T "${BIN}" | { grep -o 'GLIBC_[0-9.]*' || true; } | sed 's/^GLIBC_//' | sort -V | tail -1)"
if [ -z "${newest}" ] || [ "$(printf '%s\n%s\n' "${newest}" "${GLIBC_FLOOR}" | sort -V | tail -1)" != "${GLIBC_FLOOR}" ]; then
	echo "ERROR: the built binary needs glibc '${newest}', newer than the ${GLIBC_FLOOR} floor" >&2
	exit 1
fi

# Upstream's layout (vector-<target>/bin/vector) and tarball name, so whatever installs upstream's
# release installs this one the same way. The licence files and this branch's README (what was
# changed, and why) travel with the binary they describe.
PKG_ROOT="${OUT_DIR}/package"
PKG="${PKG_ROOT}/vector-${TARGET}"
rm -rf "${PKG_ROOT}"
mkdir -p "${PKG}/bin"
install -m 0755 "${BIN}" "${PKG}/bin/vector"
install -m 0644 LICENSE NOTICE "${PKG}/"
install -m 0644 eiti/README.md "${PKG}/README.eiti.md"
TARBALL="${OUT_DIR}/vector-${VERSION}-${TARGET}.tar.gz"
tar -C "${PKG_ROOT}" --sort=name --owner=0 --group=0 --numeric-owner -czf "${TARBALL}" "vector-${TARGET}"

echo ">>> Built ${TARBALL}"
echo "    $("${BIN}" --version)"
echo "    sha256 $(sha256sum "${TARBALL}" | cut -d' ' -f1)"
