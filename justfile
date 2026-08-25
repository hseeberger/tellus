set shell := ["bash", "-uc"]

nightly := `rustc --version | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' | sed 's/^/nightly-/'`

# A benchmark counts as regressed once we are 95% confident it is at least this fraction slower;
# bench-report flags it and bench-gate fails on it.
bench_regression_threshold := "0.15"

check:
    cargo check -p tellus --all-targets
    cargo check -p tellus --all-targets --features serde
    cargo check -p tellus --all-targets --features hotpath

fix:
    cargo fix -p tellus --all-targets --allow-dirty --allow-staged

fmt:
    cargo +{{ nightly }} fmt
    RUST_LOG=error taplo fmt

fmt-check:
    cargo +{{ nightly }} fmt --check

lint:
    cargo clippy -p tellus --all-targets --no-deps                    -- -D warnings
    cargo clippy -p tellus --all-targets --no-deps --features serde   -- -D warnings
    cargo clippy -p tellus --all-targets --no-deps --features hotpath -- -D warnings

lint-fix:
    cargo clippy -p tellus --all-targets --no-deps --allow-dirty --allow-staged --fix

test:
    cargo test -p tellus
    cargo test -p tellus --features serde

doc:
    RUSTDOCFLAGS="-D warnings" cargo +{{ nightly }} doc -p tellus --no-deps --all-features

all: check fmt lint test doc

bench:
    cargo bench -p tellus

bench-save baseline:
    cargo bench -p tellus --bench messaging -- --save-baseline {{ baseline }}

bench-compare baseline:
    cargo bench -p tellus --bench messaging -- --baseline-lenient {{ baseline }}

bench-bencher:
    cargo bench -p tellus --bench messaging -- --output-format bencher

bench-report:
    #!/usr/bin/env bash
    set -euo pipefail
    threshold={{ bench_regression_threshold }}
    threshold_pct=$(awk -v t="$threshold" 'BEGIN { printf "%.0f", t * 100 }')
    printf '### Benchmark comparison\n\n'
    printf '| Benchmark | Time | Change | Verdict |\n'
    printf '| --- | --- | --- | --- |\n'
    regressed=0
    while IFS= read -r change; do
        dir=${change%/change/estimates.json}
        id=${dir#target/criterion/}
        time=$(jq -r '.mean.point_estimate' "$dir/new/estimates.json")
        read -r pct lower < <(jq -r '.mean.point_estimate, .mean.confidence_interval.lower_bound' "$change" | paste -sd' ')
        verdict=$(awk -v l="$lower" -v t="$threshold" 'BEGIN { print (l > t) ? "⚠️ regressed" : "ok" }')
        [[ $verdict == ok ]] || regressed=1
        awk -v id="$id" -v t="$time" -v p="$pct" -v v="$verdict" \
            'BEGIN { printf "| %s | %.3f ms | %+.1f%% | %s |\n", id, t / 1e6, p * 100, v }'
    done < <(find target/criterion -path '*/change/estimates.json' | sort)
    printf '\n'
    if [[ $regressed -ne 0 ]]; then
        printf '_Regression: 95%% confident a benchmark is at least %s%% slower._\n' "$threshold_pct"
    fi

bench-gate:
    #!/usr/bin/env bash
    set -euo pipefail
    regressed=0
    while IFS= read -r change; do
        id=${change#target/criterion/}
        id=${id%/change/estimates.json}
        lower=$(jq -r '.mean.confidence_interval.lower_bound' "$change")
        if awk -v l="$lower" -v t="{{ bench_regression_threshold }}" 'BEGIN { exit !(l > t) }'; then
            awk -v id="$id" -v l="$lower" 'BEGIN { printf "FAIL: %s regressed, at least %+.1f%% slower\n", id, l * 100 }'
            regressed=1
        fi
    done < <(find target/criterion -path '*/change/estimates.json' | sort)
    if [[ $regressed -eq 0 ]]; then
        echo "ok: no benchmark regressed beyond the threshold"
    fi
    exit $regressed

profile:
    cargo run --release -p tellus --example profile --features hotpath

profile-alloc:
    cargo run --release -p tellus --example profile --features hotpath-alloc

profile-alloc-gate out="target/hotpath/profile.json":
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p "$(dirname "{{ out }}")"
    HOTPATH_OUTPUT_FORMAT=json HOTPATH_OUTPUT_PATH="{{ out }}" just profile-alloc
    just profile-alloc-check "{{ out }}"

# The steady-state messaging path must not allocate per message; "0 B" is exact, not rounded.
profile-alloc-check file:
    #!/usr/bin/env bash
    set -euo pipefail
    failed=0
    for name in tellus::actor_ref::tell tellus::quota::reserve tellus::actor_context::receive_incoming; do
        if jq -e --arg name "$name" \
            '[.functions_alloc.data[] | select(.name == $name)] | length == 1 and .[0].total == "0 B"' \
            "{{ file }}" > /dev/null; then
            echo "ok: $name allocated 0 B"
        else
            echo "FAIL: $name allocated memory or is missing from {{ file }}"
            failed=1
        fi
    done
    exit $failed

comparison:
    cargo bench -p tellus-comparison --bench frameworks

comparison-report tag="local":
    cargo run -p tellus-comparison --bin report -- --tag {{ tag }}

comparison-check:
    cargo check -p tellus-comparison --all-targets

comparison-lint:
    cargo clippy -p tellus-comparison --all-targets --no-deps -- -D warnings

run-examples-hello:
    cargo run -p tellus --example hello

run-examples-counter:
    cargo run -p tellus --example counter

run-examples-scatter-gather:
    RUST_LOG=tellus=debug cargo run -p tellus --example scatter_gather

run-examples-supervision:
    RUST_LOG=tellus=debug cargo run -p tellus --example supervision

run-examples-work-pulling:
    RUST_LOG=tellus=debug cargo run -p tellus --example work_pulling

run-examples-device-manager:
    RUST_LOG=tellus=debug cargo run -p tellus --example device_manager
