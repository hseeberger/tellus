set shell := ["bash", "-uc"]

nightly := `rustc --version | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' | sed 's/^/nightly-/'`

# Flag a benchmark as regressed once we are 95% confident it is at least this fraction slower.
bench_regression_threshold := "0.15"

check:
    cargo check -p ferrier --all-targets
    cargo check -p ferrier --all-targets --features serde

fix:
    cargo fix -p ferrier --all-targets --allow-dirty --allow-staged

fmt:
    cargo +{{ nightly }} fmt
    RUST_LOG=error taplo fmt

fmt-check:
    cargo +{{ nightly }} fmt --check

lint:
    cargo clippy -p ferrier --all-targets --no-deps                  -- -D warnings
    cargo clippy -p ferrier --all-targets --no-deps --features serde -- -D warnings

lint-fix:
    cargo clippy -p ferrier --all-targets --no-deps --allow-dirty --allow-staged --fix

test:
    cargo test -p ferrier
    cargo test -p ferrier --features serde

doc:
    RUSTDOCFLAGS="-D warnings" cargo +{{ nightly }} doc -p ferrier --no-deps --all-features

all: check fmt lint test doc

bench:
    cargo bench -p ferrier

bench-save baseline:
    cargo bench -p ferrier --bench messaging -- --save-baseline {{ baseline }}

bench-compare baseline:
    cargo bench -p ferrier --bench messaging -- --baseline-lenient {{ baseline }}

bench-bencher:
    cargo bench -p ferrier --bench messaging -- --output-format bencher

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

comparison:
    cargo bench -p ferrier-comparison --bench frameworks

comparison-report tag="local":
    cargo run -p ferrier-comparison --bin report -- --tag {{ tag }}

comparison-check:
    cargo check -p ferrier-comparison --all-targets

comparison-lint:
    cargo clippy -p ferrier-comparison --all-targets --no-deps -- -D warnings

run-examples-hello:
    cargo run -p ferrier --example hello

run-examples-counter:
    cargo run -p ferrier --example counter

run-examples-scatter-gather:
    RUST_LOG=ferrier=debug cargo run -p ferrier --example scatter_gather

run-examples-supervision:
    RUST_LOG=ferrier=debug cargo run -p ferrier --example supervision

run-examples-work-pulling:
    RUST_LOG=ferrier=debug cargo run -p ferrier --example work_pulling

run-examples-device-manager:
    RUST_LOG=ferrier=debug cargo run -p ferrier --example device_manager
