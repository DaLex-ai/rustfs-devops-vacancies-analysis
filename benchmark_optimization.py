import csv
import math
import os
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent
EVIDENCE = BASE / "evidence"
EVIDENCE.mkdir(parents=True, exist_ok=True)

RESULTS = EVIDENCE / "optimization_benchmark_runs.csv"
DETAIL = EVIDENCE / "optimization_benchmark_detail.txt"
SUMMARY = EVIDENCE / "optimization_benchmark_summary.txt"

# Measured in the first successful preparation run.
PREPARATION_SECONDS = 10.055

COMMANDS = {
    "original": [
        sys.executable,
        str(BASE / "benchmark_compute.py"),
        "--worker",
        "mapreduce",
    ],
    "optimized": [
        sys.executable,
        str(BASE / "optimized_mapreduce.py"),
        "benchmark-worker",
    ],
}

EXPECTED = {
    1: (1560, 28344),
    2: (22311, 134786),
    3: (152803, 507851),
}

all_results = []
summary_lines = []


def report(message=""):
    text = str(message)
    print(text, flush=True)
    summary_lines.append(text)


def extract_value(pattern, text, label):
    match = re.search(pattern, text, re.MULTILINE)

    if not match:
        raise RuntimeError(
            f"Missing {label} in benchmark output"
        )

    return float(match.group(1))


def save_results():
    with RESULTS.open(
        "w", encoding="utf-8-sig", newline=""
    ) as file:
        writer = csv.writer(file)
        writer.writerow([
            "method",
            "attempt",
            "compute_seconds",
            "wall_seconds",
            "peak_working_set_mib",
        ])

        writer.writerows(all_results)


def run_worker(method, attempt):
    environment = os.environ.copy()
    environment["PYTHONIOENCODING"] = "utf-8"

    started = time.perf_counter()

    completed = subprocess.run(
        COMMANDS[method],
        cwd=BASE,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=180,
        check=False,
    )

    wall_seconds = time.perf_counter() - started
    output = completed.stdout

    with DETAIL.open("a", encoding="utf-8") as file:
        file.write(
            f"\n=== {method}, attempt={attempt} ===\n"
        )
        file.write(f"Return code: {completed.returncode}\n")
        file.write(output + "\n")

    if (
        completed.returncode != 0
        or "BENCHMARK_OK" not in output
    ):
        print(output, flush=True)
        raise RuntimeError(
            f"Worker failed: {method}, attempt={attempt}"
        )

    if method == "optimized":
        for k, (unique, occurrences) in EXPECTED.items():
            expected_line = (
                f"Stack {k}: {unique} unique, "
                f"{occurrences} occurrences"
            )

            if expected_line not in output:
                raise AssertionError(
                    f"Missing result verification: {expected_line}"
                )

    compute_seconds = extract_value(
        r"^CORE_SECONDS=([0-9.]+)$",
        output,
        "compute time",
    )

    memory_mib = extract_value(
        r"^PEAK_WORKING_SET_MIB=([0-9.]+)$",
        output,
        "peak memory",
    )

    if compute_seconds <= 0 or memory_mib <= 0:
        raise AssertionError(
            "Invalid performance measurement"
        )

    result = (
        method,
        attempt,
        round(compute_seconds, 3),
        round(wall_seconds, 3),
        round(memory_mib, 2),
    )

    report(
        f"{method:<10} | "
        f"attempt={str(attempt):<6} | "
        f"compute={compute_seconds:8.3f}s | "
        f"wall={wall_seconds:8.3f}s | "
        f"peak={memory_mib:8.2f} MiB"
    )

    if attempt != "warmup":
        all_results.append(result)
        save_results()


def main():
    for filename in (
        "benchmark_compute.py",
        "optimized_mapreduce.py",
    ):
        if not (BASE / filename).is_file():
            raise FileNotFoundError(filename)

    DETAIL.write_text(
        "Optimization experiment: original vs curated input\n"
        "Three measured runs per method, plus warm-up\n"
        "Peak memory: Windows process peak working set\n",
        encoding="utf-8",
    )

    report("=== OPTIMIZATION BENCHMARK ===")
    report("Source: RustFS S3")
    report("Original: raw/vacancies.csv")
    report("Optimized: curated/devops_skills.csv")
    report(f"Preparation cost: {PREPARATION_SECONDS:.3f} s")

    report("\n=== WARM-UP ===")
    run_worker("original", "warmup")
    run_worker("optimized", "warmup")

    report("\n=== MEASURED RUNS ===")

    for attempt in (1, 2, 3):
        if attempt == 2:
            order = ("optimized", "original")
        else:
            order = ("original", "optimized")

        for method in order:
            run_worker(method, attempt)

    report("\n=== SUMMARY ===")

    stats = {}

    for method in ("original", "optimized"):
        rows = [
            r for r in all_results
            if r[0] == method
        ]

        assert len(rows) == 3

        times = [r[2] for r in rows]
        memory = [r[4] for r in rows]

        stats[method] = {
            "median_time": statistics.median(times),
            "mean_time": statistics.mean(times),
            "max_memory": max(memory),
        }

        report(
            f"{method}: "
            f"median={stats[method]['median_time']:.3f}s, "
            f"mean={stats[method]['mean_time']:.3f}s, "
            f"max_peak={stats[method]['max_memory']:.2f} MiB"
        )

    original = stats["original"]
    optimized = stats["optimized"]

    original_time = original["median_time"]
    optimized_time = optimized["median_time"]

    speedup = original_time / optimized_time
    reduction = 100 * (1 - optimized_time / original_time)

    report("\n=== IMPROVEMENT ===")
    report(f"Speedup: {speedup:.2f}x")
    report(f"Time reduction: {reduction:.2f}%")

    memory_change = 100 * (
        optimized["max_memory"] / original["max_memory"] - 1
    )

    report(
        f"Peak memory change: {memory_change:+.2f}%"
    )

    report("\n=== PREPARATION BREAK-EVEN ===")

    if original_time > optimized_time:
        saved_per_run = original_time - optimized_time

        # Smallest integer number of repeated calculations
        # whose time savings exceed the preparation cost.
        runs = math.floor(
            PREPARATION_SECONDS / saved_per_run
        ) + 1

        report(
            f"Preparation: {PREPARATION_SECONDS:.3f}s"
        )
        report(
            f"Median savings per run: {saved_per_run:.3f}s"
        )
        report(
            f"Break-even: {runs} calculation(s)"
        )

    else:
        report(
            "No positive calculation-time improvement measured"
        )

    report("\n=== OUTPUT FILES ===")
    report(str(RESULTS))
    report(str(DETAIL))
    report(str(SUMMARY))

    report("\nOPTIMIZATION BENCHMARK COMPLETED")

    SUMMARY.write_text(
        "\n".join(summary_lines) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
