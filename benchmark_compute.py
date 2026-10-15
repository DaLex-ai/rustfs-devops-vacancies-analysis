import csv
import io
import os
import re
import statistics
import subprocess
import sys
import time

from collections import Counter
from itertools import islice
from pathlib import Path

BASE = Path(__file__).resolve().parent

EXPECTED = {
    1: (1560, 28344),
    2: (22311, 134786),
    3: (152803, 507851),
}


def benchmark_mapreduce():
    from check_storage import s3
    from mapreduce_devops import map_chunk, reduce_chunk

    started = time.perf_counter()

    counters = [Counter(), Counter(), Counter()]
    rows_total = 0
    skilled_total = 0

    response = s3.get_object(
        Bucket="hh-lake",
        Key="raw/vacancies.csv",
    )
    body = response["Body"]

    csv.field_size_limit(20 * 1024 * 1024)

    try:
        with io.TextIOWrapper(
            body, encoding="utf-8-sig", newline=""
        ) as stream:
            reader = csv.DictReader(stream)

            selected = (
                row for row in reader
                if (row.get("role") or "").strip()
                == "DevOps-инженер"
            )

            while True:
                chunk = list(islice(selected, 250))

                if not chunk:
                    break

                local, n, skilled = map_chunk(chunk)
                reduce_chunk(counters, local)

                rows_total += n
                skilled_total += skilled

    finally:
        body.close()

    assert rows_total == 5158
    assert skilled_total == 3534

    for k, counter in enumerate(counters, 1):
        ordered = sorted(counter.items())

        actual = (
            len(ordered),
            sum(count for _, count in ordered),
        )

        assert actual == EXPECTED[k], (
            f"MapReduce stack {k}: {actual} "
            f"!= {EXPECTED[k]}"
        )

    elapsed = time.perf_counter() - started

    print(f"CORE_SECONDS={elapsed:.6f}", flush=True)
    import psutil

    memory = psutil.Process(os.getpid()).memory_info()
    peak = getattr(memory, "peak_wset", None)

    if peak is None:
        peak = getattr(memory, "peak_rss", None)

    if peak is None or peak <= 0:
        raise RuntimeError("Peak working set unavailable")

    print(
        f"PEAK_WORKING_SET_MIB={peak / 1048576:.3f}",
        flush=True,
    )

    print("BENCHMARK_OK", flush=True)


def benchmark_duckdb():
    import duckdb
    from check_storage import access_key, secret_key

    def quote(value):
        return "'" + str(value).replace("'", "''") + "'"

    started = time.perf_counter()
    con = duckdb.connect(":memory:")

    try:
        con.execute("SET threads=1")
        con.execute("LOAD httpfs")

        con.execute(f"""
            CREATE SECRET benchmark_rustfs (
                TYPE s3,
                PROVIDER config,
                KEY_ID {quote(access_key)},
                SECRET {quote(secret_key)},
                REGION 'us-east-1',
                ENDPOINT 'localhost:9000',
                URL_STYLE 'path',
                USE_SSL false
            )
        """)

        prepare = (
            BASE / "sql" / "prepare.sql"
        ).read_text(encoding="utf-8-sig")

        con.execute(prepare)

        rows = con.execute(
            "SELECT COUNT(*) FROM devops_rows"
        ).fetchone()[0]

        skilled = con.execute(
            "SELECT COUNT(DISTINCT vacancy_id) "
            "FROM devops_skills"
        ).fetchone()[0]

        assert rows == 5158
        assert skilled == 3534

        for k in (1, 2, 3):
            query = (
                BASE / "sql" / f"skills_{k}.sql"
            ).read_text(
                encoding="utf-8-sig"
            ).strip().rstrip(";")

            con.execute(
                f"CREATE TEMP TABLE bench_{k} AS {query}"
            )

            actual = con.execute(f"""
                SELECT
                    COUNT(*),
                    COALESCE(SUM(count), 0)
                FROM bench_{k}
            """).fetchone()

            assert tuple(actual) == EXPECTED[k], (
                f"DuckDB stack {k}: {actual} "
                f"!= {EXPECTED[k]}"
            )

            con.execute(f"DROP TABLE bench_{k}")

    finally:
        con.close()

    elapsed = time.perf_counter() - started

    print(f"CORE_SECONDS={elapsed:.6f}", flush=True)
    import psutil

    memory = psutil.Process(os.getpid()).memory_info()
    peak = getattr(memory, "peak_wset", None)

    if peak is None:
        peak = getattr(memory, "peak_rss", None)

    if peak is None or peak <= 0:
        raise RuntimeError("Peak working set unavailable")

    print(
        f"PEAK_WORKING_SET_MIB={peak / 1048576:.3f}",
        flush=True,
    )

    print("BENCHMARK_OK", flush=True)


def run_one(method, label, psutil, log_path):
    environment = os.environ.copy()
    environment["PYTHONIOENCODING"] = "utf-8"

    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        method,
    ]

    started = time.perf_counter()

    process = subprocess.Popen(
        command,
        cwd=BASE,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    monitor = psutil.Process(process.pid)
    peak_bytes = 0

    while process.poll() is None:
        try:
            current = monitor.memory_info().rss
            peak_bytes = max(peak_bytes, current)
        except psutil.Error:
            pass

        time.sleep(0.02)

    try:
        peak_bytes = max(
            peak_bytes,
            monitor.memory_info().rss
        )
    except psutil.Error:
        pass

    output, _ = process.communicate()
    wall_seconds = time.perf_counter() - started

    with log_path.open("a", encoding="utf-8") as file:
        file.write(
            f"\n=== {label}: {method} ===\n"
            f"Exit code: {process.returncode}\n"
            f"{output}\n"
        )

    match = re.search(
        r"^CORE_SECONDS=([0-9.]+)$",
        output,
        re.MULTILINE,
    )

    if (
        process.returncode != 0
        or "BENCHMARK_OK" not in output
        or match is None
    ):
        print(output, flush=True)
        raise RuntimeError(
            f"Benchmark failed: {method}, {label}. "
            f"See {log_path}"
        )

    peak_match = re.search(
        r"^PEAK_WORKING_SET_MIB=([0-9.]+)$",
        output,
        re.MULTILINE,
    )

    if peak_match is None:
        raise RuntimeError(
            "Worker did not report peak working set"
        )

    result = (
        round(float(match.group(1)), 3),
        round(wall_seconds, 3),
        round(float(peak_match.group(1)), 2),
    )

    print(
        f"{label:>8} | {method:<9} | "
        f"compute={result[0]:>8.3f}s | "
        f"wall={result[1]:>8.3f}s | "
        f"peakWSet={result[2]:>8.2f} MiB",
        flush=True,
    )

    return result


def main():
    import psutil

    evidence = BASE / "evidence"
    evidence.mkdir(parents=True, exist_ok=True)

    log_path = evidence / "benchmark_detail.txt"
    result_path = evidence / "benchmark_results.csv"

    log_path.write_text(
        "Compute benchmark: RustFS / S3\n"
        "DuckDB threads: 1\n"
        "Warm-up excluded from summary\n"
        "Peak memory: Windows peak working set reported by worker\n",
        encoding="utf-8",
    )

    all_results = []

    print("=== ENVIRONMENT ===", flush=True)
    print("Python:", sys.version.split()[0], flush=True)
    print("Platform:", sys.platform, flush=True)
    print("psutil:", psutil.__version__, flush=True)

    print(
        "\n=== WARM-UP (not included in final statistics) ===",
        flush=True,
    )

    for method in ("mapreduce", "duckdb"):
        run_one(method, "warmup", psutil, log_path)

    print(
        "\n=== THREE RECORDED ATTEMPTS PER METHOD ===",
        flush=True,
    )

    for attempt in (1, 2, 3):
        if attempt % 2 == 1:
            order = ("mapreduce", "duckdb")
        else:
            order = ("duckdb", "mapreduce")

        for method in order:
            compute_s, wall_s, peak_mib = run_one(
                method,
                f"run{attempt}",
                psutil,
                log_path,
            )

            all_results.append((
                method,
                attempt,
                compute_s,
                wall_s,
                peak_mib,
            ))

            with result_path.open(
                "w",
                encoding="utf-8-sig",
                newline="",
            ) as file:
                writer = csv.writer(file)

                writer.writerow([
                    "method",
                    "attempt",
                    "compute_s",
                    "wall_s",
                    "peak_working_set_mib",
                ])

                writer.writerows(all_results)

    print(
        "\n=== SUMMARY (3 attempts, compute time) ===",
        flush=True,
    )

    for method in ("mapreduce", "duckdb"):
        subset = [
            row for row in all_results
            if row[0] == method
        ]

        median_time = statistics.median(
            row[2] for row in subset
        )

        mean_time = statistics.mean(
            row[2] for row in subset
        )

        peak_memory = max(
            row[4] for row in subset
        )

        print(
            f"{method}: "
            f"time median={median_time:.3f}s, "
            f"time mean={mean_time:.3f}s, "
            f"peak working set max={peak_memory:.2f} MiB",
            flush=True,
        )

    print(f"\nSaved: {result_path}")
    print("BENCHMARK COMPLETED")


if __name__ == "__main__":
    if (
        len(sys.argv) == 3
        and sys.argv[1] == "--worker"
    ):
        if sys.argv[2] == "mapreduce":
            benchmark_mapreduce()
        elif sys.argv[2] == "duckdb":
            benchmark_duckdb()
        else:
            raise ValueError("Unknown worker method")
    else:
        main()
