import csv
import hashlib
import io
import time
from collections import Counter
from itertools import islice
from pathlib import Path

from check_storage import s3
from mapreduce_devops import map_chunk, reduce_chunk

BASE = Path(__file__).resolve().parent
OUT = BASE / "results" / "optimized"
OUT.mkdir(parents=True, exist_ok=True)

BUCKET = "hh-lake"
RAW = "raw/vacancies.csv"
CURATED = "curated/devops_skills.csv"

EXPECTED = {
    1: (1560, 28344),
    2: (22311, 134786),
    3: (152803, 507851),
}

csv.field_size_limit(20 * 1024 * 1024)


def object_sha256(key):
    response = s3.get_object(Bucket=BUCKET, Key=key)
    body = response["Body"]
    digest = hashlib.sha256()

    try:
        for block in body.iter_chunks(
            chunk_size=1024 * 1024
        ):
            if block:
                digest.update(block)
    finally:
        body.close()

    return digest.hexdigest()


def prepare():
    print("=== PREPARE DEVOPS DATASET ===", flush=True)
    started = time.perf_counter()

    local = OUT / "devops_skills.csv"

    response = s3.get_object(
        Bucket=BUCKET,
        Key=RAW,
    )

    body = response["Body"]
    count = 0
    skilled = 0
    seen_ids = set()

    try:
        with io.TextIOWrapper(
            body,
            encoding="utf-8-sig",
            newline=""
        ) as stream, local.open(
            "w",
            encoding="utf-8",
            newline=""
        ) as output:

            reader = csv.DictReader(stream)

            writer = csv.DictWriter(
                output,
                fieldnames=["vacancy_id", "key_skills"]
            )
            writer.writeheader()

            for row in reader:
                if (
                    (row.get("role") or "").strip()
                    != "DevOps-инженер"
                ):
                    continue

                vacancy_id = (
                    row.get("vacancy_id") or ""
                ).strip()

                if (
                    not vacancy_id
                    or vacancy_id in seen_ids
                ):
                    raise ValueError(
                        "Missing or repeated DevOps vacancy_id"
                    )

                seen_ids.add(vacancy_id)

                skills = row.get("key_skills") or ""

                writer.writerow({
                    "vacancy_id": vacancy_id,
                    "key_skills": skills,
                })

                count += 1
                skilled += bool(skills.strip())

    finally:
        body.close()

    assert (count, skilled) == (5158, 3534), (
        count, skilled
    )

    size = local.stat().st_size

    local_sha = hashlib.sha256(
        local.read_bytes()
    ).hexdigest()

    s3.upload_file(
        str(local),
        BUCKET,
        CURATED,
    )

    remote_size = s3.head_object(
        Bucket=BUCKET,
        Key=CURATED,
    )["ContentLength"]

    remote_sha = object_sha256(CURATED)

    if (size, local_sha) != (
        remote_size, remote_sha
    ):
        raise AssertionError(
            "Curated object hash or size differs"
        )

    print("Rows:", count)
    print("Rows with skills:", skilled)
    print("Object: s3://hh-lake/" + CURATED)
    print("Object bytes:", remote_size)
    print("SHA256:", remote_sha)

    print(
        f"Preparation time: "
        f"{time.perf_counter() - started:.3f} s"
    )

    print("PREPARE PASSED")


def load_reference(k):
    key = f"results/mapreduce/skills_{k}.csv"

    response = s3.get_object(
        Bucket=BUCKET,
        Key=key,
    )

    body = response["Body"]

    columns = [
        f"skill_{i}"
        for i in range(1, k + 1)
    ]

    counts = Counter()

    try:
        with io.TextIOWrapper(
            body,
            encoding="utf-8-sig",
            newline=""
        ) as stream:

            for row in csv.DictReader(stream):
                skill_key = tuple(
                    row[c] for c in columns
                )

                if skill_key in counts:
                    raise AssertionError(
                        f"Repeated reference combination: "
                        f"{skill_key}"
                    )

                counts[skill_key] = int(row["count"])

    finally:
        body.close()

    return counts


def run(verify):
    print("=== OPTIMIZED MAPREDUCE ===", flush=True)

    start = time.perf_counter()

    counts = [
        Counter(),
        Counter(),
        Counter(),
    ]

    total = 0
    skilled_total = 0
    mappers = 0

    response = s3.get_object(
        Bucket=BUCKET,
        Key=CURATED,
    )

    body = response["Body"]

    try:
        with io.TextIOWrapper(
            body,
            encoding="utf-8-sig",
            newline=""
        ) as stream:

            reader = csv.DictReader(stream)

            while True:
                chunk = list(
                    islice(reader, 250)
                )

                if not chunk:
                    break

                partial, n, with_skills = map_chunk(
                    chunk
                )

                reduce_chunk(counts, partial)

                total += n
                skilled_total += with_skills
                mappers += 1

    finally:
        body.close()

    assert (
        total, skilled_total, mappers
    ) == (5158, 3534, 21)

    for k, counter in enumerate(counts, 1):
        ordered = sorted(counter.items())

        actual = (
            len(ordered),
            sum(freq for _, freq in ordered),
        )

        assert actual == EXPECTED[k], (
            k, actual
        )

        print(
            f"Stack {k}: "
            f"{actual[0]} unique, "
            f"{actual[1]} occurrences"
        )

    elapsed = time.perf_counter() - start

    print(f"CORE_SECONDS={elapsed:.6f}")

    if verify:
        for k, counter in enumerate(counts, 1):
            reference = load_reference(k)

            if counter != reference:
                raise AssertionError(
                    f"Full MapReduce comparison failed "
                    f"for stack {k}"
                )

            print(
                f"Stack {k}: EXACT MATCH "
                f"with raw-source MapReduce",
                flush=True
            )

        print("OPTIMIZED VERIFICATION PASSED")

    else:
        import os
        import psutil

        mem = psutil.Process(
            os.getpid()
        ).memory_info()

        peak = getattr(
            mem, "peak_wset", None
        )

        if peak is None:
            raise RuntimeError(
                "Windows peak working set unavailable"
            )

        print(
            f"PEAK_WORKING_SET_MIB="
            f"{peak / 1048576:.3f}"
        )

        print("BENCHMARK_OK")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "mode",
        choices=[
            "prepare",
            "verify",
            "benchmark-worker"
        ]
    )

    args = parser.parse_args()

    if args.mode == "prepare":
        prepare()
    else:
        run(
            verify=args.mode == "verify"
        )
