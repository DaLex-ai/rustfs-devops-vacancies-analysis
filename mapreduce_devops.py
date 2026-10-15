import csv
import io
import time
from collections import Counter
from itertools import combinations, islice
from pathlib import Path

from check_storage import s3

BUCKET = "hh-lake"
SOURCE_KEY = "raw/vacancies.csv"
TARGET_ROLE = "DevOps-инженер"
CHUNK_ROWS = 250

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "results" / "mapreduce"
EVIDENCE_DIR = BASE_DIR / "evidence"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)

csv.field_size_limit(20 * 1024 * 1024)


def log(message, file):
    print(message, flush=True)
    print(message, file=file, flush=True)


def map_chunk(rows):
    """Map: create local counts for one chunk."""
    local = [Counter(), Counter(), Counter()]
    with_skills = 0

    for row in rows:
        raw = row.get("key_skills") or ""

        skills = sorted({
            skill.strip()
            for skill in raw.split(";")
            if skill.strip()
        })

        if skills:
            with_skills += 1

        for k in (1, 2, 3):
            local[k - 1].update(combinations(skills, k))

    return local, len(rows), with_skills


def reduce_chunk(global_counts, local_counts):
    """Reduce: merge mapper results."""
    for global_counter, local_counter in zip(
        global_counts, local_counts
    ):
        global_counter.update(local_counter)


def write_result(k, counter):
    """Write sorted counts into a local CSV."""
    path = OUTPUT_DIR / f"skills_{k}.csv"

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [f"skill_{i}" for i in range(1, k + 1)]
            + ["count"]
        )

        for combination, count in sorted(counter.items()):
            writer.writerow([*combination, count])

    return path


def main():
    log_path = EVIDENCE_DIR / "mapreduce_run_1.txt"

    with log_path.open("w", encoding="utf-8") as evidence:
        def report(message):
            log(message, evidence)

        started = time.perf_counter()

        report("=== MAPREDUCE DEVOPS ===")
        report(f"S3 source: s3://{BUCKET}/{SOURCE_KEY}")
        report(f"Mapper chunk size: {CHUNK_ROWS} vacancies")

        global_counts = [
            Counter(),
            Counter(),
            Counter(),
        ]

        total_rows = 0
        total_with_skills = 0
        mapper_count = 0

        response = s3.get_object(
            Bucket=BUCKET,
            Key=SOURCE_KEY,
        )
        body = response["Body"]

        try:
            with io.TextIOWrapper(
                body,
                encoding="utf-8-sig",
                newline=""
            ) as stream:
                reader = csv.DictReader(stream)

                devops_rows = (
                    row for row in reader
                    if (row.get("role") or "").strip()
                    == TARGET_ROLE
                )

                while True:
                    chunk = list(islice(devops_rows, CHUNK_ROWS))

                    if not chunk:
                        break

                    mapper_count += 1

                    local, row_count, with_skills = map_chunk(
                        chunk
                    )

                    reduce_chunk(global_counts, local)

                    total_rows += row_count
                    total_with_skills += with_skills

                    report(
                        f"Mapper {mapper_count}: "
                        f"{row_count} rows | "
                        f"Total: {total_rows}"
                    )

        finally:
            body.close()

        report("\n=== CONTROL VALUES ===")
        report(f"DevOps vacancies: {total_rows}")
        report(f"Vacancies with skills: {total_with_skills}")
        report(f"Mapper executions: {mapper_count}")

        expected = [28344, 134786, 507851]

        assert total_rows == 5158, "Wrong DevOps count"
        assert total_with_skills == 3534, (
            "Wrong nonempty skills count"
        )

        for k, (counter, expected_count) in enumerate(
            zip(global_counts, expected), start=1
        ):
            occurrences = sum(counter.values())

            report(
                f"Stack {k}: "
                f"{len(counter)} unique combinations, "
                f"{occurrences} occurrences"
            )

            assert occurrences == expected_count, (
                f"Stack {k}: expected {expected_count}, "
                f"got {occurrences}"
            )

        report("\nCONTROL CHECK: PASSED")
        report("\n=== UPLOAD RESULTS TO RUSTFS ===")

        for k, counter in enumerate(global_counts, start=1):
            path = write_result(k, counter)

            target_key = (
                f"results/mapreduce/skills_{k}.csv"
            )

            s3.upload_file(
                str(path),
                BUCKET,
                target_key,
            )

            remote = s3.head_object(
                Bucket=BUCKET,
                Key=target_key,
            )

            local_size = path.stat().st_size
            assert remote["ContentLength"] == local_size

            report(
                f"Uploaded s3://{BUCKET}/{target_key} "
                f"({local_size} bytes)"
            )

        elapsed = time.perf_counter() - started

        report("\n=== FINAL RESULT ===")
        report(f"Elapsed time: {elapsed:.3f} seconds")
        report("MAPREDUCE COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()
