import csv
import io
import time
from collections import Counter
from itertools import islice
from pathlib import Path

from check_storage import s3
from mapreduce_devops import map_chunk, reduce_chunk

BASE = Path(__file__).resolve().parent
EVIDENCE = BASE / "evidence"
EVIDENCE.mkdir(exist_ok=True)

BUCKET = "hh-lake"
SOURCE_KEY = "raw/vacancies.csv"
FAILED_MAPPER = 5
MAX_ATTEMPTS = 2
CHUNK_SIZE = 250

csv.field_size_limit(20 * 1024 * 1024)


class SimulatedMapperCrash(RuntimeError):
    pass


with (EVIDENCE / "mapper_failure_test.txt").open(
    "w", encoding="utf-8"
) as evidence:

    def report(message=""):
        print(message, flush=True)
        print(message, file=evidence, flush=True)

    def run_mapper(rows, mapper_id, failure_mode="none"):
        """
        Simulate a failure after local Map computation.

        The result is committed to Reduce only when
        the mapper finishes successfully.
        """
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                partial, row_count, skilled = map_chunk(rows)

                if failure_mode == "permanent":
                    raise SimulatedMapperCrash(
                        "Permanent injected mapper failure"
                    )

                if (
                    failure_mode == "transient"
                    and mapper_id == FAILED_MAPPER
                    and attempt == 1
                ):
                    raise SimulatedMapperCrash(
                        "Injected failure after map calculation"
                    )

                return partial, row_count, skilled, attempt

            except SimulatedMapperCrash as exc:
                report(
                    f"MAPPER {mapper_id} FAILED "
                    f"on attempt {attempt}: {exc}"
                )

                if attempt == MAX_ATTEMPTS:
                    raise RuntimeError(
                        f"Mapper {mapper_id}: "
                        "maximum attempts exhausted"
                    ) from exc

                report(f"Retrying mapper {mapper_id}...")

        raise AssertionError("Unreachable retry state")


    def load_reference(k):
        key = f"results/mapreduce/skills_{k}.csv"

        response = s3.get_object(
            Bucket=BUCKET,
            Key=key,
        )

        body = response["Body"]
        result = {}
        columns = [
            f"skill_{i}" for i in range(1, k + 1)
        ]

        try:
            with io.TextIOWrapper(
                body,
                encoding="utf-8-sig",
                newline=""
            ) as stream:
                reader = csv.DictReader(stream)

                for row in reader:
                    skills = tuple(row[c] for c in columns)

                    if skills in result:
                        raise AssertionError(
                            f"Duplicate reference key: {skills}"
                        )

                    result[skills] = int(row["count"])

        finally:
            body.close()

        return result


    started = time.perf_counter()

    report("=== MAPREDUCE FAILURE RECOVERY TEST ===")
    report(f"Injected failure: mapper {FAILED_MAPPER}")
    report(f"Maximum mapper attempts: {MAX_ATTEMPTS}")
    report("Failure moment: after Map, before Reduce")

    global_counts = [
        Counter(),
        Counter(),
        Counter(),
    ]

    total = 0
    total_skilled = 0
    mapper_count = 0
    recovered_count = 0
    first_chunk = None

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

            selected = (
                row for row in reader
                if (row.get("role") or "").strip()
                == "DevOps-инженер"
            )

            while True:
                chunk = list(islice(selected, CHUNK_SIZE))

                if not chunk:
                    break

                mapper_count += 1

                if first_chunk is None:
                    first_chunk = chunk

                partial, n, skilled, attempts = run_mapper(
                    chunk,
                    mapper_count,
                    failure_mode="transient",
                )

                # Exactly one successful Map result
                # is included in the global Reduce.
                reduce_chunk(global_counts, partial)

                total += n
                total_skilled += skilled

                if attempts > 1:
                    recovered_count += 1
                    report(
                        f"MAPPER {mapper_count} RECOVERED "
                        f"on attempt {attempts}"
                    )

    finally:
        body.close()

    report("\n=== RECOVERY RESULTS ===")
    report(f"Mapper chunks processed: {mapper_count}")
    report(f"DevOps vacancies processed: {total}")
    report(f"Vacancies with skills: {total_skilled}")
    report(f"Recovered mapper chunks: {recovered_count}")

    assert mapper_count == 21
    assert total == 5158
    assert total_skilled == 3534
    assert recovered_count == 1

    report("\n=== EXACT REFERENCE VERIFICATION ===")

    expected = {
        1: (1560, 28344),
        2: (22311, 134786),
        3: (152803, 507851),
    }

    for k in (1, 2, 3):
        reference = load_reference(k)
        actual = global_counts[k - 1]

        unique_count = len(actual)
        occurrences = sum(actual.values())

        assert (unique_count, occurrences) == expected[k]

        if actual != reference:
            missing = set(reference) - set(actual)
            extra = set(actual) - set(reference)

            differences = [
                key for key in actual.keys() & reference.keys()
                if actual[key] != reference[key]
            ]

            report(
                f"Stack {k}: "
                f"missing={len(missing)}, "
                f"extra={len(extra)}, "
                f"different={len(differences)}"
            )

            raise AssertionError(
                f"Recovered result differs at stack {k}"
            )

        report(
            f"Stack {k}: EXACT MATCH | "
            f"{unique_count} unique | "
            f"{occurrences} occurrences"
        )

    report("\n=== PERMANENT FAILURE TEST ===")

    assert first_chunk is not None

    try:
        run_mapper(
            first_chunk,
            mapper_id=999,
            failure_mode="permanent",
        )

    except RuntimeError as exc:
        if "maximum attempts exhausted" not in str(exc):
            raise

        report("Permanent mapper failure detected")
        report("Retry limit enforced: YES")
        report("Unsuccessful mapper result not committed")

    else:
        raise AssertionError(
            "Permanent mapper failure was not detected"
        )

    report("\n=== FINAL RESULT ===")
    report(
        f"Elapsed time: "
        f"{time.perf_counter() - started:.3f} seconds"
    )
    report("FAILURE RECOVERY TEST PASSED")
