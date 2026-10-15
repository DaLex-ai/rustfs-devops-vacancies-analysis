import csv
import time
from pathlib import Path

import duckdb
from check_storage import s3, access_key, secret_key

BASE = Path(__file__).resolve().parent
SQL_DIR = BASE / "sql"
OUTPUT_DIR = BASE / "results" / "duckdb"
EVIDENCE_DIR = BASE / "evidence"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def read_counts(path, k):
    result = {}
    columns = [f"skill_{i}" for i in range(1, k + 1)]

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = tuple(row[col] for col in columns)
            if key in result:
                raise ValueError(f"Duplicate combination in {path}: {key}")
            result[key] = int(row["count"])

    return result


def main():
    started = time.perf_counter()
    messages = []

    def log(message):
        print(message, flush=True)
        messages.append(str(message))

    con = duckdb.connect(":memory:")

    try:
        log("=== DUCKDB DEVOPS ===")
        log("DuckDB version: " + duckdb.__version__)

        try:
            con.execute("LOAD httpfs")
        except duckdb.Error:
            log("Installing httpfs extension...")
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")

        secret_sql = f"""
            CREATE SECRET rustfs_local (
                TYPE s3,
                PROVIDER config,
                KEY_ID {quote(access_key)},
                SECRET {quote(secret_key)},
                REGION 'us-east-1',
                ENDPOINT 'localhost:9000',
                URL_STYLE 'path',
                USE_SSL false
            )
        """
        con.execute(secret_sql)

        log("S3 authentication: configured")
        log("Source: s3://hh-lake/raw/vacancies.csv")

        log("\n=== PREPARING DATA ===")

        preparation = (SQL_DIR / "prepare.sql").read_text(
            encoding="utf-8-sig"
        )

        # DuckDB supports executing a multi-statement SQL script.
        con.execute(preparation)

        rows = con.execute(
            "SELECT COUNT(*) FROM devops_rows"
        ).fetchone()[0]

        with_skills = con.execute(
            "SELECT COUNT(DISTINCT vacancy_id) FROM devops_skills"
        ).fetchone()[0]

        log(f"DevOps vacancies: {rows}")
        log(f"Vacancies with skills: {with_skills}")

        assert rows == 5158, "Wrong number of DevOps vacancies"
        assert with_skills == 3534, "Wrong number with skills"

        log("\n=== SQL COUNTS AND VERIFICATION ===")

        expected_unique = {1: 1560, 2: 22311, 3: 152803}
        expected_total = {1: 28344, 2: 134786, 3: 507851}

        for k in (1, 2, 3):
            query = (SQL_DIR / f"skills_{k}.sql").read_text(
                encoding="utf-8-sig"
            ).strip().rstrip(";")

            result_table = f"result_{k}"

            con.execute(
                f"CREATE TEMP TABLE {result_table} AS {query}"
            )

            unique_count, total_count = con.execute(
                f"""
                SELECT COUNT(*), COALESCE(SUM(count), 0)
                FROM {result_table}
                """
            ).fetchone()

            log(
                f"Stack {k}: {unique_count} unique, "
                f"{total_count} occurrences"
            )

            assert unique_count == expected_unique[k], (
                f"Unique count mismatch for stack {k}"
            )
            assert total_count == expected_total[k], (
                f"Occurrence mismatch for stack {k}"
            )

            output_path = OUTPUT_DIR / f"skills_{k}.csv"

            if output_path.exists():
                output_path.unlink()

            sql_path = output_path.as_posix()

            con.execute(
                f"""
                COPY {result_table}
                TO {quote(sql_path)}
                (FORMAT CSV, HEADER TRUE)
                """
            )

            mr_path = (
                BASE / "results" / "mapreduce"
                / f"skills_{k}.csv"
            )

            if not mr_path.is_file():
                raise FileNotFoundError(
                    f"Missing MapReduce result: {mr_path}"
                )

            mr_counts = read_counts(mr_path, k)
            db_counts = read_counts(output_path, k)

            missing = mr_counts.keys() - db_counts.keys()
            extra = db_counts.keys() - mr_counts.keys()

            differences = [
                (key, mr_counts[key], db_counts[key])
                for key in mr_counts.keys() & db_counts.keys()
                if mr_counts[key] != db_counts[key]
            ]

            log(
                f"Comparison stack {k}: "
                f"missing={len(missing)}, "
                f"extra={len(extra)}, "
                f"different_counts={len(differences)}"
            )

            if missing or extra or differences:
                log("Example missing: " + repr(list(missing)[:3]))
                log("Example extra: " + repr(list(extra)[:3]))
                log("Example differences: " + repr(differences[:3]))
                raise AssertionError(
                    f"MapReduce vs DuckDB mismatch: stack {k}"
                )

            log(f"Stack {k}: EXACT MATCH")

            remote_key = f"results/duckdb/skills_{k}.csv"

            s3.upload_file(
                str(output_path),
                "hh-lake",
                remote_key,
            )

            info = s3.head_object(
                Bucket="hh-lake",
                Key=remote_key,
            )

            assert info["ContentLength"] == output_path.stat().st_size

            log(f"Uploaded: s3://hh-lake/{remote_key}")

            con.execute(f"DROP TABLE {result_table}")

        log("\n=== FINAL RESULT ===")
        log("All three levels matched MapReduce exactly")
        log(f"Elapsed time: {time.perf_counter() - started:.3f} seconds")
        log("DUCKDB VERIFICATION PASSED")

    finally:
        con.close()
        (EVIDENCE_DIR / "duckdb_run_1.txt").write_text(
            "\n".join(messages) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()

