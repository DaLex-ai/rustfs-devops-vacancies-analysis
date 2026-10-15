import csv
from pathlib import Path
import duckdb

from check_storage import access_key, secret_key

BASE = Path(__file__).resolve().parent
EVIDENCE = BASE / "evidence"
EVIDENCE.mkdir(exist_ok=True)

logs = []


def report(message=""):
    print(message, flush=True)
    logs.append(str(message))


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


con = duckdb.connect(":memory:")

try:
    report("=== NOVOSIBIRSK SALARY AUDIT ===")

    con.execute("LOAD httpfs")

    con.execute(f"""
        CREATE SECRET rustfs_audit (
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

    con.execute("""
        CREATE TEMP TABLE nsk_rows AS
        SELECT
            vacancy_id,
            name,
            experience,
            area,
            salary_from,
            salary_to,
            currency,
            gross
        FROM read_csv(
            's3://hh-lake/raw/vacancies.csv',
            header = true,
            all_varchar = true,
            delim = ',',
            quote = '"',
            escape = '"',
            max_line_size = 20000000
        )
        WHERE TRIM(COALESCE(role, '')) = 'DevOps-инженер'
          AND TRIM(COALESCE(area, '')) = 'Новосибирск'
    """)

    con.execute("""
        CREATE TEMP TABLE parsed AS
        SELECT *,
            TRY_CAST(
                NULLIF(
                    REPLACE(
                        TRIM(COALESCE(salary_from, '')),
                        ',', '.'
                    ), ''
                ) AS DOUBLE
            ) AS lower_bound,
            TRY_CAST(
                NULLIF(
                    REPLACE(
                        TRIM(COALESCE(salary_to, '')),
                        ',', '.'
                    ), ''
                ) AS DOUBLE
            ) AS upper_bound
        FROM nsk_rows
    """)

    con.execute("""
        CREATE TEMP TABLE nsk_pay AS
        SELECT *,
            CASE
                WHEN lower_bound IS NOT NULL
                 AND upper_bound IS NOT NULL
                    THEN (lower_bound + upper_bound) / 2
                ELSE COALESCE(lower_bound, upper_bound)
            END AS salary_proxy,
            CASE
                WHEN lower_bound IS NOT NULL
                 AND upper_bound IS NOT NULL
                    THEN 'midpoint'
                WHEN lower_bound IS NOT NULL
                    THEN 'lower_only'
                ELSE 'upper_only'
            END AS salary_basis
        FROM parsed
        WHERE UPPER(TRIM(COALESCE(currency, '')))
                  IN ('RUR', 'RUB')
          AND LOWER(TRIM(COALESCE(gross, ''))) = 'false'
          AND (lower_bound IS NOT NULL
               OR upper_bound IS NOT NULL)
          AND (lower_bound IS NULL OR lower_bound > 0)
          AND (upper_bound IS NULL OR upper_bound > 0)
          AND (
                lower_bound IS NULL
                OR upper_bound IS NULL
                OR lower_bound <= upper_bound
          )
    """)

    total = con.execute(
        "SELECT COUNT(*) FROM nsk_rows"
    ).fetchone()[0]

    paid = con.execute(
        "SELECT COUNT(*) FROM nsk_pay"
    ).fetchone()[0]

    report(f"Total Novosibirsk DevOps vacancies: {total}")
    report(f"Comparable RUB net salaries: {paid}")

    report("\n=== SALARY TYPES ===")

    types = con.execute("""
        SELECT
            salary_basis,
            COUNT(*) AS n,
            MIN(salary_proxy) AS minimum,
            MEDIAN(salary_proxy) AS median,
            MAX(salary_proxy) AS maximum
        FROM nsk_pay
        GROUP BY salary_basis
        ORDER BY salary_basis
    """).fetchall()

    for kind, n, minimum, median, maximum in types:
        report(
            f"{kind}: n={n}, "
            f"min={minimum:,.0f}, "
            f"median={median:,.0f}, "
            f"max={maximum:,.0f} RUB"
        )

    overall = con.execute("""
        SELECT MEDIAN(salary_proxy)
        FROM nsk_pay
    """).fetchone()[0]

    full_n, full_median = con.execute("""
        SELECT COUNT(*), MEDIAN(salary_proxy)
        FROM nsk_pay
        WHERE salary_basis = 'midpoint'
    """).fetchone()

    report("\n=== MEDIAN VALIDATION ===")
    report(f"Overall median: {overall:,.0f} RUB")
    report(f"Full-range records: {full_n}")
    report(f"Full-range median: {full_median:,.0f} RUB")

    assert total == 160
    assert paid == 41
    assert full_n == 15
    assert overall == 165000
    assert full_median == 40000

    report("CONTROL VALUES: PASSED")

    report("\n=== ALL FULL-RANGE VACANCIES ===")

    rows = con.execute("""
        SELECT
            vacancy_id,
            name,
            experience,
            lower_bound,
            upper_bound,
            salary_proxy
        FROM nsk_pay
        WHERE salary_basis = 'midpoint'
        ORDER BY salary_proxy, vacancy_id
    """).fetchall()

    for vacancy_id, name, experience, lower, upper, salary in rows:
        report(
            f"ID={vacancy_id} | "
            f"{name} | "
            f"experience={experience} | "
            f"from={lower:,.0f} | "
            f"to={upper:,.0f} | "
            f"proxy={salary:,.0f}"
        )

    path = EVIDENCE / "novosibirsk_full_ranges.csv"

    with path.open(
        "w", encoding="utf-8-sig", newline=""
    ) as file:
        writer = csv.writer(file)
        writer.writerow([
            "vacancy_id",
            "name",
            "experience",
            "salary_from",
            "salary_to",
            "salary_proxy",
        ])
        writer.writerows(rows)

    report("\n=== FINAL ===")
    report("Salary calculations reproduced from source CSV")
    report("NOVOSIBIRSK AUDIT PASSED")

finally:
    con.close()

    (EVIDENCE / "novosibirsk_salary_audit.txt").write_text(
        "\n".join(logs) + "\n",
        encoding="utf-8"
    )
