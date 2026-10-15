import csv
import time
from pathlib import Path

import duckdb

from check_storage import s3, access_key, secret_key

BASE = Path(__file__).resolve().parent
SQL_DIR = BASE / "sql"
OUT = BASE / "results" / "regions"
EVIDENCE = BASE / "evidence"

OUT.mkdir(parents=True, exist_ok=True)
EVIDENCE.mkdir(parents=True, exist_ok=True)
SQL_DIR.mkdir(parents=True, exist_ok=True)

REGIONS = [
    "Москва",
    "Санкт-Петербург",
    "Новосибирск",
]


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def save_csv(filename, columns, rows):
    path = OUT / filename

    with path.open(
        "w", encoding="utf-8-sig", newline=""
    ) as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(rows)

    key = f"results/regions/{filename}"
    s3.upload_file(str(path), "hh-lake", key)

    remote = s3.head_object(
        Bucket="hh-lake", Key=key
    )
    assert remote["ContentLength"] == path.stat().st_size

    return f"s3://hh-lake/{key}"


def skill_query(k):
    aliases = ["a", "b", "c"][:k]

    keys = [
        f"{alias}.skill AS skill_{i}"
        for i, alias in enumerate(aliases, 1)
    ]

    join_sql = ""

    if k >= 2:
        join_sql += """
        JOIN regional_skills b
          ON a.vacancy_id = b.vacancy_id
         AND a.region = b.region
         AND a.skill < b.skill
        """

    if k >= 3:
        join_sql += """
        JOIN regional_skills c
          ON b.vacancy_id = c.vacancy_id
         AND b.region = c.region
         AND b.skill < c.skill
        """

    group_cols = ", ".join(
        ["a.region"] +
        [f"{a}.skill" for a in aliases]
    )

    sort_cols = ", ".join(
        f"skill_{i}" for i in range(1, k + 1)
    )

    return f"""
    WITH frequencies AS (
        SELECT
            a.region,
            {", ".join(keys)},
            COUNT(*) AS vacancy_count
        FROM regional_skills a
        {join_sql}
        GROUP BY {group_cols}
    ),
    ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY region
                ORDER BY vacancy_count DESC, {sort_cols}
            ) AS rank_n
        FROM frequencies
    )
    SELECT
        region, rank_n,
        {sort_cols},
        vacancy_count
    FROM ranked
    WHERE rank_n <= 10
    ORDER BY region, rank_n
    """


def main():
    started = time.perf_counter()
    logs = []

    def report(message=""):
        print(message, flush=True)
        logs.append(str(message))

    con = duckdb.connect(":memory:")

    try:
        report("=== REGIONAL DEVOPS ANALYSIS ===")
        report("Source: s3://hh-lake/raw/vacancies.csv")

        con.execute("LOAD httpfs")

        con.execute(f"""
            CREATE SECRET rustfs_regions (
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

        preparation = (
            SQL_DIR / "salary_prepare.sql"
        ).read_text(encoding="utf-8-sig")

        con.execute(preparation)

        regions_sql = ", ".join(quote(r) for r in REGIONS)

        con.execute(f"""
            CREATE TEMP TABLE regional_skills AS
            SELECT
                v.vacancy_id,
                TRIM(v.area) AS region,
                s.skill
            FROM source_devops v
            JOIN devops_skills s USING (vacancy_id)
            WHERE TRIM(v.area) IN ({regions_sql})
        """)

        summary_sql = f"""
        WITH has_skills AS (
            SELECT DISTINCT vacancy_id
            FROM devops_skills
        )
        SELECT
            TRIM(v.area) AS region,
            COUNT(*) AS total_vacancies,
            COUNT(h.vacancy_id) AS with_skills,
            COUNT(*) FILTER (
                WHERE
                    NULLIF(TRIM(COALESCE(v.salary_from, '')), '')
                        IS NOT NULL
                    OR
                    NULLIF(TRIM(COALESCE(v.salary_to, '')), '')
                        IS NOT NULL
            ) AS salary_any,
            COUNT(p.vacancy_id) AS net_rub_n,
            ROUND(MEDIAN(p.salary_proxy), 2)
                AS median_net_rub,
            COUNT(p.vacancy_id) FILTER (
                WHERE p.salary_basis = 'midpoint'
            ) AS full_range_n,
            ROUND(
                MEDIAN(p.salary_proxy) FILTER (
                    WHERE p.salary_basis = 'midpoint'
                ), 2
            ) AS full_range_median,
            ROUND(
                100.0 * COUNT(p.vacancy_id) / COUNT(*), 2
            ) AS salary_coverage_pct
        FROM source_devops v
        LEFT JOIN has_skills h USING (vacancy_id)
        LEFT JOIN eligible_pay p USING (vacancy_id)
        WHERE TRIM(v.area) IN ({regions_sql})
        GROUP BY TRIM(v.area)
        ORDER BY TRIM(v.area)
        """

        (SQL_DIR / "regional_summary.sql").write_text(
            summary_sql.strip() + ";\n",
            encoding="utf-8"
        )

        cursor = con.execute(summary_sql)
        columns = [d[0] for d in cursor.description]
        summary_rows = cursor.fetchall()

        summary = {
            row[0]: dict(zip(columns, row))
            for row in summary_rows
        }

        assert set(summary) == set(REGIONS)

        expected = {
            "Москва": (3007, 406),
            "Санкт-Петербург": (686, 103),
            "Новосибирск": (160, 41),
        }

        report("\n=== REGIONAL SALARY TABLE ===")

        for region in REGIONS:
            data = summary[region]

            actual = (
                data["total_vacancies"],
                data["net_rub_n"],
            )

            assert actual == expected[region], (
                f"Unexpected regional counts: {region}: {actual}"
            )

            report(
                f"{region}: "
                f"total={data['total_vacancies']}, "
                f"with_skills={data['with_skills']}, "
                f"net_RUB_n={data['net_rub_n']}, "
                f"median={data['median_net_rub']} RUB, "
                f"full_range_n={data['full_range_n']}, "
                f"full_range_median={data['full_range_median']} RUB, "
                f"coverage={data['salary_coverage_pct']}%"
            )

        path = save_csv(
            "regional_summary.csv",
            columns,
            summary_rows,
        )

        report("Uploaded: " + path)

        all_top_rows = []

        report("\n=== REGIONAL SKILLS TABLE ===")

        for k in (1, 2, 3):
            query = skill_query(k)

            (SQL_DIR / f"regional_top_{k}.sql").write_text(
                query.strip() + ";\n",
                encoding="utf-8"
            )

            rows = con.execute(query).fetchall()
            report(f"\nStack {k}: {len(rows)} ranked rows")

            for row in rows:
                region = row[0]
                rank = row[1]
                skills = list(row[2:2 + k])
                count = int(row[-1])

                total = summary[region]["total_vacancies"]
                with_skills = summary[region]["with_skills"]

                assert 0 < count <= total

                percent_all = round(
                    100 * count / total, 2
                )

                percent_skilled = round(
                    100 * count / with_skills, 2
                ) if with_skills else 0.0

                padded = skills + [""] * (3 - k)

                all_top_rows.append((
                    region,
                    k,
                    rank,
                    *padded,
                    count,
                    percent_all,
                    percent_skilled,
                ))

                if rank <= 3:
                    report(
                        f"{region} | "
                        f"rank={rank} | "
                        f"{' + '.join(skills)} | "
                        f"vacancies={count} | "
                        f"share={percent_all}%"
                    )

        top_columns = [
            "region",
            "stack_size",
            "rank",
            "skill_1",
            "skill_2",
            "skill_3",
            "vacancy_count",
            "percent_all_vacancies",
            "percent_vacancies_with_skills",
        ]

        path = save_csv(
            "regional_top_skills.csv",
            top_columns,
            all_top_rows,
        )

        report("\nUploaded: " + path)

        report("\n=== FINAL ===")
        report(f"Elapsed: {time.perf_counter()-started:.3f} s")
        report("REGIONAL ANALYSIS COMPLETED")

    finally:
        con.close()

        (EVIDENCE / "regional_analysis.txt").write_text(
            "\n".join(logs) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
