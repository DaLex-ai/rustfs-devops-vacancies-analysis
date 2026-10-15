import csv
import time
from pathlib import Path

import duckdb

from check_storage import s3, access_key, secret_key
from duckdb_devops import quote

BASE = Path(__file__).resolve().parent
OUT = BASE / "results" / "salary"
EVIDENCE = BASE / "evidence"
SQL = BASE / "sql"

OUT.mkdir(parents=True, exist_ok=True)
EVIDENCE.mkdir(parents=True, exist_ok=True)

# Screening thresholds, not statistical significance tests.
MIN_PAIR_N = 10
MIN_TRIPLE_N = 8
MIN_BASE_N = 15


def save_csv(path, columns, rows):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)


def upload(path):
    key = "results/salary/" + path.name
    s3.upload_file(str(path), "hh-lake", key)

    info = s3.head_object(Bucket="hh-lake", Key=key)
    assert info["ContentLength"] == path.stat().st_size

    return "s3://hh-lake/" + key


def group_query(k):
    aliases = ["a", "b", "c"][:k]

    select_keys = [
        f"{alias}.skill AS skill_{i}"
        for i, alias in enumerate(aliases, 1)
    ]

    joins = ""

    if k >= 2:
        joins += """
        JOIN devops_skills b
          ON a.vacancy_id = b.vacancy_id
         AND a.skill < b.skill
        """

    if k >= 3:
        joins += """
        JOIN devops_skills c
          ON b.vacancy_id = c.vacancy_id
         AND b.skill < c.skill
        """

    group_by = ", ".join(
        f"{alias}.skill" for alias in aliases
    )

    return f"""
        SELECT
            {", ".join(select_keys)},
            COUNT(*) AS salary_n,
            MEDIAN(p.salary_proxy) AS median_rub
        FROM devops_skills a
        {joins}
        JOIN eligible_pay p
          ON a.vacancy_id = p.vacancy_id
        GROUP BY {group_by}
        ORDER BY {group_by}
    """


def main():
    started = time.perf_counter()
    logs = []

    def report(message=""):
        print(message, flush=True)
        logs.append(str(message))

    con = duckdb.connect(":memory:")

    try:
        report("=== DEVOPS SALARY INSIGHTS ===")
        report("DuckDB: " + duckdb.__version__)
        report("Source: s3://hh-lake/raw/vacancies.csv")

        con.execute("LOAD httpfs")

        con.execute(f"""
            CREATE SECRET rustfs_salary (
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

        script = (SQL / "salary_prepare.sql").read_text(
            encoding="utf-8-sig"
        )

        con.execute(script)

        source_n = con.execute(
            "SELECT COUNT(*) FROM source_devops"
        ).fetchone()[0]

        pay_n = con.execute(
            "SELECT COUNT(*) FROM eligible_pay"
        ).fetchone()[0]

        pay_skills_n = con.execute("""
            SELECT COUNT(DISTINCT p.vacancy_id)
            FROM eligible_pay p
            JOIN devops_skills s USING (vacancy_id)
        """).fetchone()[0]

        report("\n=== ANALYSIS COHORT ===")
        report(f"All DevOps vacancies: {source_n}")
        report(f"Comparable RUB net salaries: {pay_n}")
        report(f"Comparable salaries with skills: {pay_skills_n}")

        assert source_n == 5158
        assert pay_n == 762, (
            f"Expected 762 net salary records, got {pay_n}"
        )

        report("\n=== SALARY REPRESENTATION ===")

        for basis, count in con.execute("""
            SELECT salary_basis, COUNT(*)
            FROM eligible_pay
            GROUP BY salary_basis
            ORDER BY salary_basis
        """).fetchall():
            report(f"{basis}: {count}")

        overall = con.execute("""
            SELECT MEDIAN(salary_proxy)
            FROM eligible_pay
        """).fetchone()[0]

        report(f"Overall median salary proxy: {overall:,.0f} RUB")

        # Total vacancy frequencies, independent of salary presence.
        full_single = {
            skill: int(n)
            for skill, n in con.execute("""
                SELECT skill, COUNT(*)
                FROM devops_skills
                GROUP BY skill
            """).fetchall()
        }

        full_pair = {
            (a, b): int(n)
            for a, b, n in con.execute("""
                SELECT
                    a.skill,
                    b.skill,
                    COUNT(*)
                FROM devops_skills a
                JOIN devops_skills b
                  ON a.vacancy_id = b.vacancy_id
                 AND a.skill < b.skill
                GROUP BY a.skill, b.skill
            """).fetchall()
        }

        assert len(full_single) == 1560
        assert len(full_pair) == 22311

        salary_data = {}

        report("\n=== SALARY GROUPS ===")

        for k in (1, 2, 3):
            query = group_query(k)

            (SQL / f"salary_{k}.sql").write_text(
                query.strip() + ";\n",
                encoding="utf-8"
            )

            result = con.execute(query).fetchall()

            salary_data[k] = {
                tuple(row[:k]): (int(row[k]), float(row[k + 1]))
                for row in result
            }

            path = OUT / f"salary_{k}.csv"

            columns = (
                [f"skill_{i}" for i in range(1, k + 1)]
                + ["salary_n", "median_rub"]
            )

            save_csv(path, columns, [
                (*key, n, round(median, 2))
                for key, (n, median)
                in sorted(salary_data[k].items())
            ])

            report(f"Stack {k}: {len(result)} groups with salary")
            report("Uploaded: " + upload(path))

        report("\n=== COMPARISON WITH BASELINE ===")

        effects = {2: [], 3: []}

        for pair, (n, median) in salary_data[2].items():
            base_skill = sorted(
                pair,
                key=lambda s: (-full_single[s], s)
            )[0]

            base_n, base_median = salary_data[1][(base_skill,)]
            delta = median - base_median

            effects[2].append({
                "skills": pair,
                "salary_n": n,
                "median": median,
                "base": base_skill,
                "base_total": full_single[base_skill],
                "base_n": base_n,
                "base_median": base_median,
                "delta": delta,
                "delta_pct": 100 * delta / base_median,
            })

        for triple, (n, median) in salary_data[3].items():
            a, b, c = triple

            parents = [
                (a, b),
                (a, c),
                (b, c),
            ]

            base_pair = sorted(
                parents,
                key=lambda p: (-full_pair[p], p)
            )[0]

            base_n, base_median = salary_data[2][base_pair]
            delta = median - base_median

            effects[3].append({
                "skills": triple,
                "salary_n": n,
                "median": median,
                "base": " + ".join(base_pair),
                "base_total": full_pair[base_pair],
                "base_n": base_n,
                "base_median": base_median,
                "delta": delta,
                "delta_pct": 100 * delta / base_median,
            })

        columns = [
            "skills",
            "salary_n",
            "median_rub",
            "baseline",
            "baseline_total_vacancies",
            "baseline_salary_n",
            "baseline_median_rub",
            "delta_rub",
            "delta_percent",
        ]

        for k in (2, 3):
            path = OUT / f"effects_{k}.csv"

            ordered = sorted(
                effects[k],
                key=lambda x: (x["skills"])
            )

            save_csv(path, columns, [
                (
                    " + ".join(r["skills"]),
                    r["salary_n"],
                    round(r["median"], 2),
                    r["base"],
                    r["base_total"],
                    r["base_n"],
                    round(r["base_median"], 2),
                    round(r["delta"], 2),
                    round(r["delta_pct"], 2),
                )
                for r in ordered
            ])

            report(f"All effects stack {k}: {len(ordered)}")
            report("Uploaded: " + upload(path))

            minimum = MIN_PAIR_N if k == 2 else MIN_TRIPLE_N

            eligible = [
                r for r in effects[k]
                if r["salary_n"] >= minimum
                and r["base_n"] >= MIN_BASE_N
            ]

            report(
                f"\n=== STACK {k}: ELIGIBLE COMBINATIONS ==="
            )
            report(
                f"Minimum group n={minimum}, "
                f"baseline n={MIN_BASE_N}"
            )
            report(f"Eligible combinations: {len(eligible)}")

            if not eligible:
                report("No combinations meet screening thresholds")
                continue

            positive = sorted(
                (r for r in eligible if r["delta"] > 0),
                key=lambda r: (-r["delta"], r["skills"])
            )[:10]

            negative = sorted(
                (r for r in eligible if r["delta"] < 0),
                key=lambda r: (r["delta"], r["skills"])
            )[:10]

            for title, items in [
                ("TOP POSITIVE", positive),
                ("TOP NEGATIVE", negative),
            ]:
                report(f"\n{title} STACK {k}")

                for r in items:
                    report(
                        f"{' + '.join(r['skills'])} | "
                        f"n={r['salary_n']} | "
                        f"median={r['median']:,.0f} RUB | "
                        f"baseline={r['base']} | "
                        f"base_n={r['base_n']} | "
                        f"base_median={r['base_median']:,.0f} | "
                        f"delta={r['delta']:+,.0f} RUB "
                        f"({r['delta_pct']:+.1f}%)"
                    )

        report("\n=== FINAL ===")
        report(f"Elapsed: {time.perf_counter()-started:.3f} s")
        report("SALARY ANALYSIS COMPLETED")

    finally:
        con.close()
        (EVIDENCE / "salary_insights.txt").write_text(
            "\n".join(logs) + "\n",
            encoding="utf-8"
        )


if __name__ == "__main__":
    main()
