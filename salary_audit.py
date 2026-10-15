import csv
import io
import math
from collections import Counter, defaultdict
from pathlib import Path

from check_storage import s3

BUCKET = "hh-lake"
KEY = "raw/vacancies.csv"
ROLE = "DevOps-инженер"

csv.field_size_limit(20 * 1024 * 1024)

total = 0
salary_rows = 0
bounds = Counter()
currencies = Counter()
gross_rur = Counter()
regions = defaultdict(Counter)
problems = Counter()
examples = []

rur_valid = 0
rur_valid_with_skills = 0


def parse_amount(value):
    text = (value or "").strip().replace(" ", "")
    text = text.replace("\u00a0", "").replace(",", ".")

    if not text:
        return None

    try:
        number = float(text)
    except ValueError:
        raise ValueError("non-numeric salary")

    if not math.isfinite(number) or number <= 0:
        raise ValueError("non-positive or non-finite salary")

    return number


response = s3.get_object(Bucket=BUCKET, Key=KEY)
body = response["Body"]

try:
    with io.TextIOWrapper(
        body, encoding="utf-8-sig", newline=""
    ) as stream:
        reader = csv.DictReader(stream)

        for row in reader:
            if (row.get("role") or "").strip() != ROLE:
                continue

            total += 1

            region = (row.get("area") or "").strip() or "(empty)"
            regions[region]["total"] += 1

            raw_from = (row.get("salary_from") or "").strip()
            raw_to = (row.get("salary_to") or "").strip()

            if not raw_from and not raw_to:
                bounds["missing_both"] += 1
                continue

            salary_rows += 1
            regions[region]["salary_any"] += 1

            if raw_from and raw_to:
                bounds["both"] += 1
            elif raw_from:
                bounds["only_from"] += 1
            else:
                bounds["only_to"] += 1

            currency = (row.get("currency") or "").strip()
            currency = currency or "(empty)"
            currencies[currency] += 1

            gross = (row.get("gross") or "").strip().lower()
            gross = gross or "(empty)"

            if currency in ("RUR", "RUB"):
                gross_rur[gross] += 1

            try:
                lower = parse_amount(raw_from)
                upper = parse_amount(raw_to)

                if lower is not None and upper is not None:
                    if lower > upper:
                        raise ValueError("salary_from > salary_to")

            except ValueError as exc:
                problems[str(exc)] += 1

                if len(examples) < 8:
                    examples.append(
                        (
                            row.get("vacancy_id"),
                            raw_from,
                            raw_to,
                            str(exc),
                        )
                    )
                continue

            if currency not in ("RUR", "RUB"):
                continue

            rur_valid += 1
            regions[region]["rur_valid"] += 1
            regions[region]["rur_" + gross] += 1

            if (row.get("key_skills") or "").strip():
                rur_valid_with_skills += 1

finally:
    body.close()


lines = []


def report(text=""):
    lines.append(str(text))
    print(text, flush=True)


report("=== DEVOPS SALARY AUDIT ===")
report(f"DevOps vacancies: {total}")
report(f"Vacancies with salary: {salary_rows}")
report(f"Vacancies without salary: {total - salary_rows}")

report("\n=== SALARY BOUNDS ===")
for key in ("both", "only_from", "only_to", "missing_both"):
    report(f"{key}: {bounds[key]}")

report("\n=== CURRENCIES ===")
for key, value in currencies.most_common():
    report(f"{key}: {value}")

report("\n=== GROSS FOR RUR/RUB ===")
for key, value in gross_rur.most_common():
    report(f"{key}: {value}")

report("\n=== VALID RUB SALARIES ===")
report(f"Valid RUR/RUB rows: {rur_valid}")
report(f"Valid RUR/RUB rows with skills: {rur_valid_with_skills}")

report("\n=== DATA QUALITY PROBLEMS ===")
if problems:
    for key, value in problems.items():
        report(f"{key}: {value}")
    report(f"Examples: {examples}")
else:
    report("No invalid positive salary bounds found")

report("\n=== TOP 15 REGIONS ===")
for region, data in sorted(
    regions.items(),
    key=lambda x: (-x[1]["total"], x[0])
)[:15]:
    report(
        f"{region}: "
        f"total={data['total']}, "
        f"salary_any={data['salary_any']}, "
        f"valid_RUB={data['rur_valid']}, "
        f"gross_false={data['rur_false']}, "
        f"gross_true={data['rur_true']}, "
        f"gross_empty={data['rur_(empty)']}"
    )

report("\nSALARY AUDIT COMPLETED")

evidence = Path(__file__).resolve().parent / "evidence"
evidence.mkdir(exist_ok=True)

(evidence / "salary_audit.txt").write_text(
    "\n".join(lines) + "\n",
    encoding="utf-8",
)
