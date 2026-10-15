import csv
import io
import re
from collections import Counter

from check_storage import s3

BUCKET = "hh-lake"
KEY = "raw/vacancies.csv"

print("=== DATASET INSPECTION ===", flush=True)

response = s3.get_object(Bucket=BUCKET, Key=KEY)
body = response["Body"]

csv.field_size_limit(20 * 1024 * 1024)

role_counts = Counter()
devops_title_roles = Counter()
devops_examples = []

total = 0
devops_in_role = 0
devops_in_title = 0
salary_present = 0

pattern = re.compile(
    r"dev[\s-]*ops|девопс|site reliability|\bsre\b",
    re.IGNORECASE,
)

try:
    with io.TextIOWrapper(
        body,
        encoding="utf-8-sig",
        newline=""
    ) as stream:

        reader = csv.DictReader(stream)

        print("Columns:", reader.fieldnames, flush=True)

        required = {
            "name", "role", "area",
            "salary_from", "salary_to",
            "currency", "key_skills"
        }

        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing columns: {missing}")

        for row in reader:
            total += 1

            role = (row.get("role") or "").strip()
            name = (row.get("name") or "").strip()

            role_counts[role] += 1

            if pattern.search(role):
                devops_in_role += 1

            if pattern.search(name):
                devops_in_title += 1
                devops_title_roles[role] += 1

                if len(devops_examples) < 12:
                    devops_examples.append((name, role))

            if row.get("salary_from") or row.get("salary_to"):
                salary_present += 1

            if total % 100000 == 0:
                print(
                    f"Processed: {total:,} vacancies",
                    flush=True
                )
finally:
    body.close()

print("\n=== TOTAL ===")
print("Total vacancies:", total)
print("With salary fields:", salary_present)
print("DevOps/SRE in role:", devops_in_role)
print("DevOps/SRE in title:", devops_in_title)

print("\n=== TOP 20 ROLES ===")
for role, count in role_counts.most_common(20):
    print(f"{count:>8} | {role}")

print("\n=== ROLES OF DEVOPS/SRE TITLES ===")
for role, count in devops_title_roles.most_common(20):
    print(f"{count:>8} | {role}")

print("\n=== EXAMPLE VACANCIES ===")
for name, role in devops_examples:
    print(f"Name: {name}")
    print(f"Role: {role}")
    print("---")

print("\nINSPECTION COMPLETED")
