import csv
import io
from math import comb
from collections import Counter

from check_storage import s3

BUCKET = "hh-lake"
KEY = "raw/vacancies.csv"
TARGET_ROLE = "DevOps-инженер"

csv.field_size_limit(20 * 1024 * 1024)

regions = Counter()
regions_with_salary = Counter()
currencies = Counter()
skills_counter = Counter()
skill_sizes = Counter()
vacancy_ids = Counter()

salary_count = 0
missing_skills = 0
duplicate_skills = 0
total = 0

combinations = {1: 0, 2: 0, 3: 0}

response = s3.get_object(Bucket=BUCKET, Key=KEY)
body = response["Body"]

try:
    with io.TextIOWrapper(
        body, encoding="utf-8-sig", newline=""
    ) as stream:

        reader = csv.DictReader(stream)

        for row in reader:
            role = (row.get("role") or "").strip()

            if role != TARGET_ROLE:
                continue

            total += 1

            vacancy_id = (row.get("vacancy_id") or "").strip()
            if vacancy_id:
                vacancy_ids[vacancy_id] += 1

            region = (row.get("area") or "").strip()
            regions[region or "(empty)"] += 1

            salary_from = (row.get("salary_from") or "").strip()
            salary_to = (row.get("salary_to") or "").strip()

            if salary_from or salary_to:
                salary_count += 1
                regions_with_salary[region or "(empty)"] += 1
                currencies[
                    (row.get("currency") or "").strip() or "(empty)"
                ] += 1

            raw_skills = [
                skill.strip()
                for skill in (row.get("key_skills") or "").split(";")
                if skill.strip()
            ]

            skills = set(raw_skills)

            duplicate_skills += len(raw_skills) - len(skills)

            n = len(skills)
            skill_sizes[n] += 1

            if not skills:
                missing_skills += 1

            skills_counter.update(skills)

            for k in (1, 2, 3):
                if n >= k:
                    combinations[k] += comb(n, k)
finally:
    body.close()

repeated_ids = {
    key: value
    for key, value in vacancy_ids.items()
    if value > 1
}

print("\n=== DEVOPS SAMPLE ===")
print("Total DevOps rows:", total)
print("Unique non-empty vacancy IDs:", len(vacancy_ids))
print("Rows with missing vacancy ID:", total - sum(vacancy_ids.values()))
print("Repeated vacancy IDs:", len(repeated_ids))
print("Extra rows from repeated IDs:",
      sum(v - 1 for v in repeated_ids.values()))
print("Duplicate ID examples:", list(repeated_ids.items())[:10])

print("\n=== SKILLS ===")
print("Vacancies without skills:", missing_skills)
print("Vacancies with skills:", total - missing_skills)
print("Unique skill names:", len(skills_counter))
print("Duplicate skill entries within rows:", duplicate_skills)

print("\nSkills per vacancy:")
for n, count in sorted(skill_sizes.items()):
    print(f"{n:>3} skills: {count}")

print("\n=== COMBINATIONS ===")
for k, value in combinations.items():
    print(f"Stack size {k}: {value} generated combinations")

print("\n=== TOP 20 SKILLS ===")
for skill, count in skills_counter.most_common(20):
    print(f"{count:>6} | {skill}")

print("\n=== SALARIES ===")
print("Vacancies with salary:", salary_count)
print("Vacancies without salary:", total - salary_count)
print("Currencies:")
for currency, count in currencies.most_common():
    print(f"{currency}: {count}")

print("\n=== TOP 15 REGIONS ===")
for region, count in regions.most_common(15):
    with_salary = regions_with_salary[region]
    print(
        f"{region}: {count} vacancies, "
        f"{with_salary} with salary"
    )

print("\nPROFILE COMPLETED")
