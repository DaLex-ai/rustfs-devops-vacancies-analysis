"""Verify curated vacancy rows and full pair counts against homework 3 without ZooKeeper."""
import csv
import hashlib
from collections import Counter
from itertools import combinations
from pathlib import Path

here = Path(__file__).resolve().parent
curated = here.parent / "results" / "optimized" / "devops_skills.csv"
reference = here.parent / "results" / "mapreduce" / "skills_2.csv"
assert curated.exists() and reference.exists()
sha = hashlib.sha256(curated.read_bytes()).hexdigest()
expected_sha = "541174a6d1f5a80a949dfa7a4255934ea3659a6aa850c6e859892a5a6c2e9093"
assert sha == expected_sha, f"Input SHA256 mismatch: {sha}"
counts = Counter()
with curated.open(encoding="utf-8-sig", newline="") as fh:
    rows = list(csv.DictReader(fh))
for row in rows:
    skills = sorted({s.strip() for s in (row["key_skills"] or "").split(";") if s.strip()})
    counts.update(combinations(skills, 2))
actual = (len(counts), sum(counts.values()))
assert len(rows) == 5158 and actual == (22311, 134786), (len(rows), actual)
baseline = Counter()
with reference.open(encoding="utf-8-sig", newline="") as fh:
    for row in csv.DictReader(fh):
        key = (row["skill_1"], row["skill_2"])
        assert key not in baseline
        baseline[key] = int(row["count"])
assert counts == baseline, "Per-pair comparison failed"
log = f"DevOps rows: {len(rows)}\nInput SHA256: {sha}\nUnique pairs: {actual[0]}\nOccurrences: {actual[1]}\nAll {actual[0]} pair counts: EXACT MATCH\nOFFLINE_REFERENCE_CHECK_PASSED\n"
out = here / "evidence"
out.mkdir(exist_ok=True)
(out / "offline_reference_test.txt").write_text(log, encoding="utf-8")
print(log)
