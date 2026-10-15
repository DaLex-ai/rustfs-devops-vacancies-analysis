-- Variant 11: DevOps engineer
-- Source: s3://hh-lake/raw/vacancies.csv
-- The S3 secret is configured separately in Python.

CREATE TEMP TABLE devops_rows AS
SELECT
    vacancy_id,
    key_skills
FROM read_csv(
    's3://hh-lake/raw/vacancies.csv',
    header = true,
    all_varchar = true,
    delim = ',',
    quote = '"',
    escape = '"',
    max_line_size = 20000000
)
WHERE TRIM(COALESCE(role, '')) = 'DevOps-инженер';

CREATE TEMP TABLE devops_skills AS
SELECT DISTINCT
    v.vacancy_id,
    TRIM(x.skill) AS skill
FROM devops_rows AS v
CROSS JOIN UNNEST(
    STRING_SPLIT(COALESCE(v.key_skills, ''), ';')
) AS x(skill)
WHERE TRIM(x.skill) <> '';
