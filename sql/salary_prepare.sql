-- Variant 11: DevOps engineer
-- Source: RustFS / hh-lake / raw/vacancies.csv

CREATE TEMP TABLE source_devops AS
SELECT
    vacancy_id,
    area,
    key_skills,
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
WHERE TRIM(COALESCE(role, '')) = 'DevOps-инженер';

CREATE TEMP TABLE devops_skills AS
SELECT DISTINCT
    v.vacancy_id,
    TRIM(x.skill) AS skill
FROM source_devops AS v
CROSS JOIN UNNEST(
    STRING_SPLIT(COALESCE(v.key_skills, ''), ';')
) AS x(skill)
WHERE TRIM(x.skill) <> '';

-- Main analysis: RUB, net salary only.
CREATE TEMP TABLE salary_candidates AS
SELECT
    vacancy_id,
    area,
    TRY_CAST(
        REPLACE(TRIM(salary_from), ',', '.')
        AS DOUBLE
    ) AS lower_bound,
    TRY_CAST(
        REPLACE(TRIM(salary_to), ',', '.')
        AS DOUBLE
    ) AS upper_bound
FROM source_devops
WHERE UPPER(TRIM(COALESCE(currency, ''))) IN ('RUR', 'RUB')
  AND LOWER(TRIM(COALESCE(gross, ''))) = 'false';

CREATE TEMP TABLE eligible_pay AS
SELECT
    vacancy_id,
    area,
    CASE
        WHEN lower_bound IS NOT NULL
         AND upper_bound IS NOT NULL
            THEN (lower_bound + upper_bound) / 2
        ELSE COALESCE(lower_bound, upper_bound)
    END AS salary_proxy,
    CASE
        WHEN lower_bound IS NOT NULL
         AND upper_bound IS NOT NULL THEN 'midpoint'
        WHEN lower_bound IS NOT NULL THEN 'lower_only'
        ELSE 'upper_only'
    END AS salary_basis
FROM salary_candidates
WHERE (lower_bound IS NOT NULL OR upper_bound IS NOT NULL)
  AND (lower_bound IS NULL OR lower_bound > 0)
  AND (upper_bound IS NULL OR upper_bound > 0)
  AND (
      lower_bound IS NULL
      OR upper_bound IS NULL
      OR lower_bound <= upper_bound
  );
