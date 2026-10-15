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
        WHERE TRIM(v.area) IN ('Москва', 'Санкт-Петербург', 'Новосибирск')
        GROUP BY TRIM(v.area)
        ORDER BY TRIM(v.area);
