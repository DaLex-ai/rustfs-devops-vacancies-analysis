WITH frequencies AS (
        SELECT
            a.region,
            a.skill AS skill_1,
            COUNT(*) AS vacancy_count
        FROM regional_skills a
        
        GROUP BY a.region, a.skill
    ),
    ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY region
                ORDER BY vacancy_count DESC, skill_1
            ) AS rank_n
        FROM frequencies
    )
    SELECT
        region, rank_n,
        skill_1,
        vacancy_count
    FROM ranked
    WHERE rank_n <= 10
    ORDER BY region, rank_n;
