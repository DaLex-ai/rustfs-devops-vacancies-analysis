WITH frequencies AS (
        SELECT
            a.region,
            a.skill AS skill_1, b.skill AS skill_2,
            COUNT(*) AS vacancy_count
        FROM regional_skills a
        
        JOIN regional_skills b
          ON a.vacancy_id = b.vacancy_id
         AND a.region = b.region
         AND a.skill < b.skill
        
        GROUP BY a.region, a.skill, b.skill
    ),
    ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY region
                ORDER BY vacancy_count DESC, skill_1, skill_2
            ) AS rank_n
        FROM frequencies
    )
    SELECT
        region, rank_n,
        skill_1, skill_2,
        vacancy_count
    FROM ranked
    WHERE rank_n <= 10
    ORDER BY region, rank_n;
