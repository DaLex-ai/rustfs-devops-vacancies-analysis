SELECT
            a.skill AS skill_1,
            COUNT(*) AS salary_n,
            MEDIAN(p.salary_proxy) AS median_rub
        FROM devops_skills a
        
        JOIN eligible_pay p
          ON a.vacancy_id = p.vacancy_id
        GROUP BY a.skill
        ORDER BY a.skill;
