SELECT
            a.skill AS skill_1, b.skill AS skill_2, c.skill AS skill_3,
            COUNT(*) AS salary_n,
            MEDIAN(p.salary_proxy) AS median_rub
        FROM devops_skills a
        
        JOIN devops_skills b
          ON a.vacancy_id = b.vacancy_id
         AND a.skill < b.skill
        
        JOIN devops_skills c
          ON b.vacancy_id = c.vacancy_id
         AND b.skill < c.skill
        
        JOIN eligible_pay p
          ON a.vacancy_id = p.vacancy_id
        GROUP BY a.skill, b.skill, c.skill
        ORDER BY a.skill, b.skill, c.skill;
