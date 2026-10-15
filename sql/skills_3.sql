SELECT
    a.skill AS skill_1,
    b.skill AS skill_2,
    c.skill AS skill_3,
    COUNT(*) AS count
FROM devops_skills AS a
JOIN devops_skills AS b
    ON a.vacancy_id = b.vacancy_id
    AND a.skill < b.skill
JOIN devops_skills AS c
    ON b.vacancy_id = c.vacancy_id
    AND b.skill < c.skill
GROUP BY a.skill, b.skill, c.skill
ORDER BY skill_1, skill_2, skill_3
