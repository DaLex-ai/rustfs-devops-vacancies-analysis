SELECT
    a.skill AS skill_1,
    b.skill AS skill_2,
    COUNT(*) AS count
FROM devops_skills AS a
JOIN devops_skills AS b
    ON a.vacancy_id = b.vacancy_id
    AND a.skill < b.skill
GROUP BY a.skill, b.skill
ORDER BY skill_1, skill_2
