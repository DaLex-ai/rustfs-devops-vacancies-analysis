SELECT
    skill AS skill_1,
    COUNT(*) AS count
FROM devops_skills
GROUP BY skill
ORDER BY skill_1
