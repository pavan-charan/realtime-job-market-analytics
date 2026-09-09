-- ==============================================================================
-- 20 Analytical Business Intelligence SQL Queries for Hive Data Warehouse
-- Database: job_market_dw
-- ==============================================================================

USE job_market_dw;

-- ------------------------------------------------------------------------------
-- Query 1: Overall Job Market Summary KPI Card
-- Calculates total postings, average annual salary, distinct companies, and overall remote percentage.
-- ------------------------------------------------------------------------------
SELECT
    COUNT(f.job_id) AS total_job_postings,
    ROUND(AVG(f.annual_salary_usd), 2) AS avg_annual_salary_usd,
    COUNT(DISTINCT f.company_key) AS total_hiring_companies,
    ROUND(COUNT(CASE WHEN f.is_remote = true THEN 1 END) * 100.0 / COUNT(f.job_id), 2) AS overall_remote_pct
FROM fact_job_postings f;


-- ------------------------------------------------------------------------------
-- Query 2: Top 15 Most In-Demand Skills Across All Tech Postings
-- Counts occurrences of individual skills using the Bridge and Skill dimensions.
-- ------------------------------------------------------------------------------
SELECT
    s.skill_name,
    COUNT(b.job_id) AS total_mentions,
    ROUND(AVG(f.annual_salary_usd), 2) AS avg_salary_with_skill,
    ROUND(COUNT(CASE WHEN f.is_remote = true THEN 1 END) * 100.0 / COUNT(b.job_id), 2) AS remote_pct
FROM bridge_job_skills b
JOIN dim_skill s ON b.skill_key = s.skill_key
JOIN fact_job_postings f ON b.job_id = f.job_id
GROUP BY s.skill_name
ORDER BY total_mentions DESC
LIMIT 15;


-- ------------------------------------------------------------------------------
-- Query 3: Highest Paying Tech Skills (Filtered for Statistical Significance >= 50 Postings)
-- Identifies premium skills commanding top-tier salaries in the market.
-- ------------------------------------------------------------------------------
SELECT
    s.skill_name,
    COUNT(b.job_id) AS sample_size,
    ROUND(AVG(f.annual_salary_usd), 2) AS avg_salary_usd,
    ROUND(MIN(f.annual_salary_usd), 2) AS min_salary_usd,
    ROUND(MAX(f.annual_salary_usd), 2) AS max_salary_usd
FROM bridge_job_skills b
JOIN dim_skill s ON b.skill_key = s.skill_key
JOIN fact_job_postings f ON b.job_id = f.job_id
WHERE f.annual_salary_usd IS NOT NULL
GROUP BY s.skill_name
HAVING COUNT(b.job_id) >= 50
ORDER BY avg_salary_usd DESC
LIMIT 15;


-- ------------------------------------------------------------------------------
-- Query 4: Salary Benchmark by Job Role Category
-- Compares average, min, and max compensation across major engineering disciplines.
-- ------------------------------------------------------------------------------
SELECT
    r.role_category,
    COUNT(f.job_id) AS total_openings,
    ROUND(AVG(f.annual_salary_usd), 2) AS avg_salary_usd,
    ROUND(MIN(f.annual_salary_usd), 2) AS min_salary_usd,
    ROUND(MAX(f.annual_salary_usd), 2) AS max_salary_usd
FROM fact_job_postings f
JOIN dim_job_role r ON f.role_key = r.role_key
WHERE f.annual_salary_usd IS NOT NULL
GROUP BY r.role_category
ORDER BY avg_salary_usd DESC;


-- ------------------------------------------------------------------------------
-- Query 5: Seniority & Experience Level Salary Progression
-- Measures salary growth from Entry level up to Lead / Principal roles.
-- ------------------------------------------------------------------------------
SELECT
    r.experience_level,
    COUNT(f.job_id) AS role_count,
    ROUND(AVG(f.annual_salary_usd), 2) AS avg_salary_usd,
    ROUND(AVG(f.skills_count), 1) AS avg_skills_required
FROM fact_job_postings f
JOIN dim_job_role r ON f.role_key = r.role_key
WHERE f.annual_salary_usd IS NOT NULL
GROUP BY r.experience_level
ORDER BY avg_salary_usd ASC;


-- ------------------------------------------------------------------------------
-- Query 6: Top 10 Tech Hiring Hubs (Cities with High Demand & Compensation)
-- ------------------------------------------------------------------------------
SELECT
    l.city,
    l.country,
    COUNT(f.job_id) AS total_jobs,
    ROUND(AVG(f.annual_salary_usd), 2) AS avg_salary_usd,
    ROUND(COUNT(CASE WHEN f.is_remote = true THEN 1 END) * 100.0 / COUNT(f.job_id), 2) AS remote_pct
FROM fact_job_postings f
JOIN dim_location l ON f.location_key = l.location_key
WHERE l.city != 'Remote' AND l.city != 'Unspecified'
GROUP BY l.city, l.country
ORDER BY total_jobs DESC
LIMIT 10;


-- ------------------------------------------------------------------------------
-- Query 7: Remote vs. Onsite Hiring Penetration by Role Category
-- Analyzes which technical fields offer the greatest remote flexibility.
-- ------------------------------------------------------------------------------
SELECT
    r.role_category,
    COUNT(CASE WHEN f.is_remote = true THEN 1 END) AS remote_jobs,
    COUNT(CASE WHEN f.is_remote = false THEN 1 END) AS onsite_jobs,
    ROUND(COUNT(CASE WHEN f.is_remote = true THEN 1 END) * 100.0 / COUNT(f.job_id), 2) AS remote_pct
FROM fact_job_postings f
JOIN dim_job_role r ON f.role_key = r.role_key
GROUP BY r.role_category
ORDER BY remote_pct DESC;


-- ------------------------------------------------------------------------------
-- Query 8: Top 20 Most Active Hiring Companies
-- Measures hiring volume, remote policy, and average compensation by employer.
-- ------------------------------------------------------------------------------
SELECT
    c.company_name,
    c.company_industry,
    COUNT(f.job_id) AS active_postings,
    ROUND(AVG(f.annual_salary_usd), 2) AS avg_salary_usd,
    ROUND(COUNT(CASE WHEN f.is_remote = true THEN 1 END) * 100.0 / COUNT(f.job_id), 2) AS remote_friendly_pct
FROM fact_job_postings f
JOIN dim_company c ON f.company_key = c.company_key
GROUP BY c.company_name, c.company_industry
ORDER BY active_postings DESC
LIMIT 20;


-- ------------------------------------------------------------------------------
-- Query 9: Remote Salary Premium / Discount Analysis
-- Evaluates whether remote positions pay more or less than onsite positions per role.
-- ------------------------------------------------------------------------------
SELECT
    r.role_category,
    ROUND(AVG(CASE WHEN f.is_remote = true THEN f.annual_salary_usd END), 2) AS avg_remote_salary,
    ROUND(AVG(CASE WHEN f.is_remote = false THEN f.annual_salary_usd END), 2) AS avg_onsite_salary,
    ROUND(
        AVG(CASE WHEN f.is_remote = true THEN f.annual_salary_usd END) -
        AVG(CASE WHEN f.is_remote = false THEN f.annual_salary_usd END), 2
    ) AS remote_salary_delta
FROM fact_job_postings f
JOIN dim_job_role r ON f.role_key = r.role_key
WHERE f.annual_salary_usd IS NOT NULL
GROUP BY r.role_category
ORDER BY remote_salary_delta DESC;


-- ------------------------------------------------------------------------------
-- Query 10: Monthly Hiring Volume & Salary Trajectory Trend
-- Longitudinal timeline of hiring velocity and salary shifts.
-- ------------------------------------------------------------------------------
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(f.job_id) AS total_postings,
    ROUND(AVG(f.annual_salary_usd), 2) AS avg_salary_usd
FROM fact_job_postings f
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year > 2000
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- ------------------------------------------------------------------------------
-- Query 11: Top Skills Required Specifically for Data Engineering & Data Science
-- ------------------------------------------------------------------------------
SELECT
    r.role_category,
    s.skill_name,
    COUNT(b.job_id) AS skill_demand_count,
    ROUND(AVG(f.annual_salary_usd), 2) AS avg_salary
FROM bridge_job_skills b
JOIN dim_skill s ON b.skill_key = s.skill_key
JOIN fact_job_postings f ON b.job_id = f.job_id
JOIN dim_job_role r ON f.role_key = r.role_key
WHERE r.role_category IN ('Data Engineering', 'Data Science & AI')
GROUP BY r.role_category, s.skill_name
ORDER BY r.role_category, skill_demand_count DESC
LIMIT 20;


-- ------------------------------------------------------------------------------
-- Query 12: High-Paying Remote Opportunities by Category (> $150k USD)
-- ------------------------------------------------------------------------------
SELECT
    r.role_category,
    COUNT(f.job_id) AS high_paying_remote_jobs,
    ROUND(AVG(f.annual_salary_usd), 2) AS avg_salary_usd
FROM fact_job_postings f
JOIN dim_job_role r ON f.role_key = r.role_key
WHERE f.is_remote = true AND f.annual_salary_usd >= 150000.0
GROUP BY r.role_category
ORDER BY high_paying_remote_jobs DESC;


-- ------------------------------------------------------------------------------
-- Query 13: Experience Level Distribution in Top 5 Hiring Tech Companies
-- ------------------------------------------------------------------------------
SELECT
    c.company_name,
    r.experience_level,
    COUNT(f.job_id) AS total_jobs
FROM fact_job_postings f
JOIN dim_company c ON f.company_key = c.company_key
JOIN dim_job_role r ON f.role_key = r.role_key
WHERE c.company_name IN (
    SELECT company_name FROM (
        SELECT company_name, COUNT(job_id) as cnt FROM fact_job_postings f2
        JOIN dim_company c2 ON f2.company_key = c2.company_key
        GROUP BY company_name ORDER BY cnt DESC LIMIT 5
    ) top_companies
)
GROUP BY c.company_name, r.experience_level
ORDER BY c.company_name, total_jobs DESC;


-- ------------------------------------------------------------------------------
-- Query 14: Skills with the Highest Remote Opportunity Percentage
-- (Filtered for skills with >= 30 mentions)
-- ------------------------------------------------------------------------------
SELECT
    s.skill_name,
    COUNT(b.job_id) AS total_postings,
    ROUND(COUNT(CASE WHEN f.is_remote = true THEN 1 END) * 100.0 / COUNT(b.job_id), 2) AS remote_percentage
FROM bridge_job_skills b
JOIN dim_skill s ON b.skill_key = s.skill_key
JOIN fact_job_postings f ON b.job_id = f.job_id
GROUP BY s.skill_name
HAVING COUNT(b.job_id) >= 30
ORDER BY remote_percentage DESC
LIMIT 15;


-- ------------------------------------------------------------------------------
-- Query 15: Quarter-over-Quarter (QoQ) Hiring Volume by Tech Category
-- ------------------------------------------------------------------------------
SELECT
    d.year,
    d.quarter,
    r.role_category,
    COUNT(f.job_id) AS quarterly_openings
FROM fact_job_postings f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_job_role r ON f.role_key = r.role_key
WHERE d.year > 2000
GROUP BY d.year, d.quarter, r.role_category
ORDER BY d.year, d.quarter, quarterly_openings DESC;


-- ------------------------------------------------------------------------------
-- Query 16: Multi-Skill Salary Premium (Impact of Skill Stack Breadth)
-- Evaluates whether jobs demanding more skills offer higher compensation.
-- ------------------------------------------------------------------------------
SELECT
    CASE
        WHEN f.skills_count = 0 THEN '0 Skills Listed'
        WHEN f.skills_count BETWEEN 1 AND 2 THEN '1-2 Skills (Low)'
        WHEN f.skills_count BETWEEN 3 AND 5 THEN '3-5 Skills (Medium)'
        WHEN f.skills_count BETWEEN 6 AND 10 THEN '6-10 Skills (High)'
        ELSE '10+ Skills (Extensive)'
    END AS skill_breadth_tier,
    COUNT(f.job_id) AS job_count,
    ROUND(AVG(f.annual_salary_usd), 2) AS avg_annual_salary
FROM fact_job_postings f
WHERE f.annual_salary_usd IS NOT NULL
GROUP BY
    CASE
        WHEN f.skills_count = 0 THEN '0 Skills Listed'
        WHEN f.skills_count BETWEEN 1 AND 2 THEN '1-2 Skills (Low)'
        WHEN f.skills_count BETWEEN 3 AND 5 THEN '3-5 Skills (Medium)'
        WHEN f.skills_count BETWEEN 6 AND 10 THEN '6-10 Skills (High)'
        ELSE '10+ Skills (Extensive)'
    END
ORDER BY avg_annual_salary DESC;


-- ------------------------------------------------------------------------------
-- Query 17: Industry Sector vs Average Offered Salary
-- ------------------------------------------------------------------------------
SELECT
    c.company_industry,
    COUNT(f.job_id) AS total_jobs,
    ROUND(AVG(f.annual_salary_usd), 2) AS avg_salary_usd
FROM fact_job_postings f
JOIN dim_company c ON f.company_key = c.company_key
WHERE f.annual_salary_usd IS NOT NULL AND c.company_industry IS NOT NULL
GROUP BY c.company_industry
ORDER BY avg_salary_usd DESC
LIMIT 15;


-- ------------------------------------------------------------------------------
-- Query 18: Salary Variance and Spread by Role (Max - Min Dispersion)
-- ------------------------------------------------------------------------------
SELECT
    r.role_category,
    ROUND(AVG(f.annual_salary_usd), 2) AS avg_salary,
    ROUND(MAX(f.annual_salary_usd) - MIN(f.annual_salary_usd), 2) AS salary_spread,
    ROUND(STDDEV(f.annual_salary_usd), 2) AS salary_std_dev
FROM fact_job_postings f
JOIN dim_job_role r ON f.role_key = r.role_key
WHERE f.annual_salary_usd IS NOT NULL
GROUP BY r.role_category
ORDER BY salary_std_dev DESC;


-- ------------------------------------------------------------------------------
-- Query 19: Full-Time vs Contract vs Part-Time Compensation & Volume
-- ------------------------------------------------------------------------------
SELECT
    f.employment_type,
    COUNT(f.job_id) AS posting_count,
    ROUND(AVG(f.annual_salary_usd), 2) AS avg_salary_usd,
    ROUND(COUNT(CASE WHEN f.is_remote = true THEN 1 END) * 100.0 / COUNT(f.job_id), 2) AS remote_pct
FROM fact_job_postings f
GROUP BY f.employment_type
ORDER BY posting_count DESC;


-- ------------------------------------------------------------------------------
-- Query 20: Executive & Leadership Hiring Across Top Tech Locations
-- ------------------------------------------------------------------------------
SELECT
    l.city,
    r.role_category,
    COUNT(f.job_id) AS leadership_openings,
    ROUND(AVG(f.annual_salary_usd), 2) AS avg_leadership_salary
FROM fact_job_postings f
JOIN dim_location l ON f.location_key = l.location_key
JOIN dim_job_role r ON f.role_key = r.role_key
WHERE r.experience_level = 'Lead / Principal' AND l.city != 'Remote'
GROUP BY l.city, r.role_category
ORDER BY leadership_openings DESC
LIMIT 15;
