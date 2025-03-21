-- EDA

-- Retrieve the first 10 rows to understand the dataset structure
SELECT *
FROM layoffs_staging2
LIMIT 10;

-- Count the total number of records
SELECT COUNT(*) AS total_records
FROM layoffs_staging2;

-- Check for missing values by counting NULLs in each column
SELECT 
    SUM(CASE WHEN company IS NULL THEN 1 ELSE 0 END) AS missing_companies,
    SUM(CASE WHEN industry IS NULL THEN 1 ELSE 0 END) AS missing_industries,
    SUM(CASE WHEN total_laid_off IS NULL THEN 1 ELSE 0 END) AS missing_laid_off
FROM layoffs_staging2;

-- Get the total number of layoffs and average layoffs per company
SELECT 
    COUNT(DISTINCT company) AS total_companies,
    SUM(total_laid_off) AS total_laid_off,
    AVG(total_laid_off) AS avg_laid_off
FROM layoffs_staging2;

-- Identify the company with the highest number of layoffs
SELECT company, total_laid_off 
FROM layoffs_staging2
ORDER BY total_laid_off DESC
LIMIT 1;

-- Count how many layoffs occurred in each industry
SELECT industry, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY industry
ORDER BY total_laid_off DESC;

-- Count the number of layoffs per year
SELECT YEAR(date) AS year, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY YEAR(date)
ORDER BY year;

-- Identify which month had the highest layoffs
SELECT MONTHNAME(date) AS month, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY MONTHNAME(date)
ORDER BY total_laid_off DESC
LIMIT 1;

-- Get the number of layoffs per year and industry
SELECT YEAR(date) AS year, industry, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY YEAR(date), industry
ORDER BY year, total_laid_off DESC;

-- Find the top 5 countries with the most layoffs
SELECT country, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY country
ORDER BY total_laid_off DESC
LIMIT 5;

-- Find the funding stage with the highest layoffs
SELECT stage, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY stage
ORDER BY total_laid_off DESC;

-- Find which industry has the highest percentage of layoffs
SELECT industry, AVG(percentage_laid_off) AS avg_layoff_percentage
FROM layoffs_staging2
GROUP BY industry
ORDER BY avg_layoff_percentage DESC;

-- Find companies with layoffs above the average
SELECT company, total_laid_off
FROM layoffs_staging2
WHERE total_laid_off > (SELECT AVG(total_laid_off) FROM layoffs_staging2)
ORDER BY total_laid_off DESC;

-- Find the most funded company that had layoffs
SELECT company, funds_raised_millions, total_laid_off
FROM layoffs_staging2
ORDER BY funds_raised_millions DESC
LIMIT 1;

-- Compare layoffs in US vs. Europe
SELECT country, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
WHERE country IN ('United States', 'Germany', 'United Kingdom', 'France', 'Spain')
GROUP BY country
ORDER BY total_laid_off DESC;
