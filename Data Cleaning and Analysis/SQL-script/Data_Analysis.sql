-- Exploratory Data Analysis

SELECT *
FROM layoffs_staging2;


-- counting the total as well as min and max in total and percentage respectivly
SELECT count(total_laid_off), 
MAX(total_laid_off) , MAX(percentage_laid_off), 
MIN(total_laid_off) , MIN(percentage_laid_off)
FROM layoffs_staging2;

-- checking 100% laid off
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1 ;

-- Checking date range
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- Checking Total laid as per company
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Checking total laid as per indusry
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Checking total laid as per country
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Checking total laid as per YEAR
SELECT YEAR(`date`) AS `YEARS`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `YEARS`
ORDER BY 1 DESC;

-- Checking total layoff by year and month
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS TOTAL_LAID_OFFs
FROM layoffs_staging2
GROUP BY `MONTH`
ORDER BY 1 DESC;

-- Rolling Total
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS TOTAL_LAID_OFFs
FROM layoffs_staging2
GROUP BY `MONTH`
ORDER BY 1 DESC
)
SELECT `MONTH`, TOTAL_LAID_OFFs, SUM(TOTAL_LAID_OFFs) OVER (order by `MONTH`) AS Rolling_Total
FROM Rolling_Total;

SELECT YEAR(`date`) AS year, MONTH(`date`) AS month, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging
GROUP BY year, month
ORDER BY year DESC, month DESC;

SELECT company, YEAR(`date`) AS year, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging
GROUP BY company, year
ORDER BY year DESC;

-- Rank Companies by Total Layoffs Within Each Year
WITH Company_Year AS
(
SELECT company, YEAR(`date`) AS year, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging
GROUP BY company, year
), Company_Year_Rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY year ORDER BY total_layoffs DESC) AS Ranking
FROM Company_Year
WHERE year is not null
)
SELECT *
FROM Company_Year_Rank
ORDER BY Ranking;

-- it is not neccessory we can also do it directly............my br not don't know
WITH Company_Year AS
(
SELECT company, YEAR(`date`) AS year, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging
GROUP BY company, year
), Company_Year_Rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY year ORDER BY total_layoffs DESC) AS Ranking
FROM Company_Year
WHERE year is not null
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;

-- Compare Layoffs Trends Year-Over-Year
SELECT YEAR(`date`) AS year,
       SUM(total_laid_off) AS total_layoffs,
       SUM(total_laid_off) - LAG(SUM(total_laid_off)) OVER (ORDER BY YEAR(`date`)) AS change_from_previous_year
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY year;

WITH RECURSIVE Layoff_Trend AS (
    -- Base case: Start with the earliest year
    SELECT 
        YEAR(`date`) AS year,
        SUM(total_laid_off) AS total_layoffs
    FROM layoffs_staging2
    WHERE YEAR(`date`) = (SELECT MIN(YEAR(`date`)) FROM layoffs_staging2)
    GROUP BY YEAR(`date`)
    
    UNION ALL

    -- Recursive case: Add subsequent years
    SELECT 
        lt.year + 1 AS year,
        (SELECT SUM(total_laid_off) 
         FROM layoffs_staging2 
         WHERE YEAR(`date`) = lt.year + 1) AS total_layoffs
    FROM Layoff_Trend lt
    WHERE EXISTS (
        SELECT 1
        FROM layoffs_staging2
        WHERE YEAR(`date`) = lt.year + 1
    )
)
SELECT * 
FROM Layoff_Trend;

-- Which to Use?
# If the dataset is clean, complete, and straightforward, direct aggregation is the better choice due to simplicity and performance.
# If the analysis requires cumulative trends, interpolation for missing years, or hierarchical dependencies, the recursive CTE is more appropriate.
# Both are valid, but the recursive CTE adds versatility at the cost of complexity.

-- simulated future layoffs by forecasting a 5% increase in total layoffs for each month
WITH Monthly_Layoffs AS (
    SELECT YEAR(`date`) AS year,
           MONTH(`date`) AS month,
           SUM(total_laid_off) AS total_laid_off
    FROM layoffs_staging2
    GROUP BY YEAR(`date`), MONTH(`date`)
)
SELECT year, month,
       total_laid_off,
       total_laid_off * 1.05 AS predicted_layoffs
FROM Monthly_Layoffs
ORDER BY year DESC, month DESC;

--  the percentage of layoffs and calculated the impact on total layoffs.
SELECT company, industry,
       total_laid_off,
       total_laid_off * 1.1 AS increased_layoffs
FROM layoffs_staging2;

SELECT * FROM layoffs_staging2;
