
SELECT * 
FROM layoffs;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

# here we are creating a copy table with column
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging;

# Inserting data from exiting table
INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT * FROM layoffs_staging;

-- 1. Remove duplicate

SELECT * ,
Row_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT * ,
Row_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1 ;

SELECT * FROM layoffs_staging
where company = 'Cazoo' ;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

WITH duplicate_cte AS
(
SELECT * ,
Row_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) AS row_num
FROM layoffs_staging2
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1 ;

SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT * ,
Row_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,
 country, funds_raised) AS row_num
FROM layoffs_staging ;

SELECT * FROM layoffs_staging2;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1 ;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1 ;

-- Standardizing Data

SELECT DISTINCT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry, TRIM(industry)
FROM layoffs_staging2
ORDER BY industry; 

SELECT DISTINCT  country, TRIM(country)
FROM layoffs_staging2
ORDER BY country; 

SELECT  count(DISTINCT country)
FROM layoffs_staging2;

SELECT `date`
FROM layoffs_staging2;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

SELECT `date`,
       STR_TO_DATE(`date`, '%Y-%m-%d') AS formatted_date
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%Y-%m-%d');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT * 
FROM layoffs_staging2;

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off = '' or total_laid_off IS NULL ;

UPDATE layoffs_staging2
SET total_laid_off = null
WHERE total_laid_off ='';

SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off = '' or percentage_laid_off IS NULL ;

UPDATE layoffs_staging2
SET percentage_laid_off = null
WHERE percentage_laid_off ='';

SELECT * 
FROM layoffs_staging2
WHERE location = '' or location IS NULL ;

UPDATE layoffs_staging2
SET location = null
WHERE location ='';

SELECT * 
FROM layoffs_staging2
WHERE industry = '' or industry IS NULL ;

UPDATE layoffs_staging2
SET industry = null
WHERE industry ='';

SELECT * 
FROM layoffs_staging2
WHERE stage = '' or stage IS NULL ;

UPDATE layoffs_staging2
SET stage = null
WHERE stage ='';

SELECT * 
FROM layoffs_staging2
WHERE funds_raised = '' or funds_raised IS NULL ;

UPDATE layoffs_staging2
SET funds_raised = null
WHERE funds_raised ='';

#---------------------------------------------------------------------------------------------------------------

-- Here we are checking if industry column has same two or more row where on industry is empty and another 
-- one is filled with industry then we copy that value from the filled on and paste into the blank one....
SELECT * 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
    AND t1.location = t2.location
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL; 
-- here we don't have repeated value .......

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company 
    AND t1.location = t2.location
SET t1.indystry = t2.industry     
WHERE t1.industry IS NULL AND 
	  t2.industry IS NOT NULL; 
 
 #----------------------------------------------------------------------------------------
 -- NOW here we are removing a data that is nothing but unnessery data- WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL
 -- beacuse on basis of this value we are going to tell that how many laid offs done.......
 
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;  

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
  AND percentage_laid_off IS NULL;
  
#-----------------------------------------------------------------------------------------

SELECT * 
FROM layoffs_staging2;  

-- Now we are removing a column not we don't want

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

#--------------------------------------------------------------------------------------------------
  
SELECT * 
FROM layoffs_staging2;


  
