-- SQL Project - Data Cleaning

-- Dataset source: https://www.kaggle.com/datasets/swaptr/layoffs-2022

-- 1. Load and create a working table
SELECT * 
FROM world_layoffs.layoffs;

-- Create a staging table to clean and transform the data. 
-- This table will serve as a working area, while the original raw data is preserved for reference or recovery if needed.
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- 2. Remove duplicates
-- Identify duplicate records by assigning a row number to each entry within groups of identical values based on specific columns.
SELECT *,
ROW_NUMBER () OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

-- Only rows with a row number greater than 1 are considered duplicates and are selected for further review or handling.
SELECT *
FROM (
	SELECT *,
	ROW_NUMBER () OVER(
	PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
	FROM layoffs_staging
	) AS row_num
WHERE row_num > 1;

-- Verify data for 'Oda' to ensure correctness and identify any potential anomalies or duplicates.
SELECT *
FROM layoffs_staging
WHERE company = 'Oda';

-- These entries appear to be valid and should not be deleted. A thorough row-by-row review is necessary to ensure accuracy.

-- Refine duplicate identification by incorporating additional columns (`stage`, `country`, and `funds_raised_millions`) in the partitioning criteria.
-- This ensures a more precise detection of true duplicates by considering additional differentiating factors.

-- These are the actual duplicate entries that need to be reviewed and addressed based on the refined partitioning criteria.
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER () OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Verify data for 'Casper' to ensure correctness and identify any potential anomalies or duplicates.
SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

-- 3. Create a new staging table for cleaned data with a row number column for duplicate handling.
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Disable SQL_SAFE_UPDATES to allow DELETE operations
SET SQL_SAFE_UPDATES = 0;

-- Verify that the new table has been created successfully
SELECT *
FROM layoffs_staging2;

-- Populate layoffs_staging2 with data from layoffs_staging while assigning row numbers for duplicate handling.
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER () OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Verify that data has been inserted correctly into layoffs_staging2
SELECT *
FROM layoffs_staging2;

-- Identify duplicates in the new staging table before deletion
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Remove duplicate records where row_num > 1, keeping only the first occurrence.
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Confirm that duplicates have been removed successfully
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- 4. Standardization
-- The next step is to standardize the dataset by ensuring consistency in text formatting, date formats, and numerical values.

-- 4.1 Standardizing company names
-- Check for variations in company names by retrieving distinct values.
SELECT DISTINCT company
FROM layoffs_staging2;

-- Identify discrepancies by trimming leading and trailing spaces from company names.
SELECT DISTINCT company, TRIM(company)
FROM layoffs_staging2;

-- Update the company column to remove any leading or trailing spaces.
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Verify that company names are now standardized.
SELECT DISTINCT company, TRIM(company)
FROM layoffs_staging2;

-- 4.2 Standardizing industry names
-- Retrieve distinct values to identify inconsistencies in industry names.
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- Identify incorrect or inconsistent industry names related to 'Crypto'.
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Unify industry names by updating all variations of 'Crypto' to a single standard value.
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Verify that all variations of 'Crypto' have been standardized.
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- 4.3 Standardizing location names
-- Retrieve distinct location values to identify inconsistencies or misspellings.
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

-- Correct known location name inconsistencies.
-- Some cities were found to have incorrect spellings or variations that need standardization.

-- Correct 'Düsseldorf' spelling variations.
UPDATE layoffs_staging2
SET location = 'Düsseldorf'
WHERE location LIKE 'D%sseldorf';

-- Correct 'Florianopolis' spelling variations.
UPDATE layoffs_staging2
SET location = 'Florianopolis'
WHERE location LIKE 'Florian%';

-- Correct 'Malmo' spelling variations.
UPDATE layoffs_staging2
SET location = 'Malmo'
WHERE location LIKE 'Malm%';

-- Verify that location names are now standardized.
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

-- 4.4 Standardizing country names
-- Retrieve distinct country values to identify inconsistencies or misspellings.
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- The previous query revealed two variations for "United States": "United States" and "United States."
-- To ensure consistency, we need to trim any trailing periods.
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

-- Update country names to remove trailing periods.
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Verify that the country names have been standardized correctly.
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- 4.5 Standardizing date format
-- Retrieve all values from the 'date' column to check its current format.
-- The format appears to be in MM/DD/YYYY, which needs to be converted to a standard DATE format.
SELECT `date`
FROM layoffs_staging2;

-- The previous query helps confirm the format of the 'date' column.
-- Additionally, the column format was manually verified in the schema panel.
-- This step ensures that we correctly understand how dates are currently stored.

-- Convert string-based dates to the standard YYYY-MM-DD format using STR_TO_DATE.
-- This transformation follows the pattern '%m/%d/%Y' (MM/DD/YYYY).
SELECT `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Apply the date format transformation to the 'date' column.
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Modify the column type to DATE to ensure consistency in future queries.
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 5. Handling Blanks and NULLs
-- This section focuses on identifying and addressing missing values in key columns.

-- 5.1 Identifying missing values in the 'industry' column
-- Retrieve distinct industry values to understand existing categories.
SELECT DISTINCT industry
FROM layoffs_staging2;

-- Identify records where 'industry' is either NULL or blank.
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- 5.2 Handling missing industry values
-- Examine the 'Airbnb' company records to check for possible industry categorization.
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Airbnb has an industry value of 'Travel' in one record, 
-- which can be used to fill in missing values for the same company.

-- Identify cases where a company has both missing and available industry values.
-- This allows us to determine if we can infer the missing values from existing data.
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- The previous query revealed three missing industry values 
-- that can be filled using existing data (Travel, Transportation, Consumer).

-- Attempt to update missing industry values by matching them with known values from the same company and location.
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry=t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- If the previous update does not work correctly, ensure blank values are converted to NULLs before retrying.
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry ='';

-- Verify that blank values have been successfully converted to NULLs.
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Since NULL conversion was successful, attempt the update again.
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry=t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Verify that there are no more missing industry values where they could have been inferred.
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

-- Double-check the correction by reviewing the Airbnb records again.
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Perform a final check to identify any remaining missing values in the 'industry' column.
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- The previous query revealed that 'Bally's Interactive' has only one record, 
-- meaning its missing industry value cannot be inferred from other records.
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

-- 5.3 Handling missing values in 'total_laid_off' and 'percentage_laid_off'
-- Identify records where both 'total_laid_off' and 'percentage_laid_off' are NULL.
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Check number of duplicate records before deletion
SELECT COUNT(*) 
FROM layoffs_staging2
WHERE row_num > 1;

-- Records where both 'total_laid_off' and 'percentage_laid_off' are NULL provide no insight into layoffs.
-- Since they do not contribute to analysis, they will be removed.
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Verify that duplicates have been removed successfully
SELECT COUNT(*) 
FROM layoffs_staging2
WHERE row_num > 1;

-- Verify that records with NULL values in these columns have been removed.
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 5.4 Removing unnecessary columns
-- Review the table structure to determine if any columns are no longer needed.
SELECT *
FROM layoffs_staging2;

-- The 'row_num' column was only used for duplicate handling and is no longer needed.
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Verify that 'row_num' has been successfully removed.
SELECT *
FROM layoffs_staging2;

-- Final verification: Check total number of records after data cleaning
SELECT COUNT(*) FROM layoffs_staging2;