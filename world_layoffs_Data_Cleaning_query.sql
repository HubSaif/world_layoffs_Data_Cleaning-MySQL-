-- Data Cleaning --
-- Data world_layoffs (it is shows which industry is doing how much layoff from 2022-2023 )
-- Database name world_layoffs  
-- 1. Remove Duplicates 
-- 2. Standardize the Data
-- 3. Null or Blank values 
-- 4. Remove any column or rows
 
 -- -- Query -- ---
 Select * from layoffs;
 
 -- creating data table, that have copy of row data (duplicate of layoffs)
 CREATE TABLE layoffs_staging
 LIKE layoffs;
 
 Select * from layoffs_staging;
 
 -- Inserting data in layoffs_staging
  INSERT INTO layoffs_staging
  SELECT * from layoffs;
  
  Select * from layoffs_staging;
 
 -- Removing Duplicates (data don't have any unique column)
 -- Adding row number 
Select *,
row_number() over
	(Partition by company, industry, total_laid_off, `date`) as row_num
from layoffs_staging;

-- creating CTE

With duplicate_cte as 
(
	Select *,
row_number() over
	(Partition by company, location, 
    industry, total_laid_off, percentage_laid_off, `date`, stage, country, 
    funds_raised_millions) as row_num
from layoffs_staging
)
 Select * from duplicate_cte
 Where row_num > 1;
 
 Select * from layoffs_staging 
	where company = 'Casper';
 
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

 Select * from layoffs_staging2;
 
 INSERT into layoffs_staging2
 SELECT *,
 ROW_NUMBER() over(
	Partition by company, location, 
    industry, total_laid_off, percentage_laid_off, `date`, stage, country, 
    funds_raised_millions) as row_num
from layoffs_staging;
 
 
 -- Deleting duplicate values / rows 
 
Delete
  from layoffs_staging2
  where row_num > 1;
 
Select *
  from layoffs_staging2
  where row_num > 1;
  
-- Standardizing data

Select company,(trim(company))
  from layoffs_staging2;
 
 UPDATE layoffs_staging2
	SET company = trim(company);

Select company
  from layoffs_staging2;

Select Distinct(industry)
  from layoffs_staging2
  order by 1;
 
 Select *
  from layoffs_staging2
  Where industry like 'crypto%';
 
Update layoffs_staging2
	SET industry ='Crypto'
    where industry like 'crypto%';
 
Select * from layoffs_staging2
where industry like 'crypto';
 
Select Distinct(industry)
  from layoffs_staging2;

Select Distinct(country)
  from layoffs_staging2
  order by 1;
  
Select *
  from layoffs_staging2
  where country like 'united States%'
  order by 1;

Select Distinct(country), trim(TRAILING'.' from country)
  from layoffs_staging2
  order by 1;
 
UPDATE layoffs_staging2
	set country = trim(TRAILING'.' from country)
    where country like 'United States';

Select `date`,
	str_to_date(`date`,'%m/%d/%Y')
    from layoffs_staging2;
 
UPDATE layoffs_staging2
	set `date` = str_to_date(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
modify column `date` DATE;

SELECT * from layoffs_staging2;

-- Work on Null & missing values 
 
SELECT * 
from layoffs_staging2
Where total_laid_off is null
and percentage_laid_off is null;
 
Select Distinct(industry)
from layoffs_staging2;

-- replace blanck space to null 

update layoffs_staging2
SET industry = null
where industry = '';

Select *
from layoffs_staging2
where industry is null
or industry =''; 

Select *
from layoffs_staging2
where company = 'Airbnb';

Select t1.industry,t2.industry
from layoffs_staging2 t1
 join layoffs_staging2 t2
 on t1.company = t2.company
Where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

UPDATE layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
SET t1.industry = t2.industry
Where (t1.industry is null )
and t2.industry is not null;

Select *
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

 Delete
	from layoffs_staging2
	where total_laid_off is null 
	and percentage_laid_off is null;

Select *
from layoffs_staging2;

Alter table layoffs_staging2
DROP column row_num;

Select *
from layoffs_staging2;


-- List the columns you want to check for duplicates

SELECT company, location, 
    industry, total_laid_off, percentage_laid_off, `date`, stage, country, 
    funds_raised_millions
FROM layoffs_staging2
GROUP BY  company, location, 
    industry, total_laid_off, percentage_laid_off, `date`, stage, country, 
    funds_raised_millions  
HAVING COUNT(*) > 1;


CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` date DEFAULT NULL,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


Select * from layoffs_staging3;

insert into layoffs_staging3
	SELECT company, location, 
    industry, total_laid_off, percentage_laid_off, `date`, stage, country, 
    funds_raised_millions
FROM layoffs_staging2
GROUP BY  company, location, 
    industry, total_laid_off, percentage_laid_off, `date`, stage, country, 
    funds_raised_millions  
HAVING COUNT(*) > 1;

Select * from layoffs_staging3;

DELETE FROM layoffs_staging2;


Insert into layoffs_staging2
	Select * from layoffs_staging3;
    
Select * from layoffs_staging2;

 -- Checking for duplicates

SELECT company, location, 
    industry, total_laid_off, percentage_laid_off, `date`, stage, country, 
    funds_raised_millions
FROM layoffs_staging2
GROUP BY  company, location, 
    industry, total_laid_off, percentage_laid_off, `date`, stage, country, 
    funds_raised_millions  
HAVING COUNT(*) > 1;

Drop table layoffs_staging3;

-- EDA Explotary Data analysis --

Select Max(total_laid_off),
	max(percentage_laid_off)
    from layoffs_staging2;
    
Select *
    from layoffs_staging2
    where percentage_laid_off = 1
    order by percentage_laid_off DESC;

Select company, sum(total_laid_off)
    from layoffs_staging2
    group by company
    order by 2 DESC;
    
Select min(`date`),max(`date`)
	from layoffs_staging2;

Select company, sum(total_laid_off)
    from layoffs_staging2
    group by company
    order by 2 DESC;

Select industry, sum(total_laid_off)
    from layoffs_staging2
    group by industry
    order by 2 DESC;

Select country, sum(total_laid_off)
    from layoffs_staging2
    group by country
    order by 2 DESC;
    
Select year(`date`),SUM(total_laid_off)
    from layoffs_staging2
    group by year(`date`)
    order by 2 DESC;
    
Select stage,SUM(total_laid_off)
    from layoffs_staging2
    group by stage
    order by 2 DESC;
    
Select company,sum(percentage_laid_off)
    from layoffs_staging2
    group by company
    order by 2 DESC;
    
 Select Substring(`date`,1,7) as 'Months', sum(total_laid_off)
	from layoffs_staging2
    Where substring(`date`,1,7) is not null
    group by substring(`date`,1,7)
    Order by 1;
    
With Rolling_total AS
	(
	Select Substring(`date`,1,7) as Months, sum(total_laid_off) as total_off
		from layoffs_staging2
		Where SUBSTRING(`date`, 1, 7) is not null
		group by SUBSTRING(`date`, 1, 7)
		Order by 1
    )
    Select Months, total_off,
    sum(total_off)
    over(order by Months) as rolling_total
    from Rolling_total;
    
Select company, year(`date`), sum(total_laid_off)
	from layoffs_staging2
    group by company,year(`date`)
    order by 3 DESC;
    
With Company_Year  (company ,years,total_laid_off) as 
(
	Select company, year(`date`), sum(total_laid_off)
	from layoffs_staging2
    group by company,year(`date`)
),
Company_Year_Rank as
(
	Select *, 
	DENSE_RANK() OVER (partition by years order by total_laid_off DESC) As Ranking 
	from Company_year
	where years is not null
)
select *
from company_Year_Rank 
where Ranking <=5; 


    
    
    