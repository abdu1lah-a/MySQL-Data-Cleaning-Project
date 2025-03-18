-- Data cleaning
select * from layoffs;

-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null values or blank value
-- 4. Remove any Columns 

-- Create a staging database from the raw data (it is not best pracitise to work on raw data)

create table layoffs_staging like layoffs;

insert into layoffs_staging select * from layoffs;

select * from layoffs_staging;



-- 1. Removing duplicates

-- identifying duplicates (Since there are no primary key rows, we have to use window function)
Select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

-- Create a CTE for the above query to check for duplicates (row_num > 2)
with duplicate_cte as 
(
Select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select * from duplicate_cte
where row_num > 1;

-- Create another staging table because MySQL does not allow for deletion from CTEs
CREATE TABLE `layoffs_staging_2` (
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

select * from layoffs_staging_2;

-- insert into staging 2 values
insert into layoffs_staging_2 
Select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

select * from layoffs_staging_2;

-- delete the duplicate rows from 
delete from layoffs_staging_2
where row_num > 1;

select * from layoffs_staging_2; 





-- 2. Standadizing data

-- Removing redundant white spaces from company column
select distinct (company), trim(company)
from layoffs_staging_2;

update layoffs_staging_2
set company = trim(company);

-- Changing rows with the same industry but different names 
select *
from layoffs_staging_2
where industry like "crypto%";

update layoffs_staging_2
set industry = 'Crypto'
where industry like 'crypto%';

select distinct (industry) from layoffs_staging_2 order by industry;

-- Fixing the location column
select distinct country, trim(trailing '.' from country)
from layoffs_staging_2
order by 1;

update layoffs_staging_2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- Fixing the date column
select `date`, str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging_2;

update layoffs_staging_2 
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging_2 modify column `date` date;





-- 3. Null and blank values
select distinct industry from layoffs_staging_2 order by 1;

-- Populating industry where it is possible to do so
select t1.company, t1.industry, t2.company, t2.industry from
layoffs_staging_2 t1 
join 
layoffs_staging_2 t2
on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging_2 
set industry = null
where industry = '';

update layoffs_staging_2 t1 
join 
layoffs_staging_2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

-- Deleting rows that provide not much information about layoffs
select * from layoffs_staging_2 where 
total_laid_off is null 
and 
percentage_laid_off is null;

delete from layoffs_staging_2
where total_laid_off is null 
and 
percentage_laid_off is null;



-- 4. Remove any columns
-- Remove row_num column because we don't need it anymore
select * from layoffs_staging_2;

alter table layoffs_staging_2
drop column row_num;