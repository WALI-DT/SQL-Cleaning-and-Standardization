-- REMOVE DUPLICATES --
-- CREATE AND INSERT DATA INTO TABLE --
-- STANDARDISE THE DATA --
-- REMOVE NULL OR BLANK VALUES -- 
-- REMOVE IRRELEVANT COLUMNS AND DATA --

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;


insert layoffs_staging
select *
from layoffs;



-- REMOVING DUPLICATES --

with duplicates_cte as
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off,
 `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging 
)
select *
from duplicates_cte
where row_num > 1;

select *
from layoffs_staging;



-- CREATING AND INSERTING DATA INTO TABLE --

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


select *
from layoffs_staging2;


insert layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off,
 `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;


delete
from layoffs_staging2
where row_num > 1;



-- STANDARDIZING THE DATA --

update layoffs_staging2
set company = trim(company);


update layoffs_staging2
set industry = "Crypto"
where industry like "crypto%";


update layoffs_staging2
set country = "United States"
where country like "United States%";


select distinct country, trim(trailing "." from country)
from layoffs_staging2
order by 1;


update layoffs_staging2
set `date` = str_to_date (`date`, '%m/%d/%Y');


alter table layoffs_staging2
modify column `date` date;



-- REMOVING NULL AND BLANK VALUES --

select *
from layoffs_staging2
where industry is null or 
industry = "";


select *
from layoffs_staging2
where company  = 'airbnb';


update layoffs_staging2
set industry = null
where industry = "";


select table1.industry, table2.industry
from layoffs_staging2 as table1
join layoffs_staging2 as table2
	on table1.company = table2.company
where (table1.industry is null or table1.industry = "")
and	table2.industry is not null;


update layoffs_staging2 as table1
join layoffs_staging2 as table2
	on table1.company = table2.company
set table1.industry = table2.industry
where table1.industry is null
and	table2.industry is not null;


select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;



-- REMOVING IRRELEVANT COLUMNS AND DATA --

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


alter table layoffs_staging2
drop column row_num;


select *
from layoffs_staging2;