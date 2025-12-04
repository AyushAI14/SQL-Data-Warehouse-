-- This file is for cleaning the data

-- 1. Checking for duplicated and null value
select cst_id,count(*) as c_no
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is NULL

-- 2. Checking for unwanted spaces in the string 
-- cst_firstname has unwanted spaces
-- cst_lastname has unwanted spaces
-- cst_gndr has no unwanted spaces

select cst_lastname from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname)


-- 3. Standarization and distict
-- we will check distict value here and coverting single word into full like (M -> Male)
select distinct cst_gndr from bronze.crm_cust_info 
select distinct cst_marital_status from bronze.crm_cust_info 


-- crm_prd_info -------------------
select * from bronze.crm_prd_info 

-- 1. Checking for duplicated and null value
select prd_id,count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is NULL


select prd_nm from bronze.crm_prd_info
where prd_nm != trim(prd_nm)


-- for other check the cleaning query 
