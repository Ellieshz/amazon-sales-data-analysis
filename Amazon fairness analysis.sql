-- Preview the datasets
SELECT COUNT(*) 
FROM `plucky-vault-430618-j4.Amazon_datasets.sales_data`;
SELECT COUNT(*)
FROM `plucky-vault-430618-j4.Amazon_datasets.Customer_Behavior_data`;

SELECT *
FROM `plucky-vault-430618-j4.Amazon_datasets.sales_data`
LIMIT 10;
SELECT *
FROM `plucky-vault-430618-j4.Amazon_datasets.Customer_Behavior_data`
LIMIT 10;


-- Get basic stats for the datasets
SELECT 
  COUNT(*) AS total_records, 
  COUNT(DISTINCT product_id) AS unique_products, 
  COUNT(DISTINCT user_id) AS unique_users
FROM `plucky-vault-430618-j4.Amazon_datasets.sales_data`;

SELECT 
  COUNT(*) AS total_records,
  COUNT(DISTINCT age) AS unique_ages,
  COUNT(DISTINCT gender) AS unique_genders,
  COUNT(DISTINCT Purchase_Frequency) AS unique_purchase_frequencies
FROM `plucky-vault-430618-j4.Amazon_datasets.Customer_Behavior_data`;



-- Check for missing values in the datasets
SELECT 
  COUNT(CASE WHEN product_id IS NULL THEN 1 END) AS missing_product_id,
  COUNT(CASE WHEN user_id IS NULL THEN 1 END) AS missing_user_id,
  COUNT(CASE WHEN rating IS NULL THEN 1 END) AS missing_rating,
  COUNT(CASE WHEN discounted_price IS NULL THEN 1 END) AS missing_discounted_price
FROM `plucky-vault-430618-j4.Amazon_datasets.sales_data`; -- No missing values 


SELECT 
  COUNT(CASE WHEN age IS NULL THEN 1 END) AS missing_age,
  COUNT(CASE WHEN gender IS NULL THEN 1 END) AS missing_gender,
  COUNT(CASE WHEN Purchase_Frequency IS NULL THEN 1 END) AS missing_purchase_frequency
FROM `plucky-vault-430618-j4.Amazon_datasets.Customer_Behavior_data`; -- No missing values


-- Merge the datasets
CREATE TABLE `plucky-vault-430618-j4.Amazon_datasets.amazon_merged` AS
SELECT 
    a.product_id, 
    a.product_name, 
    a.category AS product_category, 
    a.discounted_price, 
    a.rating, 
    b.age, 
    b.gender, 
    b.Purchase_Frequency, 
    b.Browsing_Frequency, 
    b.Cart_Completion_Frequency
FROM `plucky-vault-430618-j4.Amazon_datasets.sales_data` a
JOIN `plucky-vault-430618-j4.Amazon_datasets.Customer_Behavior_data` b
ON a.category = b.Purchase_Categories;


-- Validat the merged dataset
SELECT * 
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_merged`
LIMIT 10;

SELECT COUNT(*)
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_merged`;


-- Splitting categories in sales_data and Customer_Behavior
CREATE OR REPLACE TABLE `plucky-vault-430618-j4.Amazon_datasets.amazon_sales_cleaned_split` AS
SELECT 
  product_id, 
  product_name, 
  SPLIT(category, '|')[OFFSET(0)] AS cleaned_category,  -- extract first category
  discounted_price, 
  rating
FROM `plucky-vault-430618-j4.Amazon_datasets.sales_data`;


CREATE OR REPLACE TABLE `plucky-vault-430618-j4.Amazon_datasets.amazon_consumer_behavior_cleaned_split` AS
SELECT 
  age, 
  gender, 
  SPLIT(Purchase_Categories, ';')[OFFSET(0)] AS cleaned_purchase_category,  -- extract first category
  Purchase_Frequency, 
  Browsing_Frequency, 
  Cart_Completion_Frequency
FROM `plucky-vault-430618-j4.Amazon_datasets.Customer_Behavior_data`;


-- Preview the cleaned tables
SELECT *
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_sales_cleaned_split` 
LIMIT 10;

SELECT *
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_consumer_behavior_cleaned_split`
LIMIT 10;


--Merge the cleaned data
CREATE OR REPLACE TABLE `plucky-vault-430618-j4.Amazon_datasets.amazon_merged_cleaned` AS
SELECT 
    a.product_id, 
    a.product_name, 
    a.cleaned_category AS product_category, 
    a.discounted_price, 
    a.rating, 
    b.age, 
    b.gender, 
    b.Purchase_Frequency, 
    b.Browsing_Frequency, 
    b.Cart_Completion_Frequency
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_sales_cleaned_split` a
JOIN `plucky-vault-430618-j4.Amazon_datasets.amazon_consumer_behavior_cleaned_split` b
ON a.cleaned_category = b.cleaned_purchase_category;

-- Validate the merged data
SELECT * 
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_merged_cleaned`
LIMIT 10;
SELECT COUNT(*) 
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_merged_cleaned`; -- No data to display

-- fuzzy matching
SELECT * 
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_sales_cleaned_split` a
JOIN `plucky-vault-430618-j4.Amazon_datasets.amazon_consumer_behavior_cleaned_split` b
ON a.cleaned_category LIKE CONCAT('%', b.cleaned_purchase_category, '%')
LIMIT 10;                                                            -- Still no data to display


-- Lowercase and trim whitespace 

CREATE OR REPLACE TABLE `plucky-vault-430618-j4.Amazon_datasets.amazon_sales_cleaned_lower` AS
SELECT 
  product_id, 
  product_name, 
  LOWER(TRIM(SPLIT(category, '|')[OFFSET(0)])) AS cleaned_category, -- lowercasing and trimming
  discounted_price, 
  rating
FROM `plucky-vault-430618-j4.Amazon_datasets.sales_data`;


CREATE OR REPLACE TABLE `plucky-vault-430618-j4.Amazon_datasets.amazon_consumer_behavior_cleaned_lower` AS
SELECT 
  age, 
  gender, 
  LOWER(TRIM(SPLIT(Purchase_Categories, ';')[OFFSET(0)])) AS cleaned_purchase_category, -- lowercasing and trimming
  Purchase_Frequency, 
  Browsing_Frequency, 
  Cart_Completion_Frequency
FROM `plucky-vault-430618-j4.Amazon_datasets.Customer_Behavior_data`;

-- Partile matches 
CREATE OR REPLACE TABLE `plucky-vault-430618-j4.Amazon_datasets.amazon_merged_cleaned_partial` AS
SELECT 
    a.product_id, 
    a.product_name, 
    a.cleaned_category AS product_category, 
    a.discounted_price, 
    a.rating, 
    b.age, 
    b.gender, 
    b.Purchase_Frequency, 
    b.Browsing_Frequency, 
    b.Cart_Completion_Frequency
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_sales_cleaned_lower` a
JOIN `plucky-vault-430618-j4.Amazon_datasets.amazon_consumer_behavior_cleaned_lower` b
ON a.cleaned_category LIKE CONCAT('%', b.cleaned_purchase_category, '%') 
OR b.cleaned_purchase_category LIKE CONCAT('%', a.cleaned_category, '%');

-- Manul Inspection of Non-matching categories
SELECT DISTINCT a.cleaned_category
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_sales_cleaned_lower` a
LEFT JOIN `plucky-vault-430618-j4.Amazon_datasets.amazon_consumer_behavior_cleaned_lower` b
  ON a.cleaned_category LIKE CONCAT('%', b.cleaned_purchase_category, '%')
  OR b.cleaned_purchase_category LIKE CONCAT('%', a.cleaned_category, '%')
WHERE b.cleaned_purchase_category IS NULL;

SELECT DISTINCT b.cleaned_purchase_category
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_consumer_behavior_cleaned_lower` b
LEFT JOIN `plucky-vault-430618-j4.Amazon_datasets.amazon_sales_cleaned_lower` a
  ON a.cleaned_category LIKE CONCAT('%', b.cleaned_purchase_category, '%')
  OR b.cleaned_purchase_category LIKE CONCAT('%', a.cleaned_category, '%')
WHERE a.cleaned_category IS NULL;

-- Create a mapping table 
CREATE OR REPLACE TABLE `plucky-vault-430618-j4.Amazon_datasets.category_mapping` AS
SELECT "beauty and personal care" AS consumer_behavior_category, "health&personalcare" AS sales_category
UNION ALL
SELECT "groceries and gourmet food", "groceries"
UNION ALL
SELECT "clothing and fashion", "clothing"
UNION ALL
SELECT "home and kitchen", "home&kitchen"
UNION ALL
SELECT "others", "others"
UNION ALL
SELECT "electronics", "electronics"
UNION ALL
SELECT "computers", "computers&accessories"
UNION ALL
SELECT "home improvement", "homeimprovement"
UNION ALL
SELECT "musical instruments", "musicalinstruments"
UNION ALL
SELECT "car and motorbike", "car&motorbike"
UNION ALL
SELECT "toys and games", "toys&games"

-- Use the mapping table to merge the data 
CREATE OR REPLACE TABLE `plucky-vault-430618-j4.Amazon_datasets.amazon_merged_mapped` AS
SELECT 
    a.product_id, 
    a.product_name, 
    a.cleaned_category AS product_category, 
    a.discounted_price, 
    a.rating, 
    b.age, 
    b.gender, 
    b.Purchase_Frequency, 
    b.Browsing_Frequency, 
    b.Cart_Completion_Frequency
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_sales_cleaned_lower` a
JOIN `plucky-vault-430618-j4.Amazon_datasets.category_mapping` m
  ON a.cleaned_category = m.sales_category
JOIN `plucky-vault-430618-j4.Amazon_datasets.amazon_consumer_behavior_cleaned_lower` b
  ON m.consumer_behavior_category = b.cleaned_purchase_category;


-- validate the merged data 
SELECT * 
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_merged_mapped`
LIMIT 10;
SELECT COUNT(*) 
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_merged_mapped`; -- worked, received 15031 rows



--  Inspect values in Purchase_Frequency and Cart_Completion_Frequency
SELECT DISTINCT Purchase_Frequency
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_merged_mapped`;

SELECT DISTINCT Cart_Completion_Frequency
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_merged_mapped`;


-- Bias and Fairness Analysis
-- Analyze phurchase and cart behavior bias
SELECT 
    gender, 
    AVG(CASE 
        WHEN Purchase_Frequency = 'Once a week' THEN 1
        WHEN Purchase_Frequency = 'Once a month' THEN 0.25
        WHEN Purchase_Frequency = 'Few times a month' THEN 2
        WHEN Purchase_Frequency = 'Multiple times a week' THEN 3
        WHEN Purchase_Frequency = 'Less than once a month' THEN 0.1
        ELSE 0  -- Default to 0 if unrecognized
    END) AS avg_purchase_frequency
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_merged_mapped`
GROUP BY gender;      --Purchase Frequency by Gender


SELECT 
    age, 
    AVG(CASE 
        WHEN Purchase_Frequency = 'Once a week' THEN 1
        WHEN Purchase_Frequency = 'Once a month' THEN 0.25
        WHEN Purchase_Frequency = 'Few times a month' THEN 2
        WHEN Purchase_Frequency = 'Multiple times a week' THEN 3
        WHEN Purchase_Frequency = 'Less than once a month' THEN 0.1
        ELSE 0
    END)AS avg_purchase_frequency
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_merged_mapped`
GROUP BY age;          --Purchase Frequency by age 


SELECT 
    gender, 
    AVG(CASE 
        WHEN Cart_Completion_Frequency = 'Always' THEN 1
        WHEN Cart_Completion_Frequency = 'Often' THEN 0.75
        WHEN Cart_Completion_Frequency = 'Sometimes' THEN 0.5
        WHEN Cart_Completion_Frequency = 'Rarely' THEN 0.25
        WHEN Cart_Completion_Frequency = 'Never' THEN 0
        ELSE 0  -- Default to 0 if unrecognized
    END) AS avg_cart_completion
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_merged_mapped`
GROUP BY gender;        --Cart Completion by gender

SELECT 
    age, 
    AVG(CASE 
        WHEN Cart_Completion_Frequency = 'Always' THEN 1
        WHEN Cart_Completion_Frequency = 'Often' THEN 0.75
        WHEN Cart_Completion_Frequency = 'Sometimes' THEN 0.5
        WHEN Cart_Completion_Frequency = 'Rarely' THEN 0.25
        WHEN Cart_Completion_Frequency = 'Never' THEN 0
        ELSE 0  -- Default to 0 if unrecognized
    END) AS avg_cart_completion
FROM `plucky-vault-430618-j4.Amazon_datasets.amazon_merged_mapped`
GROUP BY age;            --Cart Completion by age


