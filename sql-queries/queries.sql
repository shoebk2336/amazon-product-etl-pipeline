
-- Amazon Product Catalog - Data Validation Queries
-- Use these queries in Amazon Athena to validate your ETL pipeline outputs

-- ============================================
-- 1. DATA QUALITY CHECKS
-- ============================================

-- Check for null values in critical columns
SELECT
    COUNT(*) as total_records,
    COUNT(name) as non_null_names,
    COUNT(main_category) as non_null_categories,
    COUNT(actual_price) as non_null_prices,
    COUNT(*) - COUNT(name) as null_names,
    COUNT(*) - COUNT(main_category) as null_categories,
    COUNT(*) - COUNT(actual_price) as null_prices
FROM cleaned_products;

-- Identify pricing errors (discount > actual)
SELECT
    name,
    main_category,
    actual_price,
    discount_price,
    discount_percentage
FROM cleaned_products
WHERE discount_price > actual_price
ORDER BY discount_percentage DESC;

-- Check for invalid ratings (outside 0-5 range)
SELECT
    name,
    ratings,
    rating_category
FROM cleaned_products
WHERE ratings < 0 OR ratings > 5;

-- ============================================
-- 2. DATA DISTRIBUTION ANALYSIS
-- ============================================

-- Rating distribution
SELECT
    rating_category,
    COUNT(*) as product_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM cleaned_products
GROUP BY rating_category
ORDER BY product_count DESC;

-- Price range distribution
SELECT
    CASE
        WHEN actual_price < 500 THEN 'Budget (<500)'
        WHEN actual_price BETWEEN 500 AND 2000 THEN 'Mid-range (500-2000)'
        WHEN actual_price BETWEEN 2001 AND 5000 THEN 'Premium (2001-5000)'
        ELSE 'Luxury (>5000)'
    END as price_range,
    COUNT(*) as product_count,
    ROUND(AVG(ratings), 2) as avg_rating
FROM cleaned_products
GROUP BY 1
ORDER BY 2 DESC;

-- ============================================
-- 3. BUSINESS INSIGHTS VALIDATION
-- ============================================

-- Top 10 products by potential revenue
SELECT
    name,
    main_category,
    discount_price,
    no_of_ratings,
    discount_price * no_of_ratings as potential_revenue
FROM cleaned_products
ORDER BY potential_revenue DESC
LIMIT 10;

-- Category performance comparison
SELECT
    main_category,
    COUNT(*) as product_count,
    ROUND(AVG(ratings), 2) as avg_rating,
    ROUND(AVG(discount_percentage), 2) as avg_discount,
    ROUND(AVG(actual_price), 2) as avg_price,
    SUM(no_of_ratings) as total_reviews
FROM cleaned_products
GROUP BY main_category
ORDER BY product_count DESC;

-- Products with highest discounts
SELECT
    name,
    main_category,
    actual_price,
    discount_price,
    discount_percentage
FROM cleaned_products
WHERE discount_percentage > 0
ORDER BY discount_percentage DESC
LIMIT 20;

-- ============================================
-- 4. AGGREGATION VALIDATION
-- ============================================

-- Verify category summary matches source data
SELECT
    'Source Data' as source,
    main_category,
    COUNT(*) as product_count,
    ROUND(AVG(ratings), 2) as avg_rating
FROM cleaned_products
GROUP BY main_category
ORDER BY main_category;

-- Compare with category_summary table
SELECT
    'Aggregated Data' as source,
    main_category,
    product_count,
    avg_rating
FROM category_summary
ORDER BY main_category;

-- ============================================
-- 5. DATA COMPLETENESS CHECKS
-- ============================================

-- Check record counts at each stage
SELECT 'Raw Data' as stage, COUNT(*) as record_count FROM input_data
UNION ALL
SELECT 'Cleaned Data' as stage, COUNT(*) as record_count FROM cleaned_products
UNION ALL
SELECT 'Data Quality Issues' as stage, COUNT(*) as record_count FROM outliers;

-- Verify no data loss in transformations
SELECT
    (SELECT COUNT(*) FROM input_data) as input_records,
    (SELECT COUNT(*) FROM cleaned_products) as output_records,
    (SELECT COUNT(*) FROM outliers) as filtered_records,
    (SELECT COUNT(*) FROM input_data) - 
    (SELECT COUNT(*) FROM cleaned_products) - 
    (SELECT COUNT(*) FROM outliers) as unaccounted_records;

-- ============================================
-- 6. PERFORMANCE METRICS
-- ============================================

-- Average processing metrics by category
SELECT
    main_category,
    COUNT(*) as products,
    ROUND(AVG(actual_price), 2) as avg_price,
    ROUND(AVG(discount_percentage), 2) as avg_discount,
    ROUND(AVG(ratings), 2) as avg_rating,
    ROUND(AVG(no_of_ratings), 0) as avg_reviews
FROM cleaned_products
GROUP BY main_category
ORDER BY products DESC;

-- Identify outliers (statistical)
SELECT
    name,
    main_category,
    actual_price,
    ratings,
    no_of_ratings
FROM cleaned_products
WHERE actual_price > (SELECT AVG(actual_price) + 2 * STDDEV(actual_price) FROM cleaned_products)
   OR no_of_ratings > (SELECT AVG(no_of_ratings) + 2 * STDDEV(no_of_ratings) FROM cleaned_products)
ORDER BY actual_price DESC;


