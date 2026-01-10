SELECT name,
    main_category,
    sub_category,
    ratings,
    no_of_ratings,
    actual_price,
    discount_price,
    CASE
        WHEN ratings<2 THEN 'Poor'
        WHEN ratings between 3 AND 3.9 THEN 'Average'
        WHEN ratings between 4 AND 4.9 THEN 'Good'
        else 'Excellent'
        END AS rating_category
    ,
round((actual_price - discount_price)*(100/actual_price),2) as discounted_percentage
FROM (SELECT
    name,
    main_category,
    sub_category,
    --applying filters
    coalesce(ratings,0) as ratings,
    coalesce(no_of_ratings,0) as no_of_ratings,
    -- Clean and convert price columns
    CAST(REGEXP_REPLACE(actual_price, '[^0-9.]', '') AS DOUBLE) as actual_price,
    CAST(REGEXP_REPLACE( coalesce(discount_price,actual_price), '[^0-9.]', '') AS DOUBLE) as discount_price

    
FROM myDataSource
WHERE name IS NOT NULL
AND main_category IS NOT NULL
AND CAST(REGEXP_REPLACE(actual_price, '[^0-9.]', '') AS DOUBLE) >0)
