select 
main_category,
count(*) as product_count,
Round(avg(ratings),2) as average_ratings,
round(avg(discounted_percentage),2) as average_discount,
sum(no_of_ratings) as total_reviews


from myDataSource
Group By main_category
