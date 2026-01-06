call retail_db.abt_buy.update_similarity_threshold(0.8);
show procedures;
use database retail_db;
use schema abt_buy;

select * from matching_metrics;
call update_product_matches();