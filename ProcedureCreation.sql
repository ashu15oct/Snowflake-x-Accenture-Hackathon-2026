use database retail_db;
use schema abt_buy;

create or replace procedure update_similarity_threshold(new_threshold float)
returns string not null
language sql
as
$$
begin
    update retail_db.abt_buy.match_config
    set similarity_threshold = :new_threshold;
    return 'successfully updated similarity threshold to ' || :new_threshold;
end;
$$;

create or replace procedure update_product_matching()
returns string not null
language sql
as
$$
declare
    v_threshold float;
begin
    select max(similarity_threshold) into :v_threshold
    from retail_db.abt_buy.match_config;

    -- rebuild product_matches using the threshold
    create or replace table retail_db.abt_buy.product_matches as
    select abt_id, buy_id, similarity
    from (
        select
            abt_id,
            buy_id,
            similarity,
            row_number() over (partition by abt_id order by similarity desc) as rn
        from retail_db.abt_buy.similarity_scores
    )
    where rn = 1
      and similarity >= :v_threshold;

    return 'successfully updated product_matches table according to threshold ' || to_varchar(:v_threshold);
end;
$$;

select * from match_config;
call update_similarity_threshold(0.8);
call update_product_matching();