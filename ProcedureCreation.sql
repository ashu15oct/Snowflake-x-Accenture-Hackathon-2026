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

create or replace table retail_db.abt_buy.model_config (
    model_name varchar()
) as 
select column1 as model_name FROM VALUES 
    ('snowflake-arctic-embed-l-v2.0'),
    ('snowflake-arctic-embed-l-v2.0-8k'),
    ('nv-embed-qa-4'),
    ('multilingual-e5-large'),
    ('voyage-multilingual-2'),
    ('snowflake-arctic-embed-m-v1.5'),
    ('snowflake-arctic-embed-m'),
    ('e5-base-v2');

alter table retail_db.abt_buy.match_config
add column default_model varchar;

alter table retail_db.abt_buy.model_config
add column model_id number;

update retail_db.abt_buy.model_config as mc
set model_order = v.pos
from (
  select * from values
    (1, 'snowflake-arctic-embed-l-v2.0'),
    (2, 'snowflake-arctic-embed-l-v2.0-8k'),
    (3, 'nv-embed-qa-4'),
    (4, 'multilingual-e5-large'),
    (5, 'voyage-multilingual-2'),
    (6, 'snowflake-arctic-embed-m-v1.5'),
    (7, 'snowflake-arctic-embed-m'),
    (8, 'e5-base-v2')
) v(pos, model_name)
where mc.model_name = v.model_name;

update retail_db.abt_buy.match_config
set default_model = (
    select model_name
    from retail_db.abt_buy.model_config
    where model_id=7
);

create or replace procedure generate_canonical_and_embeddings()
returns string not null
language sql
as
$$
declare
v_model string;
begin
select default_model
into :v_model
from retail_db.abt_buy.match_config
where default_model is not null
limit 1;
    -- Create canonical view for ABT
    create or replace view retail_db.abt_buy.abt_canonical as
    select 
        price,
        id as product_id,
        concat_ws(' ', name, description) as clean_text
    from retail_db.abt_buy.abt;

    -- Create canonical view for BUY
    create or replace view retail_db.abt_buy.buy_canonical as
    select 
        price,
        id as product_id,
        concat_ws(' ', name, manufacturer, description) as clean_text
    from retail_db.abt_buy.buy;

    -- Generate embeddings for ABT
    create or replace table retail_db.abt_buy.abt_embeddings as
    select 
        product_id,
        clean_text,
        ai_embed(:v_model, clean_text) as embedding
    from retail_db.abt_buy.abt_canonical;

    -- Generate embeddings for BUY
    create or replace table retail_db.abt_buy.buy_embeddings as
    select 
        product_id,
        ai_embed(:v_model, clean_text) as embedding
    from retail_db.abt_buy.buy_canonical;

    return 'Successfully generated canonical views and embeddings for ABT and BUY tables';
end;
$$;

call generate_canonical_and_embeddings();