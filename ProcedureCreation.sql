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

create or replace procedure update_product_matches()
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
    model_name varchar,
    model_id number
) as 
select column1, column2 from values
    ('snowflake-arctic-embed-l-v2.0', 1),
    ('snowflake-arctic-embed-l-v2.0-8k', 2),
    ('nv-embed-qa-4', 3),
    ('multilingual-e5-large', 4),
    ('voyage-multilingual-2', 5),
    ('snowflake-arctic-embed-m-v1.5', 6),
    ('snowflake-arctic-embed-m', 7),
    ('e5-base-v2', 8);

alter table retail_db.abt_buy.match_config
add column default_model varchar;

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
  v_sql   string;
  v_model string;
begin
  -- Get model from config
  select model_name
    into v_model
  from retail_db.abt_buy.model_config
  limit 1;

  -- Create canonical view for ABT
  v_sql := '
    create or replace view retail_db.abt_buy.abt_canonical as
    select 
      price,
      id as product_id,
      concat_ws('' '', name, description) as clean_text
    from retail_db.abt_buy.abt
  ';
  execute immediate v_sql;

  -- Create canonical view for BUY
  v_sql := '
    create or replace view retail_db.abt_buy.buy_canonical as
    select 
      price,
      id as product_id,
      concat_ws('' '', name, manufacturer, description) as clean_text
    from retail_db.abt_buy.buy
  ';
  execute immediate v_sql;

  -- Generate embeddings for ABT
  v_sql := '
    create or replace table retail_db.abt_buy.abt_embeddings as
    select 
      product_id,
      clean_text,
      ai_embed(''' || v_model || ''', clean_text) as embedding
    from retail_db.abt_buy.abt_canonical
  ';
  execute immediate v_sql;

  -- Generate embeddings for BUY
  v_sql := '
    create or replace table retail_db.abt_buy.buy_embeddings as
    select 
      product_id,
      clean_text,
      ai_embed(''' || v_model || ''', clean_text) as embedding
    from retail_db.abt_buy.buy_canonical
  ';
  execute immediate v_sql;

  return 'Successfully generated canonical tables and embeddings for ABT and BUY tables';
end;
$$;

create or replace procedure update_similarity_scores()
returns string not null
language sql
as
$$
begin
    create or replace table retail_db.abt_buy.similarity_scores as
    with similarity_scores_cte as (
        select
            a.product_id as abt_id,
            b.product_id as buy_id,
            vector_cosine_similarity(a.embedding, b.embedding) as similarity
        from abt_embeddings a
        cross join buy_embeddings b
    )
    select
        abt_id,
        buy_id,
        similarity
    from similarity_scores_cte
    order by similarity desc;
    
    return 'successfully updated similarity scores table';
end;
$$;