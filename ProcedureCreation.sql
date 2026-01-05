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

call update_similarity_threshold(0.8);

select * from match_config;