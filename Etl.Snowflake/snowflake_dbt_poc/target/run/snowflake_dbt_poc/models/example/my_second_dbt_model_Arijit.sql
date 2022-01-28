
  create or replace  view DEMO_DB.PUBLIC.my_second_dbt_model_Arijit  as (
    -- Use the `ref` function to select from other models

select *
from DEMO_DB.PUBLIC.my_first_dbt_model_Arijit
where id = 1
  );
