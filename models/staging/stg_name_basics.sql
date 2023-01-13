
{{
  config(
    tags = ["stage", "imdb"]
  )
}}

with base as (

    select * from {{ source('imdb', 'name_basics') }}

),

final as (

    select
    nconst as person_id,
    primary_name as name,
    split(primary_profession) as professions,
    split(known_for_titles) as titles_arr
    from base
)

select * from final