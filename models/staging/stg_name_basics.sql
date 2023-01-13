{{
  config(
    tags = ["stage", "imdb"]
  )
}}

with base as (

    select * from {{ source('imdb', 'name_basics') }}

),

formated as (

    select
    nconst as person_id,
    primary_name as name,
    primary_profession as professions,
    split(known_for_titles) as titles_arr
    from base
),

final as (

    select *
    from formated
    where professions like '%director%'
    or professions like '%actor%'
    or professions like '%writer%'
    or professions like '%actress%'
    or professions like '%producer%'
)

select * from final