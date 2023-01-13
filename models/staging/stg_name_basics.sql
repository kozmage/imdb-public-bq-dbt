{{
  config(
    tags = ["stage", "imdb"]
  )
}}

with base as (

    select * from {{ source('imdb', 'name_basics') }}

),

movie_filter as(
    select movie_id from {{ ref('stg_title_ratings') }}
),

formated as (

    select
    nconst as person_id,
    primary_name as name,
    primary_profession as professions,
    split(known_for_titles) as titles_arr
    from base
),

filtered as (

    select
    person_id,
    name,
    professions,
    array(
        select distinct u from movie_filter 
        inner join unnest(titles_arr) u
        on u = movie_id
    ) as titles_arr
    from formated
),

final as (

    select *
    from filtered
    where professions like '%director%'
    or professions like '%actor%'
    or professions like '%writer%'
    or professions like '%actress%'
    or professions like '%producer%'
)

select * from final