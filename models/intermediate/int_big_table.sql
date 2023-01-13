
{{
  config(
    tags = ["intermediate", "imdb"]
  )
}}

with names as(
    select * from {{ ref('stg_names_basics') }}
),

titles as(
    select * from {{ ref('stg_title_basics') }}
),

ratings as(
    select * from {{ ref('stg_title_ratings') }}
),

final as (

    select
    n.person_id,
    n.name,
    n.professions,
    array(
        select * 
        from unnest(n.titles_arr) ot
        inner join titles st
        on ot.x = st.movie_id
        inner join ratings sr
        on ot.x = sr.movie_id
    ) as titles_arr
    from names n
    left outer join titles t
    on
    left outer join ratings r
    on
)

select * from final