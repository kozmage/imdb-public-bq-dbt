
{{
  config(
    tags = ["intermediate", "imdb"]
  )
}}

with names as(
    select * from {{ ref('stg_name_basics') }}
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
    array_agg(struct(
        u as ID,
        t.title as title,
        t.year as year,
        t.runtime_minutes as length,
        t.genres_arr as genres,
        r.rating as rating,
        r.vote_count as vote_count
    ))
    as info
    from names n , 
    unnest(n.titles_arr) u
    left outer join titles t
    on u = t.movie_id
    left outer join ratings r
    on u = r.movie_id
    group by 1, 2, 3
)

select * from final



/*
    array(
        select 
        ot.f0_
        from names sn, unnest(sn.titles_arr) ot
        inner join titles st
        on ot.f0_ = st.movie_id
        inner join ratings sr
        on ot.f0_ = sr.movie_id
    ) as titles_arr
*/