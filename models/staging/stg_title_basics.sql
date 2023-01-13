
with base as (

    select * from {{ source('imdb', 'title_basics') }}

),

final as (

    select
    tconst as movie_id,
    titly_type as type,
    primary_title as title,
    start_year as year,
    runtime_minutes,
    split(genres) as genres_arr
    from base
    where title_type = 'movie'
)

select * from final