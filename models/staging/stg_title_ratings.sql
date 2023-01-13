
{{
  config(
    tags = ["stage", "imdb"]
  )
}}

with base as (

    select * from {{ source('imdb', 'title_ratings') }}

),

movie_filter as(
    select movie_id from {{ ref('stg_title_basics') }}
),

final as (

    select
    tconst as movie_id,
    avarage_rating as rating,
    num_votes as vote_count
    from base b
    inner join movie_filter f
    on b.movie_id = f.movie_id
)

select * from final