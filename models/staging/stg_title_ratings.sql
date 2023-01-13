
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
    f.movie_id,
    b.avarage_rating as rating,
    b.num_votes as vote_count
    from movie_filter f
    inner join base b
    on b.movie_id = f.movie_id
)

select * from final