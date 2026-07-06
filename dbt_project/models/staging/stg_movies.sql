with source as (
    select * from {{ source('plex_ingest', 'raw_movies') }}
),

resolved as (
    select
        rating_key,
        title,
        year,
        content_rating,
        thumb_url,
        genres,
        imdb_rating,
        synced_at,
        list_filter(guids, g -> g like 'imdb://%')[1] as imdb_guid
    from source
)

select
    regexp_extract(imdb_guid, 'imdb://(.*)', 1) as imdb_id,
    rating_key,
    title,
    year,
    content_rating,
    thumb_url,
    genres,
    imdb_rating,
    synced_at
from resolved
-- Raw layer keeps every Plex item, including ones with no IMDb guid; staging is
-- where we apply the business rule that imdb_id is required downstream.
where imdb_guid is not null
