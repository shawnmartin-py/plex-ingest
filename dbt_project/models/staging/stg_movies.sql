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
        video_resolution,
        synced_at,
        list_filter(guids, g -> g like 'imdb://%')[1] as imdb_guid,
        -- Streaming-platform placeholder clips are ~4s stand-ins named
        -- "Title - Year - (Platform).ext" (see docs/vector-store-contract.md). The
        -- trailing-"(...)" filename shape alone isn't distinctive enough — some real
        -- BluRay/WEB-DL release groups (e.g. "Tigole") also end filenames in a
        -- parenthesized descriptor — so a placeholder is only recognized when the
        -- filename matches *and* the file itself is implausibly short for a real
        -- movie. 60s is generously below the shortest real download observed in this
        -- library (~99 minutes) and generously above the longest placeholder (~4s).
        -- regexp_extract returns '' rather than NULL on no match, hence the nullif.
        case
            when duration_ms < 60000
                then nullif(regexp_extract(file_path, '\(([^)]+)\)\.[a-zA-Z0-9]+$', 1), '')
        end as source_platform
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
    -- A placeholder clip's videoResolution reflects the ~4s stand-in file itself, not
    -- a real download's quality, so it's meaningless and dropped once source_platform
    -- is set — the two fields are mutually exclusive by construction.
    case when source_platform is null then video_resolution end as video_resolution,
    source_platform,
    synced_at
from resolved
-- Raw layer keeps every Plex item, including ones with no IMDb guid; staging is
-- where we apply the business rule that imdb_id is required downstream.
where imdb_guid is not null
