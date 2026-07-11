"""Domain enums describing where a movie's file actually comes from: a real
downloaded file at some resolution, or a short placeholder clip standing in for a
title that's only available on a streaming platform (see
docs/vector-store-contract.md's "source_platform"/"video_resolution" fields).
Mirrored by hand in plex-rag's app/models/media_item.py, per that doc's
"no shared package" contract philosophy."""

import enum


class VideoResolution(enum.Enum):
    """Mirrors Plex's own `Media.videoResolution` vocabulary exactly, so a raw value
    read off a `Movie`'s `Media` maps 1:1 via `VideoResolution(raw_value)` with no
    translation table to maintain."""

    SD = "sd"
    R480 = "480"
    R576 = "576"
    R720 = "720"
    R1080 = "1080"
    R4K = "4k"


class StreamingSource(enum.Enum):
    """The platform tag embedded in a placeholder clip's filename, e.g.
    "Title - Year - (Netflix).mp4" -> StreamingSource("Netflix"). A closed set by
    design: a new platform means adding a member here, not accepting an arbitrary
    string, so a naming-convention typo or an unplanned platform surfaces as a loud
    ValueError in stg_movies_reader rather than silently mis-tagging a movie."""

    NETFLIX = "Netflix"
    DISNEY_PLUS = "Disney+"
