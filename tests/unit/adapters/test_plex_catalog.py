from types import SimpleNamespace

from plex_ingest.lib.adapters.plex_catalog import (
    _content_rating,
    _first_video_stream,
    _hdr_formats,
    _imdb_rating,
)


def _video_stream(
    colorTrc: str | None = None, DOVIPresent: bool = False
) -> SimpleNamespace:
    return SimpleNamespace(colorTrc=colorTrc, DOVIPresent=DOVIPresent)


def _item(video_streams: list[SimpleNamespace] | None = None) -> SimpleNamespace:
    if video_streams is None:
        return SimpleNamespace(media=[])
    part = SimpleNamespace(videoStreams=lambda: video_streams)
    return SimpleNamespace(media=[SimpleNamespace(parts=[part])])


def _rating(image: str | None, value: float) -> SimpleNamespace:
    return SimpleNamespace(image=image, value=value)


def test_first_video_stream_is_none_when_item_has_no_media() -> None:
    assert _first_video_stream(_item(video_streams=None)) is None


def test_first_video_stream_is_none_when_part_has_no_video_streams() -> None:
    assert _first_video_stream(_item(video_streams=[])) is None


def test_first_video_stream_returns_the_first_stream() -> None:
    stream = _video_stream()
    assert _first_video_stream(_item([stream])) is stream


def test_hdr_formats_empty_when_no_video_stream() -> None:
    assert _hdr_formats(None) == []


def test_hdr_formats_empty_for_sdr() -> None:
    assert _hdr_formats(_video_stream(colorTrc="bt709")) == []


def test_hdr_formats_hdr_for_hdr10_pq_transfer() -> None:
    assert _hdr_formats(_video_stream(colorTrc="smpte2084")) == ["HDR"]


def test_hdr_formats_hdr_for_hlg_transfer() -> None:
    assert _hdr_formats(_video_stream(colorTrc="arib-std-b67")) == ["HDR"]


def test_hdr_formats_includes_dv_alongside_hdr_when_dovi_present() -> None:
    assert _hdr_formats(_video_stream(colorTrc="smpte2084", DOVIPresent=True)) == [
        "HDR",
        "DV",
    ]


def test_hdr_formats_dovi_without_pq_fallback_still_counts_as_hdr() -> None:
    """A Dolby Vision profile 5 file has no HDR10-compatible base layer (no PQ
    colorTrc), but DV is itself an HDR format, so DOVIPresent alone must still imply
    "HDR" — not just "DV"."""
    assert _hdr_formats(_video_stream(colorTrc=None, DOVIPresent=True)) == [
        "HDR",
        "DV",
    ]


def test_imdb_rating_none_when_no_ratings() -> None:
    assert _imdb_rating(SimpleNamespace(ratings=[])) is None


def test_imdb_rating_picks_the_imdb_entry_when_not_first() -> None:
    """Regression test: this used to blindly take ratings[0], which would silently
    mis-tag a non-IMDb score (e.g. Rotten Tomatoes, listed first here) as
    imdb_rating."""
    ratings = [
        _rating("rottentomatoes://image.rating.ripe", 8.5),
        _rating("imdb://image.rating", 7.2),
    ]
    assert _imdb_rating(SimpleNamespace(ratings=ratings)) == 7.2


def test_imdb_rating_none_when_no_entry_has_an_imdb_image() -> None:
    ratings = [_rating("rottentomatoes://image.rating.ripe", 8.5)]
    assert _imdb_rating(SimpleNamespace(ratings=ratings)) is None


def test_imdb_rating_none_when_image_is_none() -> None:
    ratings = [_rating(None, 8.5)]
    assert _imdb_rating(SimpleNamespace(ratings=ratings)) is None


def test_content_rating_strips_locale_prefix() -> None:
    assert _content_rating(SimpleNamespace(contentRating="gb/15")) == "15"


def test_content_rating_strips_locale_prefix_with_letter_suffix() -> None:
    assert _content_rating(SimpleNamespace(contentRating="gb/12A")) == "12A"


def test_content_rating_passes_through_a_bare_us_style_value() -> None:
    assert _content_rating(SimpleNamespace(contentRating="PG-13")) == "PG-13"


def test_content_rating_passes_through_not_rated_with_no_prefix() -> None:
    assert _content_rating(SimpleNamespace(contentRating="Not Rated")) == "Not Rated"


def test_content_rating_strips_prefix_from_not_rated_too() -> None:
    assert _content_rating(SimpleNamespace(contentRating="gb/Not Rated")) == "Not Rated"


def test_content_rating_none_when_plex_has_no_rating() -> None:
    assert _content_rating(SimpleNamespace(contentRating=None)) is None
