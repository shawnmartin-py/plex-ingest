from types import SimpleNamespace

from plex_ingest.lib.adapters.plex_catalog import _first_video_stream, _hdr_formats


def _video_stream(
    colorTrc: str | None = None, DOVIPresent: bool = False
) -> SimpleNamespace:
    return SimpleNamespace(colorTrc=colorTrc, DOVIPresent=DOVIPresent)


def _item(video_streams: list[SimpleNamespace] | None = None) -> SimpleNamespace:
    if video_streams is None:
        return SimpleNamespace(media=[])
    part = SimpleNamespace(videoStreams=lambda: video_streams)
    return SimpleNamespace(media=[SimpleNamespace(parts=[part])])


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
