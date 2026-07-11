from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from plex_ingest.defs.resources.synopsis_judge import SynopsisJudgeResource
from plex_ingest.lib.ports import SynopsisMatchResult


def test_check_delegates_to_adapter(mocker: MockerFixture) -> None:
    resource = SynopsisJudgeResource()
    mock_adapter: MagicMock = mocker.MagicMock()
    mock_adapter.check.return_value = SynopsisMatchResult(matches=True, reason="ok")
    resource._adapter = mocker.MagicMock(return_value=mock_adapter)  # type: ignore[method-assign]

    result = resource.check(title="Test Film", year=2020, synopsis="A great film.")

    assert result == SynopsisMatchResult(matches=True, reason="ok")
    mock_adapter.check.assert_called_once_with(
        title="Test Film", year=2020, synopsis="A great film."
    )
