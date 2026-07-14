import os

import dagster as dg

from plex_ingest.lib.adapters.omdb_client import OmdbRuntimeLookup
from plex_ingest.lib.ports import RuntimeLookup


class OmdbResource(dg.ConfigurableResource):
    """Config + adapter factory only — the OMDb HTTP call lives in
    lib/adapters/omdb_client.py, behind the RuntimeLookup port.

    `api_key` is deliberately plain `str | None`, not `dg.EnvVar`: this integration is
    optional, not required config, so a missing OMDB_API_KEY must make
    `streaming_runtime` a clean no-op (runtime stays NULL for streaming-platform
    movies) rather than a startup error — `dg.EnvVar` raises if its variable is
    unset."""

    api_key: str | None = None

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def _adapter(self) -> RuntimeLookup:
        assert self.api_key is not None  # noqa: S101 — guarded by is_configured() at call sites
        return OmdbRuntimeLookup(self.api_key)

    def fetch_runtime_minutes(self, imdb_id: str) -> int | None:
        return self._adapter().fetch_runtime_minutes(imdb_id)


defs = dg.Definitions(
    resources={"omdb": OmdbResource(api_key=os.environ.get("OMDB_API_KEY"))}
)
