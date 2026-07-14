import dagster as dg

poll_plex_job = dg.define_asset_job(
    "poll_plex_job",
    # stg_movies (dbt) depends on raw_movies and carries no automation_condition of
    # its own, so it needs a direct trigger here too, not just raw_movies —
    # otherwise it would never re-run after the initial materialization. Asset keys
    # given as strings (rather than imported symbols) since define_asset_job's
    # selection must be a homogeneous sequence and stg_movies is a dbt-generated
    # asset, not a plain @dg.asset function to import.
    selection=["raw_movies", "stg_movies", "stg_watch_history"],
)

# UTC — this repo has no established local-timezone convention yet (see
# docs/pipeline-design.md's open "Scheduling cadence" item). Pass
# execution_timezone="<IANA tz>" here if 1am should be local time instead.
poll_plex_schedule = dg.ScheduleDefinition(
    job=poll_plex_job,
    cron_schedule="0 1 * * *",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(jobs=[poll_plex_job], schedules=[poll_plex_schedule])
