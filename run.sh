python3 -m firestore_sync --input gs://at_datalake_staging/MediaMath/log-level/attributed-events/2021/04/24/conversion/mm_attributed_events_102130_20210424_conversion_2021042500.txt\
    --collection_name mediamath_loglevel_conversions\
    --project_id at-engineering-staging \
    --service_account 363220685993-compute@developer.gserviceaccount.com \
    --experiment use_unsupported_python_version
