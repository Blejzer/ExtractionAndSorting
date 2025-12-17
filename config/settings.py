import os

def env_bool(key, default=False):
    return os.getenv(key, str(default)).lower() in ("1", "true", "yes", "on")

DEBUG_PRINT = env_bool("DEBUG_PRINT")
PREVIEW_MODE = env_bool("PREVIEW_MODE")
IMPORT_DRY_RUN = env_bool("IMPORT_DRY_RUN")
REQUIRE_PARTICIPANTS_LIST = env_bool("REQUIRE_PARTICIPANTS_LIST")