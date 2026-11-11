"""Keep automated tests away from the application's live session database."""

import os
import tempfile


# This is set before test modules are imported, so the persistence singleton
# uses an isolated file even when tests clear or rewrite state.
os.environ["STRADDLE_STATE_FILE"] = os.path.join(
    tempfile.gettempdir(),
    f"nautilus_straddle_pytest_{os.getpid()}.json",
)

# Tests must never inherit credentials from the developer's .env file. This
# prevents the launcher test suite from creating or closing real exchange orders.
os.environ["BYBIT_API_KEY"] = ""
os.environ["BYBIT_API_SECRET"] = ""

# A pytest plugin may import the persistence module before this conftest runs.
# Redirect the singleton explicitly as well as setting the environment variable.
from straddle import persistence as _persistence

_test_state_path = os.environ["STRADDLE_STATE_FILE"]
_persistence.state_persistence.filepath = _test_state_path
_persistence.state_persistence.state = _persistence.state_persistence._default_state()
