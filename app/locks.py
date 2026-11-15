"""Process-wide synchronisation primitives.

A single re-entrant lock serialises all schema/batch writers within the
process. Read paths never take it; WAL mode lets readers proceed while a
write transaction is in flight.
"""
from __future__ import annotations

import threading

writer_lock = threading.RLock()
