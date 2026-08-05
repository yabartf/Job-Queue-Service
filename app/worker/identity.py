"""Worker identity.

Unique per **slot**, not per process. Two slots sharing an id would let one
overwrite the other's job after a reclaim — see specs/04-claiming.md section 5.
The host and pid make a line of logs traceable back to a container.
"""

import os
import socket


def worker_id(slot: int) -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{slot}"
