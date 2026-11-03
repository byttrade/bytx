"""BingX: status for health(). The API docs have no status / maintenance endpoint
(https://bingx-api.github.io/docs-v3/) — status ``unknown``; availability and latency come from probing
server time GET /openApi/swap/v2/server/time (done by health()).
"""
from __future__ import annotations

from typing import Any

from bytx.health import StatusResult

DOC = None


async def system_status(ex: Any) -> StatusResult:
    return StatusResult('unknown', DOC, message='BingX has no documented status endpoint',
                        probed=False)
