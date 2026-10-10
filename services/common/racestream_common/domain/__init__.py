"""Shared domain model.

``SessionState`` lives here rather than in the processor because two services
derive it independently: the stream processor, which persists what it derives,
and the API, which maintains its own copy to serve the live WebSocket view.

Putting it in `common` is what keeps those two from drifting. A sector-merge
rule that differs between the stored data and the live view would be the worst
kind of bug: one that only shows up as a disagreement between two screens.
"""

from racestream_common.domain.state import DriverState, SectorTimes, SessionState

__all__ = ["DriverState", "SectorTimes", "SessionState"]
