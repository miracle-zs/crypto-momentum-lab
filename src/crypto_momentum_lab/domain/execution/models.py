from enum import StrEnum


class ExecutionRunMode(StrEnum):
    PAPER = "paper"
    PAPER_DAEMON = "paper_daemon"
    LIVE = "live"
