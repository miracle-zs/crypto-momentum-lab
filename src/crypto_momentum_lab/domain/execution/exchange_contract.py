"""Exchange submission errors shared by Binance adapters and callers."""


class LiveSubmissionDisabledError(RuntimeError):
    pass


class ExchangeOrderRejectedError(RuntimeError):
    pass


class ExchangeOrderAlreadyAbsentError(RuntimeError):
    """The exchange explicitly confirmed that the target order is absent."""

    def __init__(
        self,
        message: str,
        *,
        exchange_code: int | None = None,
        exchange_message: str | None = None,
        http_status: int | None = None,
        open_orders_checked: bool = False,
    ) -> None:
        super().__init__(message)
        self.exchange_code = exchange_code
        self.exchange_message = exchange_message or message
        self.http_status = http_status
        self.open_orders_checked = open_orders_checked


class ExchangeSubmissionTimeoutError(TimeoutError):
    pass


class ExchangeOrderQueryUnknownError(RuntimeError):
    """Order lookup failed before the exchange state was known."""

    def __init__(
        self,
        message: str,
        *,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class ExchangeCancellationUnknownError(RuntimeError):
    """The cancel request outcome is unknown and needs reconciliation."""

    def __init__(
        self,
        message: str,
        *,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds
