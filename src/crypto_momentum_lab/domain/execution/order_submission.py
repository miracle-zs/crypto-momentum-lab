"""Local precondition errors raised before exchange submissions."""


class OrderPreSubmissionError(RuntimeError):
    """A local precondition failed before an exchange write was attempted."""
