import os


def resolve_database_url(
    value: str | None,
    *environment_variables: str,
) -> str | None:
    """Resolve an explicit URL before the configured environment fallbacks."""

    if value:
        return value
    for name in environment_variables:
        resolved = os.environ.get(name)
        if resolved:
            return resolved
    return None
