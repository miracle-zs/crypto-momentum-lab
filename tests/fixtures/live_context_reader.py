"""Complete context reader fake for tests supplying a context loader."""


class ContextReader:
    def __init__(self, loader):
        self.loader = loader
        self.generation = 0

    async def __call__(self, state):
        return await self.loader(state)

    def is_current(self, context):
        observed_at = context.account_observed_at
        return (
            observed_at is not None
            and (context.now - observed_at).total_seconds() <= 60
        )

    def invalidate(self, event=None):
        self.generation += 1


def context_reader(loader):
    if all(
        hasattr(loader, name) for name in ("generation", "is_current", "invalidate")
    ):
        return loader
    return ContextReader(loader)
