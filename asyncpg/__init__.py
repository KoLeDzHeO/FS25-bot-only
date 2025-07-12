from contextlib import asynccontextmanager


class Pool:
    """Lightweight stub for asyncpg.Pool."""

    @asynccontextmanager
    async def acquire(self):
        yield self

    async def fetch(self, *args, **kwargs):
        return []

    async def execute(self, *args, **kwargs):
        pass

    async def executemany(self, *args, **kwargs):
        pass

    async def close(self):
        pass


async def create_pool(*args, **kwargs):
    return Pool()
