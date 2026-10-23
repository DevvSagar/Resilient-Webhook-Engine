import asyncio
from app.core.redis import init_redis, close_redis


async def main():
    print("Connecting to Redis...")
    client = await init_redis()

    # 1. PING test
    pong = await client.ping()
    print(f"1. Ping response: {pong} (Expected: True)")

    # 2. Key-Value & TTL test
    await client.set("test_key", "hello_relayhook", ex=10)
    val = await client.get("test_key")
    ttl = await client.ttl("test_key")
    print(f"2. Retrieved key: '{val}' | TTL remaining: {ttl}s")

    # 3. Atomic Increment test
    counter = await client.incr("test_counter")
    print(f"3. Atomic increment test: counter = {counter}")

    # Cleanup
    await client.delete("test_key", "test_counter")
    await close_redis()
    print("Redis connection verified and closed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
