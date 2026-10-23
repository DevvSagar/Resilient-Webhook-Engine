import asyncio
from app.core.redis import init_redis, close_redis
from app.services.rate_limiter import check_rate_limit


async def main():
    redis = await init_redis()
    sub_id = "test_sub_dominos"
    nike_id = "test_sub_nike"

    # Clean any leftover test keys
    await redis.delete(f"ratelimit:sub:{sub_id}", f"ratelimit:sub:{nike_id}")

    print("=====================================================")
    print("🚦 TESTING TOKEN-BUCKET RATE LIMITER WITH REDIS")
    print("=====================================================")
    print("Configuration: Capacity = 5 tokens | Refill Rate = 2.0 tokens/sec\n")

    # 1. Drain the bucket: 5 rapid requests
    print("📦 Phase 1: Rapid Fire 5 Requests (Burst to Domino's)...")
    for i in range(1, 6):
        res = await check_rate_limit(redis, sub_id, capacity=5, refill_rate=2.0)
        print(f"   Order #{i}: Allowed={res.allowed} | Remaining Tokens={res.remaining_tokens:.2f}")

    # 2. 6th request immediately (Bucket should be empty)
    print("\n🛑 Phase 2: Immediate 6th Request (Over Quota)...")
    res_6 = await check_rate_limit(redis, sub_id, capacity=5, refill_rate=2.0)
    print(f"   Order #6: Allowed={res_6.allowed} | Remaining Tokens={res_6.remaining_tokens:.2f} | Retry-After={res_6.retry_after:.2f}s")
    assert not res_6.allowed, "Order #6 should have been rejected!"

    # 3. Wait for token to drip in (0.6 seconds @ 2 tokens/sec adds > 1 token)
    print(f"\n⏳ Phase 3: Sleeping {res_6.retry_after}s for token to drip into Domino's jar...")
    await asyncio.sleep(res_6.retry_after + 0.1)

    res_7 = await check_rate_limit(redis, sub_id, capacity=5, refill_rate=2.0)
    print(f"   Order #7 (After Refill): Allowed={res_7.allowed} | Remaining Tokens={res_7.remaining_tokens:.2f}")
    assert res_7.allowed, "Order #7 should have been allowed after refill!"

    # 4. Multi-Tenant Isolation Check (Nike's Jar)
    print("\n👟 Phase 4: Checking Multi-Tenant Isolation (Nike)...")
    res_nike = await check_rate_limit(redis, nike_id, capacity=5, refill_rate=2.0)
    print(f"   Nike Order #1: Allowed={res_nike.allowed} | Remaining Tokens={res_nike.remaining_tokens:.2f}")
    assert res_nike.allowed and res_nike.remaining_tokens == 4.0, "Nike should have a fresh bucket!"
    print("   ✅ Nike was completely unaffected by Domino's running out of tokens!")

    # Cleanup
    await redis.delete(f"ratelimit:sub:{sub_id}", f"ratelimit:sub:{nike_id}")
    await close_redis()

    print("\n=====================================================")
    print("🎉 ALL RATE LIMITER TESTS PASSED 100%!")
    print("=====================================================")


if __name__ == "__main__":
    asyncio.run(main())
