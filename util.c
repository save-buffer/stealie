static uint64_t PreviousPowerOf2(uint64_t x)
{
    x |= (x >> 1);
    x |= (x >> 2);
    x |= (x >> 4);
    x |= (x >> 8);
    x |= (x >> 16);
    x |= (x >> 32);
    return x - (x >> 1);
}

typedef struct
{
    uint64_t state;
} Rng64;

// Adapted from Daniel Lemire's implementation of wyhash
static inline void RngSeed(Rng64 *rng, uint64_t seed)
{
    rng->state = seed;
}

static inline uint64_t RngNext(Rng64 *rng)
{
    rng->state += 0x60bee2bee120fc15ull;
    __uint128_t tmp;
    tmp = (__uint128_t)rng->state * 0xa3b195354a39b70dull;
    uint64_t m1 = (tmp >> 64) ^ tmp;
    tmp = (__uint128_t)m1 * 0x1b03738712fad5c9ull;
    uint64_t m2 = (tmp >> 64) ^ tmp;
    return m2;
}

#if defined(__APPLE__)
void *memset(void *str, int c, unsigned long n);
#else
void *memset(void *str, int c, uint64_t n);
#endif
