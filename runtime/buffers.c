#define InitialBufferPoolSizeBytes 4096

typedef struct
{
    _Atomic uint64_t size;
    _Atomic(void *) buffers[];
} BufferArray;

typedef struct
{
    _Atomic uint64_t memory_limit;
    _Atomic(BufferArray *) array;
    _Atomic uint64_t top;
    _Atomic uint64_t bottom;
} BufferPool;

static bool AllocateBuffer(void **out_buffer, BufferPool *pool, uint64_t size)
{
    uint64_t my_memory_limit = atomic_load_explicit(&pool->memory_limit, memory_order_acquire);
    *out_buffer = NULL;
    do
    {
        if(my_memory_limit < size)
            FAIL(E_OutOfMemory);
    } while(!atomic_compare_exchange_weak_explicit(
                &pool->memory_limit,
                &my_memory_limit,
                my_memory_limit - size,
                memory_order_release,
                memory_order_relaxed));
    return MapVirtualMemory_PlatformSpecific(out_buffer, size);
}

static bool DeallocateBuffer(BufferPool *pool, void *buffer, uint64_t size)
{
    CHECK(UnmapVirtualMemory_PlatformSpecific(buffer, size));
    atomic_fetch_add_explicit(&pool->memory_limit, size, memory_order_acq_rel);
    return true;
}

static bool ResizeBufferPool(BufferPool *pool)
{
    BufferArray *array = atomic_load_explicit(&pool->array, memory_order_relaxed);
    uint64_t size = atomic_load_explicit(&array->size, memory_order_relaxed);
    uint64_t new_size = size << 1;
    uint64_t bytes = sizeof(BufferArray) + size * sizeof(void *);
    uint64_t new_bytes = sizeof(BufferArray) + new_size * sizeof(void *);
    BufferArray *new_array;
    CHECK(AllocateBuffer((void **)&new_array, pool, new_bytes));
    uint64_t top = atomic_load_explicit(&pool->top, memory_order_relaxed);
    uint64_t bottom = atomic_load_explicit(&pool->bottom, memory_order_relaxed);
    atomic_store_explicit(&new_array->size, new_size, memory_order_relaxed);
    for(uint64_t i = top; top < bottom; top++)
    {
        void *buf = atomic_load_explicit(&array->buffers[i % size], memory_order_relaxed);
        atomic_store_explicit(&new_array->buffers[i % new_size], buf, memory_order_relaxed);
    }
    atomic_store_explicit(&pool->array, new_array, memory_order_release);
    return DeallocateBuffer(pool, array, bytes);
}

static void *GetBuffer(BufferPool *pool)
{
    uint64_t bottom = atomic_load_explicit(&pool->bottom, memory_order_relaxed) - 1;
    atomic_store_explicit(&pool->bottom, bottom, memory_order_relaxed);
    atomic_thread_fence(memory_order_seq_cst);
    uint64_t top = atomic_load_explicit(&pool->top, memory_order_relaxed);

    void *buffer;
    if(top <= bottom)
    {
        uint64_t capacity = atomic_load_explicit(&pool->array->size, memory_order_relaxed);
        buffer = atomic_load_explicit(&pool->array->buffers[bottom % capacity], memory_order_relaxed);
        if(top == bottom)
        {
            if(!atomic_compare_exchange_strong_explicit(&pool->top, &top, top + 1, memory_order_seq_cst, memory_order_relaxed))
                buffer = NULL;
            atomic_store_explicit(&pool->bottom, bottom + 1, memory_order_relaxed);
        }
    }
    else
    {
        buffer = NULL;
        atomic_store_explicit(&pool->bottom, bottom + 1, memory_order_relaxed);
    }
    return buffer;
}

static bool ReturnBuffer(void *buffer, BufferPool *pool)
{
    uint64_t bottom = atomic_load_explicit(&pool->bottom, memory_order_relaxed);
    uint64_t top = atomic_load_explicit(&pool->top, memory_order_acquire);
    BufferArray *array = atomic_load_explicit(&pool->array, memory_order_relaxed);
    uint64_t capacity = atomic_load_explicit(&array->size, memory_order_relaxed);
    if(bottom - top > capacity)
    {
        CHECK(ResizeBufferPool(pool));
        array = atomic_load_explicit(&pool->array, memory_order_relaxed);
        capacity = atomic_load_explicit(&array->size, memory_order_relaxed);
    }
    atomic_store_explicit(&array->buffers[bottom % capacity], buffer, memory_order_relaxed);
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&pool->bottom, bottom + 1, memory_order_release);
    return true;
}

static void *StealBuffer(BufferPool *pool)
{
    uint64_t top = atomic_load_explicit(&pool->top, memory_order_acquire);
    atomic_thread_fence(memory_order_seq_cst);
    uint64_t bottom = atomic_load_explicit(&pool->bottom, memory_order_acquire);
    void *result = NULL;
    if(top < bottom)
    {
        BufferArray *array = atomic_load_explicit(&pool->array, memory_order_acquire);
        uint64_t size = atomic_load_explicit(&array->size, memory_order_relaxed);
        result = atomic_load_explicit(&array->buffers[top % size], memory_order_relaxed);
        if(!atomic_compare_exchange_strong_explicit(&pool->top, &top, top + 1, memory_order_seq_cst, memory_order_relaxed))
            return NULL;
    }
    return result;
}

static bool InitThread_Buffers(BufferPool *pool, uint64_t memory_limit)
{
    atomic_store_explicit(&pool->memory_limit, memory_limit, memory_order_relaxed);
    void *initial_buffer_array;
    CHECK(AllocateBuffer(&initial_buffer_array, pool, InitialBufferPoolSizeBytes));
    uint64_t num_buffers = (InitialBufferPoolSizeBytes - sizeof(BufferArray)) / sizeof(void *);
    num_buffers = PreviousPowerOf2(num_buffers);

    atomic_store_explicit(&pool->array, initial_buffer_array, memory_order_relaxed);
    atomic_store_explicit(&pool->array->size, num_buffers, memory_order_relaxed);
    atomic_store_explicit(&pool->top, 1, memory_order_relaxed);
    atomic_store_explicit(&pool->bottom, 1, memory_order_relaxed);
    return true;
}

static bool AllocateCustomSlab(Slab *out_slab, BufferPool *pool, uint64_t size)
{
    out_slab->size = size;
    out_slab->offset = 0;
    return AllocateBuffer(&out_slab->ptr, pool, size);
}

static bool AllocateSlab(Slab *out_slab, BufferPool *pool)
{
    return AllocateCustomSlab(out_slab, pool, SlabSize);
}

void *AllocateFromSlab(Slab *slab, uint64_t size)
{
    if(slab->offset + size >= slab->size)
        return NULL;
    void *result = slab->ptr + slab->offset;
    slab->offset += size;
    return result;
}

static Arena InitArena(Slab *slab)
{
    Arena result = AllocateFromSlab(slab, sizeof(struct Arena));
    assert(result);
    result->prev = NULL;
    result->slab = *slab;
    return result;
}

bool AllocateFromArena(void **out_ptr, Arena *in_out_arena, uint64_t size)
{
    void *result = AllocateFromSlab(&(*in_out_arena)->slab, size);
    if(result)
    {
        *out_ptr = result;
        return true;
    }

    Slab next;
    CHECK(GetSlab(&next));
    Arena new_arena = InitArena(&next);
    if(size > GetSpaceLeftInArena(new_arena))
        FAIL_MSG(E_Invalid, "Requested allocation is too big! Please chunk it");

    new_arena->prev = *in_out_arena;
    *in_out_arena = new_arena;
    *out_ptr = AllocateFromSlab(&(*in_out_arena)->slab, size);
    assert(*out_ptr);
    return true;
}

bool GetArena(Arena *out_arena)
{
    Slab slab;
    CHECK(GetSlab(&slab));
    *out_arena = InitArena(&slab);
    return true;
}

ArenaMark SetArenaMark(Arena arena)
{
    return (ArenaMark) { .arena = arena, .offset = arena->slab.offset };
}

bool RestoreArenaMark(Arena *in_out_arena, ArenaMark mark)
{
    while(*in_out_arena != mark.arena)
    {
        if(!*in_out_arena)
            FAIL_MSG(E_Invalid, "Mark is not contained within the given arena");
        Arena prev_arena = (*in_out_arena)->prev;
        CHECK(ReturnSlab((*in_out_arena)->slab));
        *in_out_arena = prev_arena;
    }
    if(*in_out_arena)
        (*in_out_arena)->slab.offset = mark.offset;
    return true;
}
