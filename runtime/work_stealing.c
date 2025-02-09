#include <stdbool.h>
#include <stdatomic.h>

#define InitialTaskArrayBytes (4096 * 4)

typedef struct
{
    Capture capture;
    TaskFn fn;
} Task;

_Static_assert(sizeof(Task) == 24, "Task must be 24 bytes big!");

typedef struct
{
    _Atomic uintptr_t capture_ptr;
    _Atomic uint64_t capture_number;
    _Atomic uintptr_t fn;
} AtomicTask;

_Static_assert(sizeof(AtomicTask) == 24, "AtomicTask must be 24 bytes big!");

typedef struct
{
    _Atomic uint64_t size;
    AtomicTask tasks[];
} AtomicTaskArray;

typedef struct
{
    _Atomic(AtomicTaskArray *) array;
    _Atomic uint64_t top;
    _Atomic uint64_t bottom;
} WorkStealingDeque;

static void AtomicStoreTask(AtomicTask *at, Task *t)
{
    atomic_store_explicit(&at->capture_ptr, (uintptr_t)t->capture.ptr, memory_order_relaxed);
    atomic_store_explicit(&at->capture_number, t->capture.number, memory_order_relaxed);
    atomic_store_explicit(&at->fn, (uintptr_t)t->fn, memory_order_relaxed);
}

static void AtomicLoadTask(Task *t, AtomicTask *at)
{
    t->capture.ptr = (void *)atomic_load_explicit(&at->capture_ptr, memory_order_relaxed);
    t->capture.number = atomic_load_explicit(&at->capture_number, memory_order_relaxed);
    t->fn = (TaskFn)atomic_load_explicit(&at->fn, memory_order_relaxed);
}

static bool InitDeque(WorkStealingDeque *deque, BufferPool *pool)
{
    void *initial_task_array;
    CHECK(AllocateBuffer(&initial_task_array, pool, InitialTaskArrayBytes));
    uint64_t num_tasks = (InitialTaskArrayBytes - sizeof(AtomicTaskArray)) / sizeof(AtomicTask);
    num_tasks = PreviousPowerOf2(num_tasks);

    atomic_store_explicit(&deque->array, initial_task_array, memory_order_relaxed);
    atomic_store_explicit(&deque->array->size, num_tasks, memory_order_relaxed);
    atomic_store_explicit(&deque->top, 0, memory_order_relaxed);
    atomic_store_explicit(&deque->bottom, 0, memory_order_relaxed);
    return true;
}

static bool ResizeDeque(WorkStealingDeque *deque, BufferPool *pool)
{
    AtomicTaskArray *array = atomic_load_explicit(&deque->array, memory_order_relaxed);
    uint64_t size = atomic_load_explicit(&array->size, memory_order_relaxed);
    uint64_t new_size = size << 1;
    uint64_t bytes = sizeof(AtomicTaskArray) + size * sizeof(AtomicTask);
    uint64_t new_bytes = sizeof(AtomicTaskArray) + new_size * sizeof(AtomicTask);
    AtomicTaskArray *new_array;
    CHECK(AllocateBuffer((void **)&new_array, pool, new_bytes));
    uint64_t top = atomic_load_explicit(&deque->top, memory_order_relaxed);
    uint64_t bottom = atomic_load_explicit(&deque->bottom, memory_order_relaxed);
    atomic_store_explicit(&new_array->size, new_size, memory_order_relaxed);
    for(uint64_t i = top; i < bottom; i++)
    {
        Task t;
        AtomicLoadTask(&t, &array->tasks[i & (size - 1)]);
        AtomicStoreTask(&new_array->tasks[i & (new_size - 1)], &t);
    }
    atomic_store_explicit(&deque->array, new_array, memory_order_release);
    return DeallocateBuffer(pool, array, bytes);
}

static bool PushTask(WorkStealingDeque *deque, BufferPool *pool, Task t)
{
    uint64_t bottom = atomic_load_explicit(&deque->bottom, memory_order_relaxed);
    uint64_t top = atomic_load_explicit(&deque->top, memory_order_acquire);
    AtomicTaskArray *array = atomic_load_explicit(&deque->array, memory_order_relaxed);
    uint64_t size = atomic_load_explicit(&array->size, memory_order_relaxed);
    if(bottom - top > size - 1)
    {
        CHECK(ResizeDeque(deque, pool));
        array = atomic_load_explicit(&deque->array, memory_order_relaxed);
    }
    size = atomic_load_explicit(&array->size, memory_order_relaxed);
    AtomicStoreTask(&array->tasks[bottom & (size - 1)], &t);
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&deque->bottom, bottom + 1, memory_order_relaxed);
    return true;
}

static Task PopTask(WorkStealingDeque *deque)
{
    uint64_t bottom = atomic_load_explicit(&deque->bottom, memory_order_relaxed) - 1;
    AtomicTaskArray *array = atomic_load_explicit(&deque->array, memory_order_relaxed);
    atomic_store_explicit(&deque->bottom, bottom, memory_order_relaxed);
    atomic_thread_fence(memory_order_seq_cst);
    uint64_t top = atomic_load_explicit(&deque->top, memory_order_relaxed);

    Task result = {};
    if(top <= bottom)
    {
        uint64_t size = atomic_load_explicit(&array->size, memory_order_relaxed);
        AtomicLoadTask(&result, &array->tasks[bottom & (size - 1)]);
        if(top == bottom)
        {
            if(!atomic_compare_exchange_strong_explicit(&deque->top, &top, top + 1, memory_order_seq_cst, memory_order_relaxed))
            {
                result.capture.number = 0;
                result.fn = 0;
            }
            atomic_store_explicit(&deque->bottom, bottom + 1, memory_order_relaxed);
        }
    }
    else
    {
        atomic_store_explicit(&deque->bottom, bottom + 1, memory_order_relaxed);
    }
    return result;
}

static Task StealTask(WorkStealingDeque *deque)
{
    uint64_t top = atomic_load_explicit(&deque->top, memory_order_acquire);
    atomic_thread_fence(memory_order_seq_cst);
    uint64_t bottom = atomic_load_explicit(&deque->bottom, memory_order_acquire);
    Task result = {};
    if(top < bottom)
    {
        AtomicTaskArray *array = atomic_load_explicit(&deque->array, memory_order_acquire);
        uint64_t size = atomic_load_explicit(&array->size, memory_order_relaxed);
        AtomicLoadTask(&result, &array->tasks[top & (size - 1)]);
        if(!atomic_compare_exchange_strong_explicit(&deque->top, &top, top + 1, memory_order_seq_cst, memory_order_relaxed))
        {
            result.capture.number = 0;
            result.fn = 0;
        }
    }
    return result;
}
