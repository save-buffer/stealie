#if defined(__APPLE__)
#define LogConcurrentIOs 4
#else
#define LogConcurrentIOs 7
#endif
#define ConcurrentIOs (1 << LogConcurrentIOs)
#define InitialIoRequestPoolSizeBytes 4096

// This is a lock-free circular free-list. The current thread may push and pop the top, while thieves may
// insert to the bottom. 
typedef struct
{
    AtomicTask tasks[ConcurrentIOs];
    _Atomic(uint64_t) free[ConcurrentIOs];
    _Atomic uint64_t top;
    _Atomic uint64_t bottom;
} IoContinuationList;

typedef enum
{
    IO_Null = 0,
    IO_Read,
    IO_Write,
    IO_OpenReadClose,
    IO_OpenWriteClose,
} IoOpcode;

typedef struct
{
    IoOpcode op_code;
    union
    {
        FileHandle fd;
        const char *filename;
    };
    void *buffer;
    uint64_t offset;
    uint64_t num_bytes;
    uintptr_t on_finished;
    uintptr_t capture_ptr;
    uint64_t capture_number;
} IoRequest;

typedef struct
{
    _Atomic IoOpcode op_code;
    _Atomic FileHandle fd;
    _Atomic(void *) buffer;
    _Atomic uint64_t offset;
    _Atomic uint64_t num_bytes;
    _Atomic uintptr_t on_finished;
    _Atomic uintptr_t capture_ptr;
    _Atomic uint64_t capture_number;
} AtomicIoRequest;

typedef struct
{
    _Atomic uint64_t size;
    AtomicIoRequest requests[];
} AtomicIoRequestArray;

typedef struct
{
    _Atomic(AtomicIoRequestArray *) array;
    _Atomic uint64_t top;
    _Atomic uint64_t bottom;
} IoRequestPool;

static void AtomicStoreIoRequest(AtomicIoRequest *aior, IoRequest *ior)
{
    atomic_store_explicit(&aior->op_code, ior->op_code, memory_order_relaxed);
    atomic_store_explicit(&aior->fd, ior->fd, memory_order_relaxed);
    atomic_store_explicit(&aior->buffer, ior->buffer, memory_order_relaxed);
    atomic_store_explicit(&aior->offset, ior->offset, memory_order_relaxed);
    atomic_store_explicit(&aior->num_bytes, ior->num_bytes, memory_order_relaxed);
    atomic_store_explicit(&aior->on_finished, ior->on_finished, memory_order_relaxed);
    atomic_store_explicit(&aior->capture_ptr, ior->capture_ptr, memory_order_relaxed);
    atomic_store_explicit(&aior->capture_number, ior->capture_number, memory_order_relaxed);
}

static void AtomicLoadIoRequest(IoRequest *ior, AtomicIoRequest *aiour)
{
    ior->op_code = atomic_load_explicit(&aiour->op_code, memory_order_relaxed);
    ior->fd = atomic_load_explicit(&aiour->fd, memory_order_relaxed);
    ior->buffer = atomic_load_explicit(&aiour->buffer, memory_order_relaxed);
    ior->offset = atomic_load_explicit(&aiour->offset, memory_order_relaxed);
    ior->num_bytes = atomic_load_explicit(&aiour->num_bytes, memory_order_relaxed);
    ior->on_finished = atomic_load_explicit(&aiour->on_finished, memory_order_relaxed);
    ior->capture_ptr = atomic_load_explicit(&aiour->capture_ptr, memory_order_relaxed);
    ior->capture_number = atomic_load_explicit(&aiour->capture_number, memory_order_relaxed);
}

static bool InitIoContinuationList(IoContinuationList *io_continuation_list)
{
    for(uint64_t i = 0; i < ConcurrentIOs; i++)
        atomic_store_explicit(&io_continuation_list->free[i], i, memory_order_relaxed);
    atomic_store_explicit(&io_continuation_list->top, 0, memory_order_relaxed);
    atomic_store_explicit(&io_continuation_list->bottom, ConcurrentIOs - 1, memory_order_relaxed);
    return true;
}

static bool InitIoRequestPool(IoRequestPool *io_request_pool, BufferPool *pool)
{
    void *initial_io_request_pool;
    CHECK(AllocateBuffer(&initial_io_request_pool, pool, InitialIoRequestPoolSizeBytes));
    uint64_t num_ios = (InitialIoRequestPoolSizeBytes - sizeof(AtomicIoRequestArray)) / sizeof(AtomicIoRequest);
    num_ios = PreviousPowerOf2(num_ios);
    
    atomic_store_explicit(&io_request_pool->array, initial_io_request_pool, memory_order_relaxed);
    atomic_store_explicit(&io_request_pool->array->size, num_ios, memory_order_relaxed);
    atomic_store_explicit(&io_request_pool->top, 1, memory_order_relaxed);
    atomic_store_explicit(&io_request_pool->bottom, 1, memory_order_relaxed);
    return true;
}

static bool InitThread_Files(
    IoRequestPool *io_request_pool,
    IoContinuationList *io_continuation_list,
    AsyncIoContext *io_context,
    BufferPool *pool)
{
    CHECK(InitIoContinuationList(io_continuation_list));
    CHECK(InitIoRequestPool(io_request_pool, pool));
    return InitAsyncIO_PlatformSpecific(io_context, LogConcurrentIOs);
}

static uint64_t PushIoContinuation(IoContinuationList *list, Task t)
{
    uint64_t top = atomic_fetch_add(&list->top, 1);
    uint64_t result = atomic_load(&list->free[top % ConcurrentIOs]);
    AtomicStoreTask(&list->tasks[result], &t);
    return result;
}

static Task FreeContinuation(IoContinuationList *list, uint64_t idx)
{
    Task result;
    AtomicLoadTask(&result, &list->tasks[idx]);
    uint64_t old_top = atomic_fetch_sub(&list->top, 1);
    atomic_store(&list->free[old_top % ConcurrentIOs], idx);
    return result;
}

static Task FreeContinuation_Thief(IoContinuationList *list, uint64_t idx)
{
    Task result;
    AtomicLoadTask(&result, &list->tasks[idx]);
    uint64_t old_bottom = atomic_fetch_add(&list->bottom, 1);
    atomic_store_explicit(&list->free[old_bottom % ConcurrentIOs], idx, memory_order_relaxed);
    return result;
}

static bool ResizeIoRequestPool(IoRequestPool *io_request_pool, BufferPool *pool)
{
    AtomicIoRequestArray *array = atomic_load_explicit(&io_request_pool->array, memory_order_relaxed);
    uint64_t size = atomic_load_explicit(&array->size, memory_order_relaxed);
    uint64_t new_size = size << 1;
    uint64_t bytes = sizeof(AtomicIoRequestArray) + size * sizeof(AtomicIoRequest);
    uint64_t new_bytes = sizeof(AtomicIoRequestArray) + new_size * sizeof(AtomicIoRequest);
    AtomicIoRequestArray *new_array;
    CHECK(AllocateBuffer((void **)&new_array, pool, new_bytes));
    uint64_t top = atomic_load_explicit(&io_request_pool->top, memory_order_relaxed);
    uint64_t bottom = atomic_load_explicit(&io_request_pool->bottom, memory_order_relaxed);
    atomic_store_explicit(&new_array->size, new_size, memory_order_relaxed);
    for(uint64_t i = top; i < bottom; i++)
    {
        IoRequest request;
        AtomicLoadIoRequest(&request, &array->requests[i & (size - 1)]);
        AtomicStoreIoRequest(&new_array->requests[i & (new_size - 1)], &request);
    }
    atomic_store_explicit(&io_request_pool->array, new_array, memory_order_release);
    return DeallocateBuffer(pool, array, bytes);
}

static bool PushIoRequest(IoRequestPool *request_pool, IoRequest *request, BufferPool *pool)
{
    uint64_t bottom = atomic_load_explicit(&request_pool->bottom, memory_order_relaxed);
    uint64_t top = atomic_load_explicit(&request_pool->top, memory_order_acquire);
    AtomicIoRequestArray *array = atomic_load_explicit(&request_pool->array, memory_order_relaxed);
    uint64_t size = atomic_load_explicit(&array->size, memory_order_relaxed);
    if(bottom - top > size - 1)
    {
        CHECK(ResizeIoRequestPool(request_pool, pool));
        array = atomic_load_explicit(&request_pool->array, memory_order_relaxed);
    }
    size = atomic_load_explicit(&array->size, memory_order_relaxed);
    AtomicStoreIoRequest(&array->requests[bottom & (size - 1)], request);
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&request_pool->bottom, bottom + 1, memory_order_relaxed);
    return true;
}

static void PopIoRequest(IoRequest *out_request, IoRequestPool *request_pool)
{
    uint64_t bottom = atomic_load_explicit(&request_pool->bottom, memory_order_relaxed) - 1;
    AtomicIoRequestArray *array = atomic_load_explicit(&request_pool->array, memory_order_relaxed);
    atomic_store_explicit(&request_pool->bottom, bottom, memory_order_relaxed);
    atomic_thread_fence(memory_order_seq_cst);
    uint64_t top = atomic_load_explicit(&request_pool->top, memory_order_relaxed);

    if(top <= bottom)
    {
        uint64_t size = atomic_load_explicit(&array->size, memory_order_relaxed);
        AtomicLoadIoRequest(out_request, &array->requests[bottom & (size - 1)]);
        if(top == bottom)
        {
            if(!atomic_compare_exchange_strong_explicit(&request_pool->top, &top, top + 1, memory_order_seq_cst, memory_order_relaxed))
            {
                out_request->op_code = IO_Null;
            }
            atomic_store_explicit(&request_pool->bottom, bottom + 1, memory_order_relaxed);
        }
    }
    else
    {
        out_request->op_code = IO_Null;
        atomic_store_explicit(&request_pool->bottom, bottom + 1, memory_order_relaxed);
    }
}

static void StealIoRequest(IoRequest *out_request, IoRequestPool *request_pool)
{
    uint64_t top = atomic_load_explicit(&request_pool->top, memory_order_acquire);
    atomic_thread_fence(memory_order_seq_cst);
    uint64_t bottom = atomic_load_explicit(&request_pool->bottom, memory_order_acquire);
    out_request->op_code = IO_Null;
    if(top < bottom)
    {
        AtomicIoRequestArray *array = atomic_load_explicit(&request_pool->array, memory_order_acquire);
        uint64_t size = atomic_load_explicit(&array->size, memory_order_relaxed);
        AtomicLoadIoRequest(out_request, &array->requests[top & (size - 1)]);
        if(!atomic_compare_exchange_strong_explicit(&request_pool->top, &top, top + 1, memory_order_seq_cst, memory_order_relaxed))
            out_request->op_code = IO_Null;
    }
}

static bool SubmitIoRequest(AsyncIoContext *io_context, IoContinuationList *io_continuation_list, IoRequest *request)
{
    Task t;
    t.capture.ptr = (void *)request->capture_ptr;
    t.capture.number = request->capture_number;
    t.fn = (TaskFn)request->on_finished;
    uint64_t id = PushIoContinuation(io_continuation_list, t);
    if(id == -1)
        FAIL(E_TooManyIOs);
    switch(request->op_code)
    {
    case IO_Read:
        return ReadFile_PlatformSpecific(
            io_context,
            request->fd,
            request->buffer,
            request->offset,
            request->num_bytes,
            id);
    case IO_Write:
        return WriteFile_PlatformSpecific(
            io_context,
            request->fd,
            request->buffer,
            request->offset,
            request->num_bytes,
            id);
    case IO_OpenReadClose:
        return OpenReadClose_PlatformSpecific(
            io_context,
            request->filename,
            request->buffer,
            request->offset,
            request->num_bytes,
            id);
    case IO_OpenWriteClose:
        return OpenWriteClose_PlatformSpecific(
            io_context,
            request->filename,
            request->buffer,
            request->offset,
            request->num_bytes,
            id);
    default:
        assert(false);
        return false;
    }
 }

bool OpenFile(FileHandle *out_file_handle, const char *path)
{
    return OpenFile_PlatformSpecific(out_file_handle, path);
}

bool CloseFile(FileHandle fd)
{
    return CloseFile_PlatformSpecific(fd);
}

bool GetFileSize(uint64_t *out_size, FileHandle fd)
{
    return GetFileSize_PlatformSpecific(out_size, fd);
}
