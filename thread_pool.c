#include <stdint.h>
#include <stdatomic.h>

typedef enum
{
    Working,
    Stealing,
} ThreadState;

typedef struct
{
    uint32_t id;
    ThreadState state;
    Rng64 rng;
    WorkStealingDeque task_pool;
    BufferPool buffer_pool;
    IoRequestPool io_request_pool;
    AsyncIoContext io_context;
    IoContinuationList io_continuation_list;
    _Atomic bool terminate;
} Thread;
static _Thread_local Thread MyThread;

typedef struct
{
    _Atomic int32_t threads_created;
    _Atomic int32_t threads_destroyed;
    Slab slab;
    uint64_t total_memory;
    uint32_t num_threads;
    Thread **threads;
    Error *errors;
} ThreadTeam;

static ThreadTeam Team;

static void WaitForCreation()
{
    while(atomic_load_explicit(&Team.threads_created, memory_order_relaxed) < Team.num_threads)
        Yield_PlatformSpecific();
}

static void WaitForTermination()
{
    while(atomic_load_explicit(&Team.threads_destroyed, memory_order_relaxed) < Team.num_threads)
        Yield_PlatformSpecific();
}

static bool InitThread_Threadpool(uint32_t id)
{
    InitDeque(&MyThread.task_pool, &MyThread.buffer_pool);
    MyThread.id = id;
    MyThread.state = Stealing;
    atomic_store_explicit(&MyThread.terminate, false, memory_order_relaxed);

    Team.threads[id] = &MyThread;
    atomic_fetch_add_explicit(&Team.threads_created, 1, memory_order_relaxed);
    if(id != 0)
        WaitForCreation();
    return true;
}

static bool InitThread(uint32_t id);

static void *TryStealBuffer()
{
    int32_t num_other_threads = (int32_t)(Team.num_threads - 1);
    uint32_t permutation[num_other_threads];
    for(int32_t i = 0; i < MyThread.id && i < num_other_threads; i++)
        permutation[i] = i;
    for(int32_t i = MyThread.id + 1; i <= num_other_threads; i++)
        permutation[i - 1] = i;

    for(int32_t i = 0; i < num_other_threads - 2; i++)
    {
        uint32_t remaining = num_other_threads - i - 1;
        uint32_t j = (uint32_t)(RngNext(&MyThread.rng) % remaining) + i;
        uint32_t tmp = permutation[i];
        permutation[i] = permutation[j];
        permutation[j] = tmp;
    }

    const int32_t num_tries = 2;
    for(int i = 0; i < num_other_threads; i++)
    {
        uint32_t victim = permutation[i];
        BufferPool *pool = &Team.threads[victim]->buffer_pool;
        void *buffer = StealBuffer(pool);
        if(buffer)
            return buffer;
    }
    return NULL;
}

static bool PollTaskQueue(uint32_t id)
{
    if(MyThread.state == Working)
    {
        Task t = PopTask(&MyThread.task_pool);
        if(!t.fn)
            MyThread.state = Stealing;
        else
            return t.fn(id, t.capture);
    }
    else
    {
        uint32_t victim = (uint32_t)(RngNext(&MyThread.rng) % Team.num_threads);
        Task t = StealTask(&Team.threads[victim]->task_pool);
        if(t.fn)
            return t.fn(id, t.capture);
    }
    return true;
}

static bool PollAsyncIoSubmissions(bool *out_did_something, uint32_t id, uint32_t victim)
{
    uint32_t submitted = IoSubmissionsInFlight_PlatformSpecific(&MyThread.io_context);
    uint32_t completed = IoCompletionsInFlight_PlatformSpecific(&MyThread.io_context);
    uint32_t max_submitted = MaxSubmissions_PlatformSpecific(&MyThread.io_context);
    uint32_t max_completed = MaxCompletions_PlatformSpecific(&MyThread.io_context);

    if(submitted + completed >= max_completed)
        return true;
    if(submitted >= max_submitted)
        return true;

    uint32_t can_submit = max_submitted - submitted;
    for(uint32_t i = 0; i < can_submit; i++)
    {
        IoRequest request;
        if(id == victim)
            PopIoRequest(&request, &MyThread.io_request_pool);
        else
            StealIoRequest(&request, &Team.threads[victim]->io_request_pool);
        if(request.op_code == IO_Null)
            break;
        CHECK(SubmitIoRequest(&MyThread.io_context, &MyThread.io_continuation_list, &request));
    }
    return true;
}

static bool PollAsyncIoCompletions(bool *out_did_something, uint32_t id, uint32_t victim)
{
    uint64_t idx;
    CHECK(PollAsyncIo_PlatformSpecific(&idx, &Team.threads[victim]->io_context));
    if(idx == -1)
        return true;
    *out_did_something = true;
    Task t = (id == victim) ? FreeContinuation(&MyThread.io_continuation_list, idx)
        : FreeContinuation_Thief(&Team.threads[victim]->io_continuation_list, idx);
    return t.fn(id, t.capture);
}

static bool PollAsyncIo(uint32_t id)
{
    {
        bool did_something = false;
        CHECK(PollAsyncIoSubmissions(&did_something, id, id));
        if(!did_something)
        {
            uint32_t victim = (uint32_t)(RngNext(&MyThread.rng) % Team.num_threads);
            CHECK(PollAsyncIoSubmissions(&did_something, id, victim));
        }
    }
    {
        bool did_something = false;
        CHECK(PollAsyncIoCompletions(&did_something, id, id));
        if(!did_something)
        {
            uint32_t victim = (uint32_t)(RngNext(&MyThread.rng) % Team.num_threads);
            CHECK(PollAsyncIoCompletions(&did_something, id, victim));
        }
    }
    return true;
}

static void TerminateOthers()
{
    for(uint32_t i = 0; i < Team.num_threads; i++)
        atomic_store_explicit(&Team.threads[i]->terminate, true, memory_order_seq_cst);
}

static void CleanupThread()
{
    CleanupAsyncIO_PlatformSpecific(&MyThread.io_context);
    atomic_fetch_add_explicit(&Team.threads_destroyed, 1, memory_order_relaxed);
}

static void StopThreadTeam()
{
    atomic_fetch_add_explicit(&Team.threads_destroyed, 1, memory_order_relaxed);
    TerminateOthers();
    WaitForTermination();
}

static ThreadReturnValue WaitForWork(void *arg)
{
    uint32_t id = (uint32_t)(uintptr_t)arg;
    if(!InitThread(id))
    {
        StopThreadTeam();
        CleanupThread();
        return 0;
    }

    WaitForCreation();
    for(bool poll_io = false;
        !atomic_load_explicit(&MyThread.terminate, memory_order_relaxed);
        poll_io = !poll_io)
    {
        atomic_thread_fence(memory_order_seq_cst);
        bool success = poll_io ? PollAsyncIo(id) : PollTaskQueue(id);
        if(!success)
        {
            TerminateOthers();
            break;
        }
    }
    CleanupThread();
    WaitForTermination();
    return 0;
}

static bool CreateThread(uint32_t id)
{
    if(id == 0)
    {
        CHECK(InitThread(id));
    }
    else
    {
        CHECK(CreateThread_PlatformSpecific(WaitForWork, (void *)(intptr_t)id));
    }
    return true;
}

static bool InitThreadTeam()
{
    Team.threads_created = 0;
    CHECK(MapVirtualMemory_PlatformSpecific(&Team.slab.ptr, 4096));
    Team.slab.size = 4096;
    Team.slab.offset = 0;
    uint64_t current_usage = GetCurrentRamUsage_PlatformSpecific();
    Team.total_memory = (GetTotalRam_PlatformSpecific() / 2) - current_usage;
    Team.num_threads = GetNumCores_PlatformSpecific();
    Team.threads = AllocateFromSlab(&Team.slab, sizeof(Thread *) * Team.num_threads);
    Team.errors = AllocateFromSlab(&Team.slab, sizeof(Error) * Team.num_threads);
    for(int i = 0; i < Team.num_threads; i++)
        CreateThread(i);
    WaitForCreation();
    return true;
}

bool ExecuteUntilTrue(_Atomic bool *condition)
{
    assert(MyThread.id == 0);
    const uint32_t id = 0;
    for(bool poll_io = false;
        !atomic_load_explicit(condition, memory_order_relaxed);
        poll_io = !poll_io)
    {
        atomic_thread_fence(memory_order_seq_cst);
        bool success = poll_io ? PollAsyncIo(id) : PollTaskQueue(id);
        if(!success)
        {
            StopThreadTeam();
            return false;
        }
        if(atomic_load_explicit(&MyThread.terminate, memory_order_relaxed))
        {
            StopThreadTeam();
            for(uint32_t i = 0; i < Team.num_threads; i++)
            {
                if(Team.errors[i].type != E_None)
                {
                    Team.errors[0] = Team.errors[i];
                    return false;
                }
            }
            return true;
        }
    }
    return true;
}

static bool ScheduleIoOperation(IoRequest *request)
{
    uint32_t submissions = IoSubmissionsInFlight_PlatformSpecific(&MyThread.io_context);
    uint32_t completions = IoCompletionsInFlight_PlatformSpecific(&MyThread.io_context);
    uint32_t max_num_submissions = MaxSubmissions_PlatformSpecific(&MyThread.io_context);
    uint32_t max_num_completions = MaxCompletions_PlatformSpecific(&MyThread.io_context);

    bool can_submit = submissions < max_num_submissions
        && (submissions + completions) < max_num_completions;

    if(can_submit)
        return SubmitIoRequest(&MyThread.io_context, &MyThread.io_continuation_list, request);
    else
        return PushIoRequest(&MyThread.io_request_pool, request, &MyThread.buffer_pool);
}

bool ScheduleTask(TaskFn fn, Capture capture)
{
    Task t;
    t.capture = capture;
    t.fn = fn;
    MyThread.state = Working;
    return PushTask(&MyThread.task_pool, &MyThread.buffer_pool, t);
}

bool ReadFile(FileHandle fd, void *buffer, uint64_t offset, uint64_t num_bytes, TaskFn on_finished, Capture capture)
{
    IoRequest request;
    request.op_code = IO_Read;
    request.fd = fd;
    request.buffer = buffer;
    request.offset = offset;
    request.num_bytes = num_bytes;
    request.on_finished = (uintptr_t)on_finished;
    request.capture_ptr = (uintptr_t)capture.ptr;
    request.capture_number = capture.number;
    return ScheduleIoOperation(&request);
}

bool WriteFile(FileHandle fd, void *buffer, uint64_t offset, uint64_t num_bytes, TaskFn on_finished, Capture capture)
{
    IoRequest request;
    request.op_code = IO_Write;
    request.fd = fd;
    request.buffer = buffer;
    request.offset = offset;
    request.num_bytes = num_bytes;
    request.on_finished = (uintptr_t)on_finished;
    request.capture_ptr = (uintptr_t)capture.ptr;
    request.capture_number = capture.number;
    return ScheduleIoOperation(&request);
}

bool GetSlab(Slab *out_slab)
{
    void *buffer = GetBuffer(&MyThread.buffer_pool);
    if(!buffer)
    {
        buffer = TryStealBuffer();
        if(!buffer)
            return AllocateSlab(out_slab, &MyThread.buffer_pool);
    }
    out_slab->ptr = buffer;
    out_slab->size = SlabSize;
    out_slab->offset = 0;
    return true;
}

bool ReturnSlab(Slab slab)
{
    return ReturnBuffer(slab.ptr, &MyThread.buffer_pool);
}

bool ReturnArena(Arena *in_out_arena)
{
    ArenaMark mark = { NULL, 0 };
    return RestoreArenaMark(in_out_arena, mark);
}

uint32_t GetThreadIndex()
{
    return MyThread.id;
}

uint32_t GetNumThreads()
{
    return Team.num_threads;
}
