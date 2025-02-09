#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/sysctl.h>
#include <sys/event.h>
#include <mach/mach.h>
#include <mach/mach_vm.h>

#include <stdatomic.h>

typedef void *ThreadReturnValue;
typedef ThreadReturnValue (*ThreadFn)(void *);
typedef int FileHandle;

int sched_yield();
long sysconf(int name);

static bool CreateThread_PlatformSpecific(ThreadFn fn, void *arg)
{
    pthread_t thread;
    int ret = pthread_create(&thread, 0, fn, arg);
    if(ret != 0)
        FAIL_PLATFORM_SPECIFIC(ret);
    return true;
}

static void Yield_PlatformSpecific()
{
    sched_yield();
}

static uint32_t GetNumCores_PlatformSpecific()
{
    uint64_t result;
    size_t size = sizeof(result);
    if(sysctlbyname("machdep.cpu.thread_count", &result, &size, NULL, 0) == 01)
        return -1;
    return result;
}

static uint64_t GetTotalRam_PlatformSpecific()
{
    uint64_t result;
    size_t size = sizeof(result);
    if(sysctlbyname("hw.memsize", &result, &size, NULL, 0) == -1)
        return -1;
    return result;
}

static uint64_t GetCurrentRamUsage_PlatformSpecific()
{
    struct mach_task_basic_info info;
    mach_msg_type_number_t info_count = MACH_TASK_BASIC_INFO_COUNT;
    if(task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &info_count) != KERN_SUCCESS)
        return -1;
    return (uint64_t)info.resident_size;
}

static bool MapVirtualMemory_PlatformSpecific(void **out_ptr, uint64_t size)
{
    mach_vm_address_t addr;
    kern_return_t ret = mach_vm_allocate(mach_task_self(), &addr, size, VM_FLAGS_ANYWHERE);
    if(ret != KERN_SUCCESS)
        FAIL_PLATFORM_SPECIFIC(ret);
    *out_ptr = (void *)addr;
    return true;
}

static bool UnmapVirtualMemory_PlatformSpecific(void *ptr, uint64_t size)
{
    kern_return_t ret = mach_vm_deallocate(mach_task_self(), (mach_vm_address_t)ptr, size);
    if(ret != KERN_SUCCESS)
        FAIL_PLATFORM_SPECIFIC(ret);
    return true;
}

// PSIO = Platform-Specific Input-Output
typedef enum
{
    PSIO_Null = 0,
    PSIO_Read,
    PSIO_Write,
    PSIO_OpenReadClose,
    PSIO_OpenWriteClose,

    PSIO_NumCodes,
} PsioOpcode;

typedef union
{
    int fd;
    const char *filename;
} PsioFd;

typedef struct
{
    _Atomic PsioOpcode opcode;
    _Atomic PsioFd fd;
    _Atomic(void *) buffer;
    _Atomic uint64_t offset;
    _Atomic uint64_t num_bytes;
    _Atomic uint64_t data;
} PsioRequest;

typedef struct
{
    _Atomic uint64_t data;
    _Atomic int result;
} PsioResponse;

typedef struct
{
    _Atomic int32_t terminated; // 0 = don't terminate, 1 = terminate requested, 2 = terminated
    uint32_t num_entries;
    struct
    {
        _Atomic uint32_t head;
        _Atomic uint32_t tail;
        uint32_t mask;
        PsioRequest *sqes;
    } sqe;

    struct
    {
        _Atomic uint32_t head;
        _Atomic uint32_t tail;
        uint32_t mask;
        PsioResponse *cqes;
    } cqe;
} AsyncIoContext;

static int Psio_Null(PsioFd fd, void *buffer, uint64_t offset, uint64_t num_bytes)
{
    return 0;
}

static int Psio_Read(PsioFd fd, void *buffer, uint64_t offset, uint64_t num_bytes)
{
    return pread(fd.fd, buffer, num_bytes, offset);
}

static int Psio_Write(PsioFd fd, void *buffer, uint64_t offset, uint64_t num_bytes)
{
    return pwrite(fd.fd, buffer, num_bytes, offset);
}

static int Psio_Unimplemented(PsioFd fd, void *buffer, uint64_t offset, uint64_t num_bytes)
{
    return -1;
}

typedef int (*PsioExecuteFn)(PsioFd, void *, uint64_t, uint64_t);

PsioExecuteFn PsioOpcodeFns[] =
{
    Psio_Null,
    Psio_Read,
    Psio_Write,
    Psio_Unimplemented,
    Psio_Unimplemented,
};

_Static_assert(
    (sizeof(PsioOpcodeFns) / sizeof(PsioOpcodeFns[0])) == PSIO_NumCodes,
    "Not every opcode has an execution function!");

static void *WaitForIO(void *arg)
{
    AsyncIoContext *ctx = arg;
    for(uint32_t terminate = 0;
        terminate == 0;
        terminate = atomic_load_explicit(&ctx->terminated, memory_order_relaxed))
    {
        uint32_t tail = atomic_load_explicit(&ctx->sqe.tail, memory_order_relaxed);
        uint32_t head = atomic_load_explicit(&ctx->sqe.head, memory_order_acquire);
        if(tail <= head)
        {
            // TODO: Exponential backoff? 
            Yield_PlatformSpecific();
            continue;
        }

        uint32_t sqe_idx = head & ctx->sqe.mask;
        atomic_store_explicit(&ctx->sqe.head, head + 1, memory_order_release);
        PsioRequest *sqe = ctx->sqe.sqes + sqe_idx;
        PsioOpcode opcode = atomic_load_explicit(&sqe->opcode, memory_order_relaxed);
        PsioFd fd = atomic_load_explicit(&sqe->fd, memory_order_relaxed);
        void *buffer = atomic_load_explicit(&sqe->buffer, memory_order_relaxed);
        uint64_t offset = atomic_load_explicit(&sqe->offset, memory_order_relaxed);
        uint64_t num_bytes = atomic_load_explicit(&sqe->num_bytes, memory_order_relaxed);
        uint64_t data = atomic_load_explicit(&sqe->data, memory_order_relaxed);

        int ret = PsioOpcodeFns[opcode](fd, buffer, offset, num_bytes);

        uint32_t cqe_tail = atomic_load_explicit(&ctx->cqe.tail, memory_order_relaxed);
        uint32_t cqe_idx = cqe_tail & ctx->cqe.mask;
        PsioResponse *cqe = ctx->cqe.cqes;
        atomic_store_explicit(&cqe->data, data, memory_order_relaxed);
        atomic_store_explicit(&cqe->result, ret, memory_order_relaxed);

        atomic_store_explicit(&ctx->cqe.tail, cqe_tail + 1, memory_order_release);
    }
    atomic_store_explicit(&ctx->terminated, 2, memory_order_release);
    return NULL;
}

static bool Submit_psio(
    AsyncIoContext *ctx,
    PsioOpcode opcode,
    PsioFd fd,
    void *buffer,
    uint64_t offset,
    uint64_t num_bytes,
    uint64_t data)
{
    uint32_t tail = atomic_load_explicit(&ctx->sqe.tail, memory_order_relaxed);
    uint32_t head = atomic_load_explicit(&ctx->sqe.head, memory_order_acquire);
    if(tail + 1 == head)
        FAIL(E_TooManyIOs);
    uint32_t idx = tail & ctx->sqe.mask;

    PsioRequest *sqe = ctx->sqe.sqes + idx;
    atomic_store_explicit(&sqe->opcode, opcode, memory_order_relaxed);
    atomic_store_explicit(&sqe->fd, fd, memory_order_relaxed);
    atomic_store_explicit(&sqe->buffer, buffer, memory_order_relaxed);
    atomic_store_explicit(&sqe->offset, offset, memory_order_relaxed);
    atomic_store_explicit(&sqe->num_bytes, num_bytes, memory_order_relaxed);
    atomic_store_explicit(&sqe->data, data, memory_order_relaxed);

    atomic_store_explicit(&ctx->sqe.tail, tail + 1, memory_order_release);
    return true;
}

static bool Poll_psio(uint64_t *out_user_data, AsyncIoContext *ctx)
{
    uint32_t head = atomic_load_explicit(&ctx->cqe.head, memory_order_relaxed);
    uint32_t tail = atomic_load_explicit(&ctx->cqe.tail, memory_order_acquire);

    if(head == tail)
        return true;

    uint32_t idx = head & ctx->cqe.mask;
    PsioResponse *cqe = &ctx->cqe.cqes[idx];
    uint64_t data = atomic_load_explicit(&cqe->data, memory_order_relaxed);
    int res = atomic_load_explicit(&cqe->result, memory_order_relaxed);
    if(!atomic_compare_exchange_strong(&ctx->cqe.head, &head, head + 1))
        return true;
    if(cqe->result < 0)
        FAIL_PLATFORM_SPECIFIC(cqe->result);
    *out_user_data = cqe->data;
    return true;
}

static bool InitAsyncIO_PlatformSpecific(AsyncIoContext *io_context, uint32_t log_num_concurrent_ios)
{
    atomic_store_explicit(&io_context->terminated, 0, memory_order_relaxed);
    io_context->num_entries = 1 << log_num_concurrent_ios;

    uint32_t num_sqes = io_context->num_entries;
    uint32_t num_cqes = num_sqes * 2;
    uint64_t sqe_bytes = sizeof(PsioRequest) * num_sqes;
    uint64_t cqe_bytes = sizeof(PsioResponse) * num_cqes;

    void *rings;
    CHECK(MapVirtualMemory_PlatformSpecific(&rings, sqe_bytes + cqe_bytes));
    atomic_store_explicit(&io_context->sqe.head, 0, memory_order_relaxed);
    atomic_store_explicit(&io_context->sqe.tail, 0, memory_order_relaxed);
    io_context->sqe.mask = num_sqes - 1;
    io_context->sqe.sqes = rings;

    atomic_store_explicit(&io_context->cqe.head, 0, memory_order_relaxed);
    atomic_store_explicit(&io_context->cqe.tail, 0, memory_order_relaxed);
    io_context->cqe.mask = num_cqes - 1;
    io_context->cqe.cqes = rings + sqe_bytes;
    CHECK(CreateThread_PlatformSpecific(WaitForIO, io_context));
    return true;
}

static bool OpenFile_PlatformSpecific(FileHandle *out_file_handle, const char *path)
{
    int flags = O_RDWR | O_CREAT;
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP;
    int fd = open(path, flags, mode);
    if(fd < 0)
        FAIL_PLATFORM_SPECIFIC(errno);
    if(fcntl(fd, F_NOCACHE, 1) == -1)
        FAIL_PLATFORM_SPECIFIC(errno);
    *out_file_handle = fd;
    return true;
}

static bool CloseFile_PlatformSpecific(FileHandle handle)
{
    if(close(handle) < 0)
        FAIL_PLATFORM_SPECIFIC(errno);
    return true;
}

static bool GetFileSize_PlatformSpecific(uint64_t *out_size, FileHandle fd)
{
    struct stat st;
    int ret = fstat(fd, &st);
    if(ret == -1)
        FAIL_PLATFORM_SPECIFIC(errno);
    *out_size = st.st_size;
    return true;
}

static bool ReadFile_PlatformSpecific(
    AsyncIoContext *io_context,
    FileHandle fd,
    void *buffer,
    uint64_t offset,
    uint64_t num_bytes,
    uint64_t user_data)
{
    PsioFd psiofd = { .fd = fd };
    return Submit_psio(io_context, PSIO_Read, psiofd, buffer, offset, num_bytes, user_data);
}

static bool WriteFile_PlatformSpecific(
    AsyncIoContext *io_context,
    FileHandle fd,
    void *buffer,
    uint64_t offset,
    uint64_t num_bytes,
    uint64_t user_data)
{
    PsioFd psiofd = { .fd = fd };
    return Submit_psio(io_context, PSIO_Write, psiofd, buffer, offset, num_bytes, user_data);
}

static bool OpenReadClose_PlatformSpecific(
    AsyncIoContext *io_context,
    const char *filename,
    void *buffer,
    uint64_t offset,
    uint64_t num_bytes,
    uint64_t user_data)
{
    PsioFd psiofd = { .filename = filename };
    return Submit_psio(io_context, PSIO_OpenReadClose, psiofd, buffer, offset, num_bytes, user_data);
}

static bool OpenWriteClose_PlatformSpecific(
    AsyncIoContext *io_context,
    const char *filename,
    void *buffer,
    uint64_t offset,
    uint64_t num_bytes,
    uint64_t user_data)
{
    PsioFd psiofd = { .filename = filename };
    return Submit_psio(io_context, PSIO_OpenWriteClose, psiofd, buffer, offset, num_bytes, user_data);
}

static bool PollAsyncIo_PlatformSpecific(uint64_t *out_user_data, AsyncIoContext *io_context)
{
    *out_user_data = -1;
    return Poll_psio(out_user_data, io_context);
}

static uint32_t MaxSubmissions_PlatformSpecific(AsyncIoContext *io_context)
{
    return io_context->num_entries;
}

static uint32_t MaxCompletions_PlatformSpecific(AsyncIoContext *io_context)
{
    return 2 * io_context->num_entries;
}

static uint32_t IoSubmissionsInFlight_PlatformSpecific(AsyncIoContext *io_context)
{
    uint32_t head = atomic_load_explicit(&io_context->sqe.head, memory_order_acquire);
    uint32_t tail = atomic_load_explicit(&io_context->sqe.tail, memory_order_acquire);
    uint32_t num_in_flight = tail - head;
    return num_in_flight;
}

static uint32_t IoCompletionsInFlight_PlatformSpecific(AsyncIoContext *io_context)
{
    uint32_t head = atomic_load_explicit(&io_context->cqe.head, memory_order_acquire);
    uint32_t tail = atomic_load_explicit(&io_context->cqe.tail, memory_order_acquire);
    uint32_t num_completed = (tail - head) + 1; // There may be one running right now
    return num_completed;
}

static void CleanupAsyncIO_PlatformSpecific(AsyncIoContext *io_context)
{
    atomic_store_explicit(&io_context->terminated, 1, memory_order_release);
    while(atomic_load_explicit(&io_context->terminated, memory_order_acquire) != 2)
        Yield_PlatformSpecific();
}
