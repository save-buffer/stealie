#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/sysinfo.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <linux/mman.h>
#include <linux/io_uring.h>
#include <stdatomic.h>

typedef void *ThreadReturnValue;
typedef ThreadReturnValue (*ThreadFn)(void *);
typedef int FileHandle;

static _Thread_local FileHandle FileTable[128];

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
    return sysconf(_SC_NPROCESSORS_ONLN);
}

static uint64_t GetTotalRam_PlatformSpecific()
{
    struct sysinfo info;
    sysinfo(&info);
    return info.totalram;
}

static uint64_t GetCurrentRamUsage_PlatformSpecific()
{
    struct rusage usage;
    int ret = getrusage(RUSAGE_SELF, &usage);
    assert(ret == 0);
    return usage.ru_maxrss;
}

static bool MapVirtualMemory_PlatformSpecific(void **out_ptr, uint64_t size)
{
    int flags = MAP_SHARED | MAP_ANONYMOUS;
    if(size >= 2 * 1024 * 1024)
        flags |= MAP_HUGE_2MB;

    *out_ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, flags, -1, 0);
    if(*out_ptr == MAP_FAILED)
        FAIL_PLATFORM_SPECIFIC(errno);
    return true;
}

static bool UnmapVirtualMemory_PlatformSpecific(void *ptr, uint64_t size)
{
    if(munmap(ptr, size) == -1)
        FAIL_PLATFORM_SPECIFIC(errno);
    return true;
}

typedef struct
{
    int ring_fd;
    uint32_t num_entries;
    uint32_t file_idx;
    
    struct
    {
        uint32_t *head;
        _Atomic uint32_t *tail;
        uint32_t *array;
        uint32_t mask;
        struct io_uring_sqe *sqes;
    } sqe;

    struct
    {
        _Atomic uint32_t *head;
        _Atomic uint32_t *tail;
        uint32_t mask;
        struct io_uring_cqe *cqes;
    } cqe;
} AsyncIoContext;

static int io_uring_setup(uint32_t entries, struct io_uring_params *params)
{
    return (int)syscall(SYS_io_uring_setup, entries, params);
}

static int io_uring_enter(int fd, uint32_t to_submit, uint32_t min_complete, uint32_t flags)
{
    return (int)syscall(SYS_io_uring_enter, fd, to_submit, min_complete, flags, NULL);
}

static int io_uring_register(int fd, uint32_t opcode, void *arg, uint32_t nargs)
{
    return (int)syscall(SYS_io_uring_register, fd, opcode, arg, nargs);
}

static bool Submit_io_uring(
    AsyncIoContext *ctx,
    uint8_t opcode,
    int fd,
    void *buffer,
    uint64_t offset,
    uint64_t num_bytes,
    uint64_t data)
{
    uint32_t head = *ctx->sqe.head & ctx->sqe.mask;
    uint32_t tail = *ctx->sqe.tail & ctx->sqe.mask;
    if((tail - head + 1) >= ctx->num_entries)
        FAIL(E_TooManyIOs);
    uint32_t idx = tail & ctx->sqe.mask;
    struct io_uring_sqe *sqe = &ctx->sqe.sqes[idx];
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = opcode;
    sqe->fd = fd;
    sqe->addr = (uintptr_t)buffer;
    sqe->len = (uint32_t)num_bytes;
    sqe->off = offset;
    sqe->user_data = data;

    ctx->sqe.array[idx] = idx;
    atomic_store_explicit(ctx->sqe.tail, tail + 1, memory_order_release);

    int ret = io_uring_enter(ctx->ring_fd, 1, 0, 0);
    if(ret < 0)
        FAIL_PLATFORM_SPECIFIC(ret);
    return true;
}

static bool SubmitOpenOpClose_io_uring(
    AsyncIoContext *ctx,
    uint8_t opcode,
    const char *filename,
    void *buffer,
    uint64_t offset,
    uint64_t num_bytes,
    uint64_t data)
{
#if 0
    uint32_t head = *ctx->sqe.head & ctx->sqe.mask;
    uint32_t tail = *ctx->sqe.tail & ctx->sqe.mask;
    if((tail - head + 3) >= ctx->num_entries)
        FAIL(E_TooManyIOs);
    uint32_t idx = tail & ctx->sqe.mask;
    struct io_uring_sqe *open = &ctx->sqe.sqes[idx];
    struct io_uring_sqe *op = &ctx->sqe.sqes[idx + 1];
    struct io_uring_sqe *close = &ctx->sqe.sqes[idx + 2];
    memset(ctx->sqe.sqes + idx, 0, sizeof(struct io_uring_sqe) * 3);

    open->opcode = IORING_OP_OPENAT;
    open->flags = IOSQE_IO_LINK;
    open->fd = AT_FDCWD;
    open->addr = (uintptr_t)filename;
    open->open_flags = O_DIRECT | O_RDWR | O_CREAT;
    open->len = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP;

    op->opcode = opcode;
    op->flags = IOSQE_IO_LINK;
    op->fd = AT_FDCWD;
    op->addr = (uintptr_t)filename;
    op->open_flags = O_DIRECT | O_RDWR | O_CREAT;
    op->len = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP;
#endif
    // TODO: Make it work when new kernel becomes more common
    return false;
}

static bool Poll_io_uring(uint64_t *out_user_data, AsyncIoContext *ctx)
{
    uint32_t head = atomic_load(ctx->cqe.head);
    uint32_t tail = atomic_load(ctx->cqe.tail);
   
    if(head == tail)
        return true;

    uint32_t idx = head & ctx->cqe.mask;
    struct io_uring_cqe *cqe = &ctx->cqe.cqes[idx];
    if(!atomic_compare_exchange_strong(ctx->cqe.head, &head, head + 1))
        return true;
    if(cqe->res < 0)
        FAIL_PLATFORM_SPECIFIC(cqe->res);
    *out_user_data = cqe->user_data;
    return true;
}

static bool InitAsyncIO_PlatformSpecific(AsyncIoContext *io_context, uint32_t log_num_concurrent_ios)
{
    struct io_uring_params params = {};

    uint32_t num_concurrent_ios = 1 << log_num_concurrent_ios;
    int fd = io_uring_setup(num_concurrent_ios, &params);
    if(fd < 0)
        FAIL_PLATFORM_SPECIFIC(fd);
    
    int submission_ring_size = params.sq_off.array + params.sq_entries * sizeof(uint32_t);
    int completion_ring_size = params.cq_off.cqes + params.cq_entries * sizeof(struct io_uring_cqe);

    bool single_mmap = params.features & IORING_FEAT_SINGLE_MMAP;
    
    if(single_mmap)
    {
        if(completion_ring_size > submission_ring_size)
            submission_ring_size = completion_ring_size;
        else
            completion_ring_size = submission_ring_size;
    }

    void *submission_ring = mmap(
        NULL,
        submission_ring_size,
        PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_POPULATE,
        fd,
        IORING_OFF_SQ_RING);

    if(submission_ring == MAP_FAILED)
        FAIL_PLATFORM_SPECIFIC(errno);

    void *completion_ring = single_mmap
        ? submission_ring
        : mmap(
            NULL,
            completion_ring_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            fd,
            IORING_OFF_CQ_RING);

    if(completion_ring == MAP_FAILED)
        FAIL_PLATFORM_SPECIFIC(errno);
    
    struct io_uring_sqe *sqes = mmap(
        NULL,
        params.sq_entries * sizeof(struct io_uring_sqe),
        PROT_READ | PROT_WRITE,
        MAP_SHARED | MAP_POPULATE,
        fd,
        IORING_OFF_SQES);

    if(sqes == MAP_FAILED)
        FAIL_PLATFORM_SPECIFIC(errno);

    io_context->ring_fd = fd;
    io_context->num_entries = num_concurrent_ios;
    io_context->sqe.head = submission_ring + params.sq_off.head;
    io_context->sqe.tail = submission_ring + params.sq_off.tail;
    io_context->sqe.mask = *(uint32_t *)(submission_ring + params.sq_off.ring_mask);
    io_context->sqe.array = submission_ring + params.sq_off.array;
    io_context->sqe.sqes = sqes;

    io_context->cqe.head = completion_ring + params.cq_off.head;
    io_context->cqe.tail = completion_ring + params.cq_off.tail;
    io_context->cqe.mask = *(uint32_t *)(completion_ring + params.cq_off.ring_mask);
    io_context->cqe.cqes = completion_ring + params.cq_off.cqes;

    io_context->file_idx = 0;
    uint32_t num_files = sizeof(FileTable) / sizeof(FileHandle);
    for(uint32_t i = 0; i < num_files; i++)
        FileTable[i] = -1;
    int ret = io_uring_register(io_context->ring_fd, IORING_REGISTER_FILES, &FileTable, num_files);
    if(ret < 0)
        FAIL_PLATFORM_SPECIFIC(ret);
    return true;
}

static bool OpenFile_PlatformSpecific(FileHandle *out_file_handle, const char *path)
{
    int flags = O_DIRECT | O_RDWR | O_CREAT;
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP;
    int fd = open(path, flags, mode);
    if(fd < 0)
    {
        // Sometimes O_DIRECT doesn't work, since it's a performance optimization
        // we can try again if it fails
        fd = open(path, flags & ~O_DIRECT, mode);
        if(fd < 0)
            FAIL_PLATFORM_SPECIFIC(errno);
    }
    *out_file_handle = fd;
    return true;
}

static bool CloseFile_PlatformSpecific(FileHandle handle)
{
    int ret = close(handle);
    if(ret == -1)
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
    return Submit_io_uring(io_context, IORING_OP_READ, fd, buffer, offset, num_bytes, user_data);
}

static bool WriteFile_PlatformSpecific(
    AsyncIoContext *io_context,
    FileHandle fd,
    void *buffer,
    uint64_t offset,
    uint64_t num_bytes,
    uint64_t user_data)
{
    return Submit_io_uring(io_context, IORING_OP_WRITE, fd, buffer, offset, num_bytes, user_data);
}

static bool OpenReadClose_PlatformSpecific(
    AsyncIoContext *io_context,
    const char *filename,
    void *buffer,
    uint64_t offset,
    uint64_t num_bytes,
    uint64_t user_data)
{
    return SubmitOpenOpClose_io_uring(io_context, IORING_OP_READ, filename, buffer, offset, num_bytes, user_data);
}

static bool OpenWriteClose_PlatformSpecific(
    AsyncIoContext *io_context,
    const char *filename,
    void *buffer,
    uint64_t offset,
    uint64_t num_bytes,
    uint64_t user_data)
{
    return SubmitOpenOpClose_io_uring(io_context, IORING_OP_WRITE, filename, buffer, offset, num_bytes, user_data);
}

static bool PollAsyncIo_PlatformSpecific(uint64_t *out_user_data, AsyncIoContext *io_context)
{
    *out_user_data = -1;
    return Poll_io_uring(out_user_data, io_context);
}

static uint32_t MaxSubmissions_PlatformSpecific(AsyncIoContext *io_context)
{
    return io_context->num_entries;
}

static uint32_t MaxCompletions_PlatformSpecific(AsyncIoContext *io_context)
{
    return io_context->num_entries * 2;
}

static uint32_t IoSubmissionsInFlight_PlatformSpecific(AsyncIoContext *io_context)
{
    uint32_t head = atomic_load(io_context->sqe.head) & io_context->sqe.mask;
    uint32_t tail = atomic_load(io_context->sqe.tail) & io_context->sqe.mask;
    uint32_t num_in_flight = (tail - head + MaxSubmissions_PlatformSpecific(io_context)) & io_context->sqe.mask;
    return num_in_flight;
}

static uint32_t IoCompletionsInFlight_PlatformSpecific(AsyncIoContext *io_context)
{
    uint32_t head = atomic_load(io_context->cqe.head) & io_context->cqe.mask;
    uint32_t tail = atomic_load(io_context->cqe.tail) & io_context->cqe.mask;
    uint32_t num_in_flight = (tail - head + MaxCompletions_PlatformSpecific(io_context)) & io_context->cqe.mask;
    return num_in_flight;
}

static void CleanupAsyncIO_PlatformSpecific()
{
    // Do nothing, rings will be freed on exit
}
