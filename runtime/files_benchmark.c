#include "runtime.c"

#include <time.h>
#include <stdio.h>

#if defined(__APPLE__)
#define NumFiles 128
#else
#define NumFiles 512
#endif
#define FileSize 128
#define SliceSize 128

_Atomic uint64_t started_write = 0;
_Atomic uint64_t written = 0;
_Atomic uint64_t started_read = 0;
_Atomic uint64_t finished_read = 0;
_Atomic bool done = false;
FileHandle fds[NumFiles];

bool OnWritten(uint32_t id, Capture c)
{
    if(atomic_fetch_add(&written, 1) == NumFiles - 1)
        atomic_store(&done, true);
    return true;
}

bool CreateAndWriteTinyFiles(uint32_t id, Capture c)
{
    for(uint64_t i = 0; i < SliceSize; i++)
    {
        uint64_t idx = atomic_fetch_add(&started_write, 1);
        if(idx >= NumFiles)
            return true;

        assert(0 <= idx && idx < NumFiles);
        c.number = idx;
        CHECK(WriteFile(fds[idx], c.ptr, 0, FileSize, OnWritten, c));
    }
    return true;
}

bool Write512TinyFiles()
{
    atomic_store(&done, false);
    atomic_store(&started_write, 0);
    atomic_store(&written, 0);
    Slab s;
    ASSERT_NOT_FAIL(GetSlab(&s));
    uint8_t *contents = AllocateFromSlab(&s, FileSize);
    for(int i = 0; i < FileSize; i++)
        contents[i] = i;
    Capture cap;
    cap.ptr = contents;
    for(int i = 0; i < ((NumFiles + SliceSize - 1) / SliceSize); i++)
        CHECK(ScheduleTask(CreateAndWriteTinyFiles, cap));
    CHECK(ExecuteUntilTrue(&done));
    return true;
}

typedef struct
{
    void *read_buffer;
    _Atomic uint64_t offset;
} ReadInfo;

bool OnRead(uint32_t id, Capture c)
{
    if(atomic_fetch_add(&finished_read, 1) == NumFiles - 1)
        atomic_store(&done, true);
    return true;
}

bool ReadTinyFiles(uint32_t id, Capture c)
{
    ReadInfo *info = c.ptr;
    for(uint64_t i = 0; i <= SliceSize; i++)
    {
        uint64_t idx = atomic_fetch_add(&started_read, 1);
        if(idx >= NumFiles)
            return true;

        uint64_t offset = atomic_fetch_add(&info->offset, FileSize);
        CHECK(ReadFile(fds[idx], info->read_buffer + offset, 0, FileSize, OnRead, c));
    }
    return true;
}

bool Read512TinyFiles()
{
    atomic_store(&done, false);
    atomic_store(&started_read, 0);
    atomic_store(&finished_read, 0);
    Slab s;
    ASSERT_NOT_FAIL(GetSlab(&s));
    ReadInfo info;
    info.read_buffer = AllocateFromSlab(&s, FileSize * NumFiles);
    atomic_store(&info.offset, 0);
    Capture c;
    c.ptr = &info;
    for(int i = 0; i < ((NumFiles + SliceSize - 1) / SliceSize); i++)
        CHECK(ScheduleTask(ReadTinyFiles, c));
    CHECK(ExecuteUntilTrue(&done));
    return true;
}

typedef bool (*BenchmarkFn)();
typedef struct
{
    const char *name;
    BenchmarkFn fn;
    uint64_t divisor;
} Benchmark;

bool DoBenchmark(Benchmark *b)
{
    const int32_t NumRepeats = 10;
    double start = ((double)clock()) / CLOCKS_PER_SEC;
    for(int32_t i = 0; i < NumRepeats; i++)
        CHECK(b->fn());
    double end = ((double)clock()) / CLOCKS_PER_SEC;

    const double Microseconds = 1.0e6;
    double avg_time = ((end - start) / NumRepeats * Microseconds) / b->divisor;
    printf("%s: %.4f usec/op\n", b->name, avg_time);
    return true;
}

int main(void)
{
    ASSERT_NOT_FAIL(InitRuntime());
    for(int i = 0; i < NumFiles; i++)
    {
        char name[] = "testXXXX.bork";
        name[4] = '0' + (i / 1000);
        name[5] = '0' + ((i  / 100) % 10);
        name[6] = '0' + ((i / 10) % 10);
        name[7] = '0' + (i % 10);
        ASSERT_NOT_FAIL(OpenFile_PlatformSpecific(&fds[i], name));
    }
    Benchmark bms[] =
    {
        { "Write512TinyFiles", Write512TinyFiles, NumFiles },
        { "Read512TinyFiles", Read512TinyFiles, NumFiles },
    };
    for(int i = 0; i < sizeof(bms) / sizeof(bms[0]); i++)
    {
        if(!DoBenchmark(&bms[i]))
        {
            printf(ERROR_FORMAT);
            return 1;
        }
    }
    return 0;
}

