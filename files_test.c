#include "runtime.c"
#include <stdio.h>
#include <string.h>

_Atomic bool file_written = false;
_Atomic bool file_read = false;
uint32_t read_thread = 0;
uint32_t write_thread = 0;

typedef struct
{
    FileHandle handle;
    uint8_t *to_write;
    uint8_t *to_read;
    uint64_t size;
} ReadWriteInfo;

bool OnRead(uint32_t id, Capture capture)
{
    ReadWriteInfo *info = capture.ptr;
    read_thread = id;
    assert(!atomic_load(&file_read));
    assert(memcmp(info->to_write, info->to_read, info->size) == 0);
    atomic_store(&file_read, true);
    return true;
}

bool OnWritten(uint32_t id, Capture capture)
{
    bool old = atomic_exchange(&file_written, true);
    assert(!old);
    write_thread = id;
    ReadWriteInfo *info = capture.ptr;
    CHECK(ReadFile(info->handle, info->to_read, 0, info->size, OnRead, capture));
    usleep(0);
    return true;
}

int main()
{
    InitRuntime();
    ReadWriteInfo info;
    OpenFile_PlatformSpecific(&info.handle, "test");
    Slab s;
    ASSERT_NOT_FAIL(GetSlab(&s));
    info.size = 1024;
    info.to_write = AllocateFromSlab(&s, 1024);
    info.to_read = AllocateFromSlab(&s, 1024);
    for(int i = 0; i < info.size; i++)
        info.to_write[i] = '0' + (i % 10);
    Capture capture;
    capture.ptr = &info;
    WriteFile(info.handle, info.to_write, 0, info.size, OnWritten, capture);
    while(!atomic_load(&file_read))
        Yield_PlatformSpecific();

    printf("Written by thread %d, Read by thread %d\n", write_thread, read_thread);
    return 0;
}
