#include "runtime.h"
#include "errors.c"
#include "util.c"

#if defined(__linux__)
#include "linux.c"
#elif defined(__APPLE__)
#include "macos.c"
#elif defined(_WIN32)
#include "windows.c"
#else
#error "Unsupported platform! Please complain to Sasha"
#endif

#include "buffers.c"
#include "work_stealing.c"
#include "files.c"

#include "thread_pool.c"

static bool InitThread(uint32_t id)
{
    MyError = &Team.errors[id];
    CHECK(InitThread_Buffers(&MyThread.buffer_pool, Team.total_memory / Team.num_threads));
    CHECK(InitThread_Files(&MyThread.io_request_pool, &MyThread.io_continuation_list, &MyThread.io_context, &MyThread.buffer_pool));
    CHECK(InitThread_Threadpool(id));
    return true;
}

bool InitRuntime()
{
    CHECK(InitThreadTeam());
    return true;
}
