#include "runtime.c"

#include <time.h>
#include <stdio.h>

typedef struct
{
    _Alignas(128) _Atomic uint64_t num_started;
    _Alignas(128) _Atomic uint64_t num_finished;
    _Alignas(128) _Atomic bool cond;
} EmptyFunctionCounter;

bool EmptyFunction(uint32_t id, Capture capture)
{
    EmptyFunctionCounter *ctr = capture.ptr;
    uint64_t num_done = atomic_fetch_add_explicit(&ctr->num_finished, 1, memory_order_relaxed);
    if(num_done >= 1024 * 1024 - 1)
        atomic_store(&ctr->cond, true);
    return true;
}

void MillionTinyTasks_SingleProducer()
{
    EmptyFunctionCounter counter;
    atomic_store(&counter.num_started, 0);
    atomic_store(&counter.num_finished, 0);
    atomic_store(&counter.cond, false);
    Capture c;
    c.ptr = &counter;
    for(uint64_t i = 0; i < 1024 * 1024; i++)
        ASSERT_NOT_FAIL(ScheduleTask(EmptyFunction, c));
    ASSERT_NOT_FAIL(ExecuteUntilTrue(&counter.cond));
}

bool DoublingFunction(uint32_t id, Capture capture)
{
    EmptyFunctionCounter *ctr = capture.ptr;
    uint64_t number = capture.number;
    if(number == 19)
    {
        uint64_t num_done = atomic_fetch_add_explicit(&ctr->num_finished, 1, memory_order_relaxed);
        if(num_done >= 1024 * 1024 - 1)
            atomic_store(&ctr->cond, true);
        return true;
    }

    capture.number += 1;
    CHECK(ScheduleTask(DoublingFunction, capture));
    CHECK(ScheduleTask(DoublingFunction, capture));
    return true;
}

void MillionTinyTasks_MultiProducer()
{
    EmptyFunctionCounter counter;
    atomic_store(&counter.num_started, 0);
    atomic_store(&counter.num_finished, 0);
    atomic_store(&counter.cond, false);
    Capture c = { .ptr = &counter, .number = 0 };
    ASSERT_NOT_FAIL(ScheduleTask(DoublingFunction, c));
    ASSERT_NOT_FAIL(ScheduleTask(DoublingFunction, c));
    ASSERT_NOT_FAIL(ExecuteUntilTrue(&counter.cond));
}
typedef void (*BenchmarkFn)();
typedef struct
{
    const char *name;
    BenchmarkFn fn;
    uint64_t divisor;
} Benchmark;

void DoBenchmark(Benchmark *b)
{
    const int32_t NumRepeats = 10;
    double start = ((double)clock()) / CLOCKS_PER_SEC;
    for(int32_t i = 0; i < NumRepeats; i++)
        b->fn();
    double end = ((double)clock()) / CLOCKS_PER_SEC;

    const double Microseconds = 1.0e6;
    double avg_time = ((end - start) / NumRepeats * Microseconds) / b->divisor;
    printf("%s: %.4f usec/op\n", b->name, avg_time);
}

int main(void)
{
    InitRuntime();
    Benchmark bms[] =
    {
        { "MillionTinyTasks_SingleProducer", MillionTinyTasks_SingleProducer, 1024 * 1024 },
        { "MillionTinyTasks_MultiProducer", MillionTinyTasks_MultiProducer, 1024 * 1024 },
    };
    for(int i = 0; i < sizeof(bms) / sizeof(bms[0]); i++)
        DoBenchmark(&bms[i]);
    return 0;
}
