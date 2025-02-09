#include "runtime.c"
#include <stdio.h>

#define NumTasks 16384

int64_t idxs[NumTasks];
_Atomic int tasks_done = 0;
_Atomic bool done = false;
bool WriteIdx(uint32_t idx, Capture c)
{
    idxs[c.number] = idx;
    int num_done = atomic_fetch_add(&tasks_done, 1);
    if(num_done >= NumTasks - 1)
        atomic_store(&done, true);
    usleep(0);
    return true;
}

int main()
{
    InitRuntime();
    for(int i = 0; i < NumTasks; i++)
    {
        Capture c;
        c.number = i;
        if(!ScheduleTask(WriteIdx, c))
            WriteIdx(0, c);
    }
    ExecuteUntilTrue(&done);

    uint32_t num_threads = GetNumThreads();
    int histogram[num_threads];
    memset(histogram, 0, sizeof(histogram));
    for(int i = 0; i < NumTasks; i++)
        histogram[idxs[i]]++;
    for(int i = 0; i < num_threads; i++)
        printf("Thread %d did %d tasks\n", i, histogram[i]);
    return 0;
}
