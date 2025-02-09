# Stealie: A Parallel Runtime Based on Theft

I learned about work-stealing. Work-stealing lets idle threads steal work from each other to ensure overall throughput of the system. They achieve this using the classic Chase-Lev Deque. I decided to take this to the extreme and steal _everything_:
- Steal tasks
- Steal 2MB pages
- Steal IO completions
- Steal IO requests

The idea is to have a fully load-balancing dynamic system.

# Motivation
Most parallel runtimes have trouble expressing "open this file and then allocate some memory and then call this function". Each of these operations is arguably asynchronous, as opening and reading a file takes a long time, you might be out of memory and need to wait. 

# Usage
Include the runtime with `#include "runtime.h"`. Initialize it, and then start scheduling tasks!
Each consists of a function pointer and a 16-byte capture. The function pointer takes two arguments: uint32_t thread ID, and Capture (by value).
You can also open, read, write, and close files asynchronously, and schedule completions.

```
int main(void)
{
    InitRuntime();
    for(int i = 0; i < 10; i++)
    {
	ScheduleTask(my_task, (Capture){ .number = i });
    }
}
```

# Spec
I've attempted to verify Stealie by model checking it in TLA+. It seems to pass! 
