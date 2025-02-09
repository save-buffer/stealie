#pragma once
#include <stdbool.h>
#include <stdint.h>
#include <assert.h>
#include "platform_aliases.h"
#include "errors.h"

#define NULL 0
#define SlabSize (2 * 1024 * 1024)

typedef union
{
    uint8_t bytes[16];
    struct
    {
        void *ptr;
        uint64_t number;
    };
} Capture;

_Static_assert(sizeof(Capture) == 16, "Capture must be 8 bytes big!");

typedef bool (*TaskFn)(uint32_t, Capture);

bool InitRuntime();
bool ScheduleTask(TaskFn fn, Capture capture);
bool ExecuteUntilTrue(_Atomic bool *condition);

bool OpenFile(FileHandle *out_fd, const char *name);
bool ReadFile(FileHandle fd, void *buffer, uint64_t offset, uint64_t num_bytes, TaskFn on_finished, Capture capture);
bool WriteFile(FileHandle fd, void *buffer, uint64_t offset, uint64_t num_bytes, TaskFn on_finished, Capture capture);
bool CloseFile(FileHandle fd);
bool GetFileSize(uint64_t *out_size, FileHandle fd);

typedef struct
{
    void *ptr;
    uint64_t size;
    uint64_t offset;
} Slab;

typedef struct Arena
{
    struct Arena *prev;
    Slab slab;
} *Arena;

typedef struct
{
    Arena arena;
    uint64_t offset;
} ArenaMark;

void *AllocateFromSlab(Slab *slab, uint64_t size);
bool GetSlab(Slab *out_slab);
bool ReturnSlab(Slab slab);

bool AllocateFromArena(void **out_ptr, Arena *in_out_arena, uint64_t size);
bool GetArena(Arena *out_arena);
bool ReturnArena(Arena *out_arena);
ArenaMark SetArenaMark(Arena arena);
bool RestoreArenaMark(Arena *in_out_arena, ArenaMark mark);

#define ArenaAllocate(ptr, arena) AllocateFromArena((void **)&((ptr)), &(arena), sizeof(*(ptr)))
#define ArenaAllocateArray(ptr, n, arena) AllocateFromArena((void **)&((ptr)), &(arena), n * sizeof(*(ptr)))
#define ArenaAllocateFlexibleArray(ptr, n, arena, member_name) AllocateFromArena((void **)&((ptr)), &(arena), sizeof(*(ptr)) + n * sizeof((ptr)->member_name[0]))

static inline uint64_t GetSpaceLeftInSlab(Slab *slab)
{
    return slab->size - slab->offset;
}

static inline uint64_t GetSpaceLeftInArena(Arena arena)
{
    return GetSpaceLeftInSlab(&arena->slab);
}

uint32_t GetThreadIndex();
uint32_t GetNumThreads();
