#pragma once

#if defined(__linux__)
#include <errno.h>
#elif defined(__APPLE__)
#include <errno.h>
#else
#error "Unsupported Platform"
#endif

typedef enum
{
    E_None = 0,
    E_OutOfMemory,
    E_TooManyIOs,
    E_PlatformSpecific,
    E_Invalid,
    E_NotImplemented,
} ErrorType;

static const char *ErrorStrings[] =
{
    "E_None",
    "E_OutOfMemory",
    "E_TooManyIOs",
    "E_PlatformSpecific",
    "E_Invalid",
    "E_NotImplemented"
};

typedef struct
{
    ErrorType type;
    const char *message;
    const char *filename;
    uint64_t line_number;
    int error_code_platform_specific;
} Error;

extern _Thread_local Error *MyError;

#define CHECK(x) if(!(x)) { return false; }
#define FAIL_IMPL(tp) do { MyError->type = tp; MyError->filename = __FILE__; MyError->line_number = __LINE__; } while(0);
#define FAIL(tp) do { FAIL_IMPL(tp); return false; } while(0);
#define FAIL_MSG(tp, msg) do { FAIL_IMPL(tp); MyError->message = msg; return false; } while(0);
#define FAIL_PLATFORM_SPECIFIC(e) do { FAIL_IMPL(E_PlatformSpecific); MyError->error_code_platform_specific = e; return false; } while(0);
#define ASSERT_NOT_FAIL(x) assert((x))
#define CLEAR_ERROR() do { MyError->type = E_NONE; MyError->message = NULL; MyError->filename = NULL, MyError->linenumber = 0; MyError.error_code_platform_specific = 0; } while(0);
#define ERROR_FORMAT "%s: %s (%s:%llu)", ErrorStrings[MyError->type], MyError->message, MyError->filename, ((long long unsigned)MyError->line_number)

