#pragma once
#if defined(__linux__)
typedef int FileHandle;
#elif defined(__APPLE__)
typedef int FileHandle;
#elif defined(_WIN32)
typedef void *FileHandle;
#else
#error "Unsupported platform! Please complain to Sasha"
#endif
