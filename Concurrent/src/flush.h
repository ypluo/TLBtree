#ifndef __FLUSH_H__
#define __FLUSH_H__

#include <x86intrin.h>

#include "common.h"

static inline void mfence() {
    asm volatile("sfence" ::: "memory");
}

static inline void flush(void * ptr) {
#ifdef CLWB
    _mm_clwb(ptr);
#elif defined(CLFLUSHOPT)
    _mm_clflushopt(ptr);
#else
    _mm_clflush(ptr);
#endif
}

inline void clwb(void *data, int len) {
#ifdef DOFLUSH
    volatile char *ptr = (char *)((unsigned long long)data &~(CACHE_LINE_SIZE-1));
    for(; ptr < (char *)data + len; ptr += CACHE_LINE_SIZE) {
        flush((void *)ptr);
    }
#endif //DOFLUSH
}

inline void clflush(void *data, int len, bool fence=true)
{
#ifdef DOFLUSH
    volatile char *ptr = (char *)((unsigned long long)data &~(CACHE_LINE_SIZE-1));
    if(fence) mfence();
    for(; ptr < (char *)data + len; ptr += CACHE_LINE_SIZE){
        flush((void *)ptr);
    }
    if(fence) mfence();
#endif //DOFLUSH
}

template<typename T>
inline void persist_assign(T* addr, const T &v) { // To ensure atomicity, the size of T should be less equal than 8
    *addr = v;
    clwb(addr, sizeof(T));
}

#endif // __FLUSH_H__