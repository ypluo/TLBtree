/*
    Copyright (c) Luo Yongping. THIS SOFTWARE COMES WITH NO WARRANTIES, 
    USE AT YOUR OWN RISK!
*/
#ifndef __COMMON_H__
#define __COMMON_H__

#include <sys/stat.h>

#define LOADSCALE 8
#define DOFLUSH

#define KILO 1024
#define MILLION (KILO * KILO)
#define CACHE_LINE_SIZE 64

using _key_t = int64_t;
using _value_t = void *;

struct Record {
    _key_t key;
    char * val; 
    Record(_key_t k=INT64_MAX, char * v=NULL) : key(k), val(v) {}
    bool operator < (const Record & other) {
        return key < other.key;
    }
};

enum OperationType {READ = 0, INSERT, UPDATE, DELETE};

struct res_t { // a result type use to pass info when split and search
    bool flag; 
    Record rec;
    res_t(bool f, Record e):flag(f), rec(e) {}
};

#include <sys/time.h>
inline double seconds()
{
  timeval now;
  gettimeofday(&now, NULL);
  return now.tv_sec + now.tv_usec/1000000.0;
}

inline int getRandom() {
    timeval now;
    gettimeofday(&now, NULL);
    return now.tv_usec;
}

inline bool file_exist(const char *pool_path) {
    struct stat buffer;
    return (stat(pool_path, &buffer) == 0);
}

#endif //__COMMON_H__