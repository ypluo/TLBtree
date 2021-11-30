project(TLBtree)

cmake_minimum_required(VERSION 3.16)

add_compile_options(-mclwb -fmax-errors=5 -fopenmp)
add_compile_options(-g)
link_libraries(/usr/lib/x86_64-linux-gnu/libpmemobj.so)
add_link_options(-pthread -fopenmp)

include_directories(include)

add_subdirectory(src)
add_subdirectory(test)