project(TLBtree)

cmake_minimum_required(VERSION 3.16)

add_compile_options(-mclwb -fmax-errors=5)
add_compile_options(-O3)
link_libraries(/usr/lib/x86_64-linux-gnu/libpmemobj.so)
add_link_options(-pthread)

include_directories(include)

add_subdirectory(src)
add_subdirectory(test)