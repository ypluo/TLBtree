add_executable(datagen "datagen.cc")

add_executable(main "main.cc")
target_link_libraries(main tlbtree)

add_executable(preload "preload.cc")
target_link_libraries(preload tlbtree)