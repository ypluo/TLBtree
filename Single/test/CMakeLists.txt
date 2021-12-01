add_executable(datagen "datagen.cc")

add_executable(main "main.cc")
target_link_libraries(main ${TLBtree})

add_executable(preload "preload.cc")
target_link_libraries(preload ${TLBtree})