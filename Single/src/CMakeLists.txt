add_library(tlbtree "tlbtree_impl.cc")

add_library(wotree "wotree256.cc")

set ( TLBtree 
        tlbtree
        wotree
    CACHE STRINGS "TLBTREE" FORCE
)