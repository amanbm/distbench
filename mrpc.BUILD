cc_library(
    name = "mrpc",
    hdrs = ["lib/mrpc.h"],
    visibility = ["//visibility:public"],
    deps = [":mrpc_static"],
    linkopts = ["-lm","-lpthread"],
)

cc_import(
    name = "mrpc_static",
    shared_library = "lib/libdistbench.so",
)
