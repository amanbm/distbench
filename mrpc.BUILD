cc_library(
    name = "mrpc",
    hdrs = glob(["**/*.h", "**/*.tcc"]),
    strip_include_prefix = "include/",
    includes = ["include/"],
    visibility = ["//visibility:public"],
    deps = [":mrpc_static"],
    linkopts = ["-lm","-lpthread"],
)

cc_import(
    name = "mrpc_static",
    static_library = "lib/libdistbench.so",
)