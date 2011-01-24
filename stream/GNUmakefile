TOPDIR	?= ..
include Makefile.common

TESTS-yes			+= $(TESTS_NON_MSVC)

include $(TOPDIR)/Makefile.subdir.gnu


test_stream1.$o:		CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=0
test_sorted_runs.$o:		CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=0
test_push_sort.$o:		CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=0
test_stream.$o:			CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=0
test_asynchronous_nodes.$o:	CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=0
test_asynchronous_stream_sort.$o:	CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=0
test_asynchronous_stream_sort.as.$o:	CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=0
test_asynchronous_stream_sort.as-ba.$o:	CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=0
test_asynchronous_distribute_gather.$o:	CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=0
test_materialize.$o:		CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=0 -DSTXXL_VERBOSE_MATERIALIZE=STXXL_VERBOSE0

$(eval $(call COMPILE_VARIANT,as,-DSTXXL_STREAM_SORT_ASYNCHRONOUS_PULL=1))
$(eval $(call COMPILE_VARIANT,as-ba,-DSTXXL_STREAM_SORT_ASYNCHRONOUS_PULL=1 -DBATCHED=1))

ifeq ($(strip $(USE_PMODE)),yes)
# ICPC 12.0: internal error: backend signals (issue 613286)
$(call reduce_optimization,3,0,test_stream,Version_12.0,any,any)
$(call reduce_optimization,3,0,test_stream1,Version_12.0,any,any)
$(call reduce_optimization,3,0,test_sorted_runs,Version_12.0,any,any)
$(call reduce_optimization,3,0,test_push_sort,Version_12.0,any,any)
$(call reduce_optimization,3,0,test_transpose,Version_12.0,any,any)
$(call reduce_optimization,3,0,test_asynchronous_nodes,Version_12.0,any,any)
$(call reduce_optimization,3,0,test_asynchronous_stream_sort,Version_12.0,any,any)
$(call reduce_optimization,3,0,test_asynchronous_stream_sort.as,Version_12.0,any,any)
$(call reduce_optimization,3,0,test_asynchronous_stream_sort.as-ba,Version_12.0,any,any)
endif

ifeq ($(strip $(USE_MCSTL)),yes)
# ICPC 12.0: internal error: backend signals (issue 613286)
$(call reduce_optimization,3,0,test_stream,Version_12.0,any,any)
$(call reduce_optimization,3,0,test_stream1,Version_12.0,any,any)
$(call reduce_optimization,3,0,test_sorted_runs,Version_12.0,any,any)
$(call reduce_optimization,3,0,test_push_sort,Version_12.0,any,any)
$(call reduce_optimization,3,0,test_transpose,Version_12.0,any,any)
$(call reduce_optimization,3,0,test_asynchronous_nodes,Version_12.0,any,any)
$(call reduce_optimization,3,0,test_asynchronous_stream_sort,Version_12.0,any,any)
$(call reduce_optimization,3,0,test_asynchronous_stream_sort.as,Version_12.0,any,any)
$(call reduce_optimization,3,0,test_asynchronous_stream_sort.as-ba,Version_12.0,any,any)

# ICPC 10.0: internal error: backend signals (issue 466173)
$(call reduce_optimization,3,1,test_stream,Version_10.0,32-bit,any)
$(call reduce_optimization,3,1,test_stream1,Version_10.0,32-bit,any)
$(call reduce_optimization,3,0,test_sorted_runs,Version_10.0,32-bit,any)
$(call reduce_optimization,3,0,test_push_sort,Version_10.0,32-bit,any)
$(call reduce_optimization,3,1,test_transpose,Version_10.0,32-bit,any)
$(call reduce_optimization,3,0,test_asynchronous_nodes,Version_10.0,32-bit,any)
$(call reduce_optimization,3,0,test_asynchronous_stream_sort,Version_10.0,32-bit,any)
$(call reduce_optimization,3,0,test_asynchronous_stream_sort.as,Version_10.0,32-bit,any)
$(call reduce_optimization,3,0,test_asynchronous_stream_sort.as-ba,Version_10.0,32-bit,any)
$(call reduce_optimization,3,0,test_asynchronous_distribute_gather,Version_10.0,32-bit,any)
endif

ifneq (,$(filter yes,$(USE_PMODE)))
# PR40146: g++ 4.4: warning: '<anonymous>' is used uninitialized in this function
# when using g++-4.4 -fopenmp -O3. Fixed in g++-4.5
$(call reduce_optimization,3,1,test_stream,gcc_version_4.4,any,any)
endif

# PR40695: g++ 4.4: warning: '<anonymous>' may be used uninitialized in this function
ifneq (,$(filter yes,$(USE_PMODE)))
$(call reduce_optimization,3,2,test_asynchronous_stream_sort,gcc_version_4.4,any,any)
endif
$(call reduce_optimization,3,2,test_asynchronous_nodes,gcc_version_4.4,any,any)
$(call reduce_optimization,3,2,test_asynchronous_stream_sort.as,gcc_version_4.4,any,any)
$(call reduce_optimization,3,2,test_asynchronous_stream_sort.as-ba,gcc_version_4.4,any,any)
