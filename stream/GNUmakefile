TOPDIR	?= ..
include Makefile.common

TESTS-yes			+= $(TESTS_NON_MSVC)

include $(TOPDIR)/Makefile.subdir.gnu


test_stream1.$o:		CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=1
test_sorted_runs.$o:		CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=1
test_push_sort.$o:		CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=1
test_stream.$o:			CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=1
test_asynchronous_nodes.$o:	CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=1
test_asynchronous_stream_sort.$o:	CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=1
test_asynchronous_stream_sort.as.$o:	CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=1
test_asynchronous_stream_sort.as-ba.$o:	CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=1
test_asynchronous_distribute_gather.$o:	CPPFLAGS += -DSTXXL_VERBOSE_LEVEL=1

$(eval $(call COMPILE_VARIANT,as,-DSTXXL_STREAM_SORT_ASYNCHRONOUS_PULL=1))
$(eval $(call COMPILE_VARIANT,as-ba,-DSTXXL_STREAM_SORT_ASYNCHRONOUS_PULL=1 -DBATCHED=1))

ifeq ($(strip $(USE_MCSTL)),yes)
# ICPC 10.0: internal error: backend signals (issue 466173)
$(call reduce_optimization,3,1,test_stream,Version_10.0,32-bit,any)
$(call reduce_optimization,3,1,test_stream1,Version_10.0,32-bit,any)
$(call reduce_optimization,3,1,test_sorted_runs,Version_10.0,32-bit,any)
$(call reduce_optimization,3,0,test_push_sort,Version_10.0,32-bit,any)
$(call reduce_optimization,3,0,test_asynchronous_nodes,Version_10.0,32-bit,any)
$(call reduce_optimization,3,0,test_asynchronous_stream_sort,Version_10.0,32-bit,any)
$(call reduce_optimization,3,0,test_asynchronous_distribute_gather,Version_10.0,32-bit,any)
endif
