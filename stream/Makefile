include Makefile.common
include ../make.settings

tests: $(TESTS)
#tests: $(TESTS_NON_MSVC)


DEPENDENCIES = $(COMMON_FILES) $(IO_LAYER_FILES) $(MNG_LAYER_FILES) \
               $(CONTAINER_FILES) $(ALGO_FILES) $(STREAM_FILES)


test_stream1: test_stream1.$(EXEEXT)
test_stream1.$(EXEEXT): test_stream1.cpp $(DEPENDENCIES)
	$(COMPILER) $(STXXL_COMPILER_OPTIONS) -c test_stream1.cpp -DSTXXL_VERBOSE_LEVEL=1
	$(LINKER)  test_stream1.$(OBJEXT) $(OUT)test_stream1.$(EXEEXT) $(STXXL_LINKER_OPTIONS)

test_sorted_runs: test_sorted_runs.$(EXEEXT)
test_sorted_runs.$(EXEEXT): test_sorted_runs.cpp $(DEPENDENCIES)
	$(COMPILER) $(STXXL_COMPILER_OPTIONS) -c test_sorted_runs.cpp -DSTXXL_VERBOSE_LEVEL=1
	$(LINKER)  test_sorted_runs.$(OBJEXT) $(OUT)test_sorted_runs.$(EXEEXT) $(STXXL_LINKER_OPTIONS)

test_push_sort: test_push_sort.$(EXEEXT)
test_push_sort.$(EXEEXT): test_push_sort.cpp $(DEPENDENCIES)
	$(COMPILER) $(STXXL_COMPILER_OPTIONS) -c test_push_sort.cpp -DSTXXL_VERBOSE_LEVEL=1
	$(LINKER)  test_push_sort.$(OBJEXT) $(OUT)test_push_sort.$(EXEEXT) $(STXXL_LINKER_OPTIONS)
	
test_stream: test_stream.$(EXEEXT)
test_stream.$(EXEEXT): test_stream.cpp $(DEPENDENCIES)
	$(COMPILER) $(STXXL_COMPILER_OPTIONS) -c test_stream.cpp -DSTXXL_VERBOSE_LEVEL=1
	$(LINKER)  test_stream.$(OBJEXT) $(OUT)test_stream.$(EXEEXT) $(STXXL_LINKER_OPTIONS)

test_asynchronous_nodes: test_asynchronous_nodes.$(EXEEXT)
test_asynchronous_nodes.$(EXEEXT): test_asynchronous_nodes.cpp $(DEPENDENCIES)
	$(COMPILER) $(STXXL_COMPILER_OPTIONS) -c test_asynchronous_nodes.cpp -DSTXXL_VERBOSE_LEVEL=1
	$(LINKER)  test_asynchronous_nodes.$(OBJEXT) $(OUT)test_asynchronous_nodes.$(EXEEXT) $(STXXL_LINKER_OPTIONS)

test_asynchronous_stream_sort: test_asynchronous_stream_sort.$(EXEEXT)
test_asynchronous_stream_sort.$(EXEEXT): test_asynchronous_stream_sort.cpp $(DEPENDENCIES)
	$(COMPILER) $(STXXL_COMPILER_OPTIONS) -c test_asynchronous_stream_sort.cpp -DSTXXL_VERBOSE_LEVEL=1 -DSTXXL_STREAM_SORT_ASYNCHRONOUS_PULL=1 -DBATCHED=1
	$(LINKER)  test_asynchronous_stream_sort.$(OBJEXT) $(OUT)test_asynchronous_stream_sort.$(EXEEXT) $(STXXL_LINKER_OPTIONS)

test_asynchronous_distribute_gather: test_asynchronous_distribute_gather.$(EXEEXT)
test_asynchronous_distribute_gather.$(EXEEXT): test_asynchronous_distribute_gather.cpp $(DEPENDENCIES)
	$(COMPILER) $(STXXL_COMPILER_OPTIONS) -c test_asynchronous_distribute_gather.cpp -DSTXXL_VERBOSE_LEVEL=1
	$(LINKER)  test_asynchronous_distribute_gather.$(OBJEXT) $(OUT)test_asynchronous_distribute_gather.$(EXEEXT) $(STXXL_LINKER_OPTIONS)


clean:
	$(RM) *.$(OBJEXT) test_stream.$(EXEEXT) test_sorted_runs.$(EXEEXT)
	$(RM) test_push_sort.$(EXEEXT) test_stream1.$(EXEEXT)
