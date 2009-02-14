# This -*- Makefile -*- is intended for processing with GNU make.

# Change this file according to your paths.

# Instead of modifying this file, you could also set your modified variables
# in make.settings.local (needs to be created first).
-include $(dir $(word $(words $(MAKEFILE_LIST)),$(MAKEFILE_LIST)))make.settings.local


USE_BOOST	?= no	# set 'yes' to use Boost libraries or 'no' to not use Boost libraries
USE_LINUX	?= yes	# set 'yes' if you run Linux, 'no' otherwise
USE_PMODE	?= no	# will be overriden from main Makefile
USE_ICPC	?= no	# will be overriden from main Makefile

STXXL_ROOT	?= $(HOME)/work/stxxl

ifeq ($(strip $(USE_ICPC)),yes)
COMPILER	?= icpc
OPENMPFLAG	?= -openmp
ICPC_PARALLEL_MODE_CPPFLAGS	?= -gcc-version=420 -cxxlib=$(FAKEGCC)
endif

ifeq ($(strip $(USE_PMODE)),yes)
COMPILER	?= g++-trunk
OPENMPFLAG	?= -fopenmp
ifeq ($(strip $(USE_ICPC)),yes)
LIBNAME		?= pmstxxl_icpc
else
LIBNAME		?= pmstxxl
endif
# only set the following variables if autodetection does not work:
endif

BOOST_INCLUDE	?= /usr/include/boost-1_33

COMPILER	?= g++
LINKER		?= $(COMPILER)
OPT		?= -O3 -Wno-deprecated # compiler optimization level
WARNINGS	?= -Wall
DEBUG		?= # put here -g option to include the debug information into the binaries

LIBNAME		?= stxxl


#### TROUBLESHOOTING
#
# For automatical checking of order of the output elements in
# the sorters: stxxl::stream::sort, stxxl::stream::merge_runs,
# stxxl::sort, and stxxl::ksort use
#
#STXXL_SPECIFIC	+= -DSTXXL_CHECK_ORDER_IN_SORTS
#
# If your program aborts with message "read/write: wrong parameter"
# or "Invalid argument"
# this could be that your kernel does not support direct I/O
# then try to set it off recompiling the libs and your code with option
#
#STXXL_SPECIFIC	+= -DSTXXL_DIRECT_IO_OFF
#
# But for the best performance it is strongly recommended
# to reconfigure the kernel for the support of the direct I/O.
#
# FIXME: documentation needed
#
#STXXL_SPECIFIC += -DSTXXL_DEBUGMON


#### You usually shouldn't need to change the sections below #####


#### STXXL OPTIONS ###############################################

STXXL_SPECIFIC	+= \
	$(CPPFLAGS_ARCH) \
	-DSORT_OPTIMAL_PREFETCHING \
	-DUSE_MALLOC_LOCK \
	-DCOUNT_WAIT_TIME \
	-I$(strip $(STXXL_ROOT))/include \
	-D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE \
	$(POSIX_MEMALIGN) $(XOPEN_SOURCE)

STXXL_LDLIBS	+= -L$(strip $(STXXL_ROOT))/lib -l$(LIBNAME) -lpthread

STXXL_LIBDEPS	+= $(strip $(STXXL_ROOT))/lib/lib$(LIBNAME).$(LIBEXT)

UNAME_M		:= $(shell uname -m)
CPPFLAGS_ARCH	+= $(CPPFLAGS_$(UNAME_M))
CPPFLAGS_i686	?= -march=i686

##################################################################


#### ICPC OPTIONS ################################################

ifeq ($(strip $(USE_ICPC)),yes)

STXXL_SPECIFIC	+= -include stxxl/bits/common/intel_compatibility.h

endif

##################################################################


#### PARALLEL_MODE OPTIONS ###############################################

ifeq ($(strip $(USE_PMODE)),yes)

PARALLEL_MODE_CPPFLAGS          += $(OPENMPFLAG) -D_GLIBCXX_PARALLEL
PARALLEL_MODE_LDFLAGS           += $(OPENMPFLAG)

ifeq ($(strip $(USE_ICPC)),yes)
PARALLEL_MODE_CPPFLAGS		+= $(ICPC_PARALLEL_MODE_CPPFLAGS)
endif

endif

##################################################################


#### BOOST OPTIONS ###############################################

BOOST_COMPILER_OPTIONS	 = \
	-DSTXXL_BOOST_TIMESTAMP \
	-DSTXXL_BOOST_CONFIG \
	-DSTXXL_BOOST_FILESYSTEM \
	-DSTXXL_BOOST_THREADS \
	-DSTXXL_BOOST_RANDOM \
	-I$(strip $(BOOST_INCLUDE)) \
	-pthread

BOOST_LINKER_OPTIONS	 = \
	-lboost_thread-gcc-mt \
	-lboost_date_time-gcc-mt \
	-lboost_iostreams-gcc-mt \
	-lboost_filesystem-gcc-mt

##################################################################


#### CPPUNIT OPTIONS ############################################

CPPUNIT_COMPILER_OPTIONS	+=

CPPUNIT_LINKER_OPTIONS		+= -lcppunit -ldl

##################################################################


#### DEPENDENCIES ################################################

HEADER_FILES_BITS	+= namespace.h version.h

HEADER_FILES_COMMON	+= aligned_alloc.h mutex.h rand.h semaphore.h state.h
HEADER_FILES_COMMON	+= timer.h utils.h gprof.h rwlock.h simple_vector.h
HEADER_FILES_COMMON	+= switch.h tmeta.h log.h exceptions.h debug.h tuple.h
HEADER_FILES_COMMON	+= types.h utils_ledasm.h settings.h

HEADER_FILES_IO		+= completion_handler.h io.h iobase.h iostats.h
HEADER_FILES_IO		+= mmap_file.h simdisk_file.h syscall_file.h
HEADER_FILES_IO		+= ufs_file.h wincall_file.h wfs_file.h boostfd_file.h

HEADER_FILES_MNG	+= adaptor.h async_schedule.h block_prefetcher.h
HEADER_FILES_MNG	+= buf_istream.h buf_ostream.h buf_writer.h mng.h
HEADER_FILES_MNG	+= write_pool.h prefetch_pool.h

HEADER_FILES_CONTAINERS	+= pager.h stack.h vector.h priority_queue.h queue.h
HEADER_FILES_CONTAINERS	+= map.h deque.h

HEADER_FILES_CONTAINERS_BTREE	+= btree.h iterator_map.h leaf.h node_cache.h
HEADER_FILES_CONTAINERS_BTREE	+= root_node.h node.h btree_pager.h iterator.h

HEADER_FILES_ALGO	+= adaptor.h inmemsort.h intksort.h run_cursor.h sort.h
HEADER_FILES_ALGO	+= async_schedule.h interleaved_alloc.h ksort.h
HEADER_FILES_ALGO	+= losertree.h scan.h stable_ksort.h random_shuffle.h

HEADER_FILES_STREAM	+= stream.h sort_stream.h pipeline.h

HEADER_FILES_UTILS	+= malloc.h

###################################################################


#### MISC #########################################################

DEPEXT	 = $(LIBNAME).d # extension of dependency files
OBJEXT	 = $(LIBNAME).o	# extension of object files
LIBEXT	 = a		# static library file extension
EXEEXT	 = $(LIBNAME).bin # executable file extension
RM	 = rm -f	# remove file command
LIBGEN	 = ar cr	# library generation
OUT	 = -o		# output file option for the compiler and linker

d	?= $(strip $(DEPEXT))
o	?= $(strip $(OBJEXT))
bin	?= $(strip $(EXEEXT))

###################################################################


#### COMPILE/LINK RULES ###########################################

DEPS_MAKEFILES	:= $(wildcard ../Makefile.subdir.gnu ../make.settings ../make.settings.local)
%.$o: %.cpp $(DEPS_MAKEFILES)
	@$(RM) $@ $*.$d
	$(COMPILER) $(STXXL_COMPILER_OPTIONS) -MD -MF $*.$dT -c $(OUTPUT_OPTION) $< && mv $*.$dT $*.$d

LINK_STXXL	 = $(LINKER) $1 $(STXXL_LINKER_OPTIONS) -o $@

%.$(bin): %.$o $(STXXL_LIBDEPS)
	$(call LINK_STXXL, $<)

###################################################################


STXXL_COMPILER_OPTIONS	+= $(STXXL_SPECIFIC)
STXXL_COMPILER_OPTIONS	+= $(OPT) $(DEBUG) $(WARNINGS)
STXXL_LINKER_OPTIONS	+= $(STXXL_LDLIBS)

ifeq ($(strip $(USE_PMODE)),yes)
STXXL_COMPILER_OPTIONS	+= $(PARALLEL_MODE_CPPFLAGS)
STXXL_LINKER_OPTIONS    += $(PARALLEL_MODE_LDFLAGS)
endif

ifeq ($(strip $(USE_BOOST)),yes)
STXXL_COMPILER_OPTIONS	+= $(BOOST_COMPILER_OPTIONS)
STXXL_LINKER_OPTIONS    += $(BOOST_LINKER_OPTIONS)
endif

STXXL_COMPILER_OPTIONS	+= $(CPPFLAGS)

# vim: syn=make
