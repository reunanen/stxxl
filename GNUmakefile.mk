#
# HowTo use GNUmakefile.mk: create GNUMAKEfile containing:
#

## # override MODE, NICE if you want
##
## # select your favorite subset of targets
## all: lib tests header-compile-test doxy
##
## include GNUmakefile.mk


MODE	?= g++
MODE	?= icpc

NICE	?= nice

default-all: lib tests header-compile-test

doxy release:
	$(MAKE) -f Makefile $@

lib:
	$(MAKE) -f Makefile library_$(MODE) library_$(MODE)_pmode

tests: lib
	$(NICE) $(MAKE) -f Makefile tests_$(MODE) tests_$(MODE)_pmode

header-compile-test: lib
	$(NICE) $(MAKE) -C test/compile-stxxl-headers
	$(NICE) $(MAKE) -C test/compile-stxxl-headers INSTANCE=pmstxxl

clean:
	$(MAKE) -f Makefile clean_$(MODE) clean_$(MODE)_pmode clean_doxy
	$(MAKE) -C test/compile-stxxl-headers clean

.PHONY: all default-all doxy lib tests header-compile-test clean
