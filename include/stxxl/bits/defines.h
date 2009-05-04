/***************************************************************************
 *  include/stxxl/bits/defines.h
 *
 *  Document all defines that may change the behavior of stxxl.
 *
 *  Part of the STXXL. See http://stxxl.sourceforge.net
 *
 *  Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 *  Distributed under the Boost Software License, Version 1.0.
 *  (See accompanying file LICENSE_1_0.txt or copy at
 *  http://www.boost.org/LICENSE_1_0.txt)
 **************************************************************************/

#ifndef STXXL_DEFINES_HEADER
#define STXXL_DEFINES_HEADER

//#define STXXL_CHECK_BLOCK_ALIGNING
// default: not defined
// used in: io/*_file.cpp
// effect:  call request::check_alignment() from request::request(...)

#ifndef STXXL_PARALLEL_MULTIWAY_MERGE
#define STXXL_PARALLEL_MULTIWAY_MERGE 1
#endif
// default: 0 (disabled), in parallel.h
// used in: algo/*sort*.h, stream/*sort*.h
// effect:  use parallel (internal memory) sorters

#endif // !STXXL_DEFINES_HEADER
