/***************************************************************************
 *  io/trim_file.cpp
 *
 *  Part of the STXXL. See http://stxxl.sourceforge.net
 *
 *  Copyright (C) 2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 *  Distributed under the Boost Software License, Version 1.0.
 *  (See accompanying file LICENSE_1_0.txt or copy at
 *  http://www.boost.org/LICENSE_1_0.txt)
 **************************************************************************/

#include <stxxl/bits/io/trim_file.h>

#if STXXL_HAVE_TRIM_FILE

__STXXL_BEGIN_NAMESPACE


trim_file::trim_file(
        const std::string & filename,
        int mode,
        int disk) : syscall_file(filename, mode, disk)
{
}

void trim_file::discard(offset_type offset, offset_type size)
{
    STXXL_VERBOSE("trim_file::discard(0x" << std::hex << offset << ", 0x" << size << ")");
}

const char * trim_file::io_type() const
{
    return "trim";
}

__STXXL_END_NAMESPACE

#endif  // #if STXXL_HAVE_TRIM_FILE
// vim: et:ts=4:sw=4
