/***************************************************************************
 *  include/stxxl/bits/io/trim_file.h
 *
 *  Part of the STXXL. See http://stxxl.sourceforge.net
 *
 *  Copyright (C) 2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 *  Distributed under the Boost Software License, Version 1.0.
 *  (See accompanying file LICENSE_1_0.txt or copy at
 *  http://www.boost.org/LICENSE_1_0.txt)
 **************************************************************************/

#ifndef STXXL_TRIM_FILE_HEADER
#define STXXL_TRIM_FILE_HEADER

#ifndef STXXL_HAVE_TRIM_FILE
#ifdef __linux__
 #define STXXL_HAVE_TRIM_FILE 1
#else
 #define STXXL_HAVE_TRIM_FILE 0
#endif
#endif

#if STXXL_HAVE_TRIM_FILE

#include <stxxl/bits/io/syscall_file.h>


__STXXL_BEGIN_NAMESPACE

//! \addtogroup fileimpl
//! \{

//! \brief Implementation of file based on UNIX syscalls
class trim_file : public syscall_file
{
    bool can_trim;
    offset_type start_lba_bytes;

public:
    //! \brief constructs file object
    //! \param filename path of file
    //! \attention filename must be resided at memory disk partition
    //! \param mode open mode, see \c stxxl::file::open_modes
    //! \param disk disk(file) identifier
    trim_file(
        const std::string & filename,
        int mode,
        int disk = -1);

    void discard(offset_type offset, offset_type size);
    const char * io_type() const;
};

//! \}

__STXXL_END_NAMESPACE

#endif  // #if STXXL_HAVE_TRIM_FILE

#endif // !STXXL_TRIM_FILE_HEADER
// vim: et:ts=4:sw=4
