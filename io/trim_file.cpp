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

#include <linux/fs.h>
#include <linux/fiemap.h>
#include <sys/ioctl.h>

__STXXL_BEGIN_NAMESPACE

namespace hdparm
{
#include "geom.c"
}

#define NUMEXTENTS 16
struct fiemap_request {
    struct fiemap fm;
    struct fiemap_extent fm_extents[NUMEXTENTS];
};


trim_file::trim_file(
        const std::string & filename,
        int mode,
        int disk) : syscall_file(filename, mode, disk), can_trim(false), start_lba_bytes(0), raw_fd(-1)
{
    // find LBA offset
    struct stat st;
    char path[PATH_MAX];
    const int verbose = 1;

    *path = '\0';
    if (fstat(this->file_des, &st) == 0) {
        if (hdparm::find_dev_in_directory(st.st_dev, "/dev", path, verbose) == 0) {
            int fd = -1;
            fd = open(path, O_RDONLY|O_NONBLOCK);
            if (fd != -1) {
                __u64 start_lba;
                if (hdparm::get_dev_geometry(fd, NULL, NULL, NULL, &start_lba, NULL) == 0) {
                    start_lba_bytes = start_lba << 9;
                    if (start_lba != 0) {
                        // we are on a partition, look for the disk device
                        if (hdparm::find_dev_in_directory(makedev(major(st.st_dev), minor(st.st_dev) & ~15), "/dev", path, verbose) == 0) {
                            close(fd);
                            fd = open(path, O_RDONLY|O_NONBLOCK);
                            if (fd != -1) {
                                if (hdparm::get_dev_geometry(fd, NULL, NULL, NULL, &start_lba, NULL) == 0) {
                                    if (start_lba == 0)
                                        can_trim = true;
                                }
                            }
                        }
                    } else {
                        can_trim = true;
                    }
                }
                if (fd != -1)
                    close(fd);
            }
        }
    }
    if (*path) {
        if ((raw_fd = open(path, O_RDONLY|O_NONBLOCK)) == -1)
            can_trim = false;
    }

    STXXL_VERBOSE("trim_file " << filename << "  raw=" << path << std::hex << "  start_lba_bytes=0x" << start_lba_bytes << "  can_trim=" << can_trim);
}

trim_file::~trim_file()
{
    if (raw_fd != -1)
        close(raw_fd);
}

void trim_file::discard(offset_type offset, offset_type size)
{
    STXXL_VERBOSE("trim_file::discard(0x" << std::hex << offset << ", 0x" << size << ")");
    if (!can_trim)
        return;

    // get LBA blocks for the requested range
    offset_type start = offset;
    offset_type end = start + size;
    while (start < end) {
        fiemap_request fs;
        assert(&fs.fm_extents[0] == &fs.fm.fm_extents[0]);
        memset(&fs, 0, sizeof(fiemap_request));
        fs.fm.fm_start = start;
        fs.fm.fm_length = end - start;
        fs.fm.fm_flags = 0;
        fs.fm.fm_extent_count = NUMEXTENTS;
        int err;
        {
            scoped_mutex_lock(this->fd_mutex);
            err = ioctl(file_des, FS_IOC_FIEMAP, &fs);
        }
        if (err < 0) {
            err = errno;
            STXXL_ERRMSG("ioctl(FIEMAP) failed (" << err << "): " << strerror(err));
            return;
        }
        STXXL_VERBOSE0("FIEMAP 0x" << std::hex << fs.fm.fm_start << " 0x" << fs.fm.fm_length << " => " << fs.fm.fm_mapped_extents);
        if (!fs.fm.fm_mapped_extents)
            break;
        for (unsigned i = 0; i < fs.fm.fm_mapped_extents; ++i) {
            STXXL_VERBOSE0("extent " << i << "/" << fs.fm.fm_mapped_extents << "  logical=0x" << std::hex << fs.fm_extents[i].fe_logical << "  physical=0x" << fs.fm_extents[i].fe_physical
                           << "  length=0x" << fs.fm_extents[i].fe_length << "  flags=0x" << fs.fm_extents[i].fe_flags);
            start = fs.fm_extents[i].fe_logical + fs.fm_extents[i].fe_length;
            if (fs.fm_extents[i].fe_flags & (FIEMAP_EXTENT_UNKNOWN | FIEMAP_EXTENT_ENCODED | FIEMAP_EXTENT_NOT_ALIGNED))
                continue;
            offset_type phy_start = fs.fm_extents[i].fe_physical;
            if (offset > fs.fm_extents[i].fe_logical)
                phy_start += offset - fs.fm_extents[i].fe_logical;
            offset_type phy_end = fs.fm_extents[i].fe_physical + fs.fm_extents[i].fe_length;
            if (fs.fm_extents[i].fe_logical + fs.fm_extents[i].fe_length > end)
                phy_end -= fs.fm_extents[i].fe_logical + fs.fm_extents[i].fe_length - end;
            STXXL_VERBOSE0("physical location: 0x" << std::hex << phy_start << "  length: 0x" << phy_end - phy_start);
            if (fs.fm_extents[i].fe_flags & FIEMAP_EXTENT_LAST) {
                start = end;
                break;
            }
        }
    }
}

const char * trim_file::io_type() const
{
    return "trim";
}

__STXXL_END_NAMESPACE

#endif  // #if STXXL_HAVE_TRIM_FILE
// vim: et:ts=4:sw=4
