/* hdparm.c - Command line interface to get/set hard disk parameters */
/*          - by Mark Lord (C) 1994-2008 -- freely distributable */
//#define _BSD_SOURCE	/* for strtoll() */
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
//#define __USE_GNU	/* for O_DIRECT */
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <ctype.h>
#include <endian.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/types.h>
//#include <sys/mount.h>
#include <sys/mman.h>
#include <sys/user.h>
#include <linux/types.h>
#include <linux/fs.h>
#include <linux/major.h>
#include <asm/byteorder.h>

#include "hdparm.h"
#include "sgio.h"


int verbose = 0;
int prefer_ata12 = 0;

static inline void abort_if_not_full_device(int, int, const char *, void *)
{ }
static inline void flush_buffer_cache(int)
{ }

struct sector_range_s {
	__u64	lba;
	__u64	nsectors;
};

static int trim_sectors (int fd, const char *devname, int nranges, void *data, __u64 nsectors)
{
	struct ata_tf tf;
	int err = 0;
	unsigned int data_bytes = nranges * sizeof(__u64);
	unsigned int data_sects = (data_bytes + 511) / 512;

	data_bytes = data_sects * 512;

	abort_if_not_full_device(fd, 0, devname, NULL);
	printf("trimming %llu sectors from %d ranges\n", nsectors, nranges);
	fflush(stdout);

	// Try and ensure that the system doesn't have the to-be-trimmed sectors in cache:
	flush_buffer_cache(fd);

	tf_init(&tf, ATA_OP_DSM, 0, data_sects);
	tf.lob.feat = 0x01;	/* DSM/TRIM */

	if (sg16(fd, SG_WRITE, SG_DMA, &tf, data, data_bytes, 300 /* seconds */)) {
		err = errno;
		perror("FAILED");
	} else {
		printf("succeeded\n");
	}
	return err;
}

static int do_trim_sector_ranges (int fd, const char *devname, int nranges, struct sector_range_s *sr)
{
	__u64 range, *data, nsectors = 0;
	unsigned int data_sects, data_bytes;
	int i, err = 0;

	abort_if_not_full_device(fd, 0, devname, NULL);

	data_sects = ((nranges * sizeof(range)) + 511) / 512;
	data_bytes = data_sects * 512;

	data = (__u64 *)mmap(NULL, data_bytes, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANONYMOUS, -1, 0);
	if (data == MAP_FAILED) {
		err = errno;
		perror("mmap(MAP_ANONYMOUS)");
		exit(err);
	}
	// FIXME: handle counts > 65535 here!
	for (i = 0; i < nranges; ++i) {
		nsectors += sr->nsectors;
		range = sr->nsectors;
		range = (range << 48) | sr->lba;
		data[i] = __cpu_to_le64(range);
		++sr;
	}

	err = trim_sectors(fd, devname, nranges, data, nsectors);
	munmap(data, data_bytes);
	return err;
}
