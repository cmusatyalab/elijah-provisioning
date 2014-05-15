ACLOCAL_AMFLAGS = -I m4

AM_CPPFLAGS = -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64
AM_CFLAGS = -std=gnu99 -W -Wall -Wstrict-prototypes -pthread \
	$(glib_CFLAGS) $(gthread_CFLAGS) $(fuse_CFLAGS)
AM_LDFLAGS = -pthread $(glib_LIBS) $(gthread_LIBS) $(fuse_LIBS)

pkglibexec_PROGRAMS = cloudletfs/cloudletfs
cloudletfs_cloudletfs_SOURCES = cloudletfs/bitmap.c cloudletfs/cond.c cloudletfs/fuse.c \
	cloudletfs/fuse-image.c cloudletfs/fuse-stats.c cloudletfs/fuse-stream.c \
	cloudletfs/io.c cloudletfs/ll-modified.c cloudletfs/ll-pristine.c \
	cloudletfs/stats.c cloudletfs/stream.c \
	cloudletfs/util.c cloudletfs/cloudletfs.c cloudletfs/cloudletfs-private.h


