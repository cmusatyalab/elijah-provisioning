/*
 * Cloudletfs - virtual filesystem for synthesized VM at Cloudlet
 *
 * Copyright (C) 2006-2014 Carnegie Mellon University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <string.h>
#include <inttypes.h>
#include <errno.h>
#include "cloudletfs-private.h"

//#define DEBUG_FUSE_IMAGE
#ifdef DEBUG_FUSE_IMAGE
#define DPRINTF(fmt, ...) \
    do { fprintf(stdout, "[DEBUG] FUSE_IMAGE: " fmt, ## __VA_ARGS__); } while (0)
#else
#define DPRINTF(fmt, ...) \
    do { } while (0)
#endif

struct io_cursor {
    /* Public fields; do not modify */
    uint64_t chunk;
    uint64_t offset;
    uint64_t length;
    uint64_t buf_offset;

    /* Private fields */
    struct cloudletfs_image *img;
    uint64_t start;
    uint64_t count;
};

/* The cursor is assumed to be allocated on the stack; this just fills
   it in. */
static void io_start(struct cloudletfs_image *img, struct io_cursor *cur,
        uint64_t start, uint64_t count)
{
    memset(cur, 0, sizeof(*cur));
    cur->img = img;
    cur->start = start;
    cur->count = count;
}

/* Populate the public fields of the cursor with information on the next
   chunk in the I/O, starting from the first, given that the last I/O
   completed @count bytes.  Returns true if we produced a valid chunk,
   false if done with this I/O.  Assumes an infinite-size image. */
static bool io_chunk(struct io_cursor *cur, uint64_t count)
{
    // offset		: offset within a chunk
    // buf_offset	: offset within requested buffer
    // length		: choose min between
    //	1) left bytes within chunk_size in case where requested size of bigger than chunk
    //	2) left bytes in last chunk
    uint64_t position;

    cur->buf_offset += count;
    if (cur->buf_offset >= cur->count) {
        /* Done */
        return false;
    }
    position = cur->start + cur->buf_offset;
    cur->chunk = position / cur->img->chunk_size;
    cur->offset = position - cur->chunk * cur->img->chunk_size;
    cur->length = MIN(cur->img->chunk_size - cur->offset,
            cur->count - cur->buf_offset);

    return true;
}

static int image_getattr(void *dentry_ctx, struct stat *st)
{
    struct cloudletfs_image *img = dentry_ctx;

    st->st_mode = S_IFREG | 0660;
    st->st_size = img->image_size;
    return 0;
}


static int image_open(void *dentry_ctx, struct cloudletfs_fuse_fh *fh)
{
    struct cloudletfs_image *img = dentry_ctx;

    fh->data = img;
    return 0;
}

static int image_read(struct cloudletfs_fuse_fh *fh, void *buf, uint64_t start,
        uint64_t count)
{
    struct cloudletfs_image *img = fh->data;
    struct io_cursor cur;
    GError *err = NULL;
    uint64_t read = 0;

    _cloudletfs_stream_group_write(img->io_stream, "read %"PRIu64"+%"PRIu64"\n",
            start, count);
    for (io_start(img, &cur, start, count); io_chunk(&cur, read); ) {
        read = _cloudletfs_io_read_chunk(img, buf + cur.buf_offset, cur.chunk,
                cur.offset, cur.length, &err);
        if (err) {
            if (g_error_matches(err, CLOUDLETFS_IO_ERROR,
                    CLOUDLETFS_IO_ERROR_INTERRUPTED)) {
                g_clear_error(&err);
                return (int) (cur.buf_offset + read) ?: -EINTR;
            } else if (g_error_matches(err, CLOUDLETFS_IO_ERROR,
                    CLOUDLETFS_IO_ERROR_EOF)) {
                g_clear_error(&err);
                return cur.buf_offset + read;
            } else {
                g_warning("%s", err->message);
                g_clear_error(&err);
                return (int) (cur.buf_offset + read) ?: -EIO;
            }
        }
        _cloudletfs_u64_stat_increment(img->bytes_read, cur.length);
    }
    return cur.buf_offset;
}

static int image_write(struct cloudletfs_fuse_fh *fh, const void *buf,
        uint64_t start, uint64_t count)
{
    DPRINTF("krha, image_write: start(%ld), count(%ld)\n", start, count);
    struct cloudletfs_image *img = fh->data;
    struct io_cursor cur;
    GError *err = NULL;
    uint64_t written = 0;

    _cloudletfs_stream_group_write(img->io_stream, "write %"PRIu64"+%"PRIu64"\n",
            start, count);
    for (io_start(img, &cur, start, count); io_chunk(&cur, written); ) {
        written = _cloudletfs_io_write_chunk(img, buf + cur.buf_offset, cur.chunk,
                cur.offset, cur.length, &err);
        if (err) {
            if (g_error_matches(err, CLOUDLETFS_IO_ERROR,
                    CLOUDLETFS_IO_ERROR_INTERRUPTED)) {
                g_clear_error(&err);
                return (int) (cur.buf_offset + written) ?: -EINTR;
            } else {
                g_warning("%s", err->message);
                g_clear_error(&err);
                return (int) (cur.buf_offset + written) ?: -EIO;
            }
        }
        _cloudletfs_u64_stat_increment(img->bytes_written, cur.length);
    }
    return cur.buf_offset;
}

static const struct cloudletfs_fuse_ops image_ops = {
    .getattr = image_getattr,
    .open = image_open,
    .read = image_read,
    .write = image_write,
};

void _cloudletfs_fuse_image_populate(struct cloudletfs_fuse_dentry *dir,
        struct cloudletfs_image *img)
{
    _cloudletfs_fuse_add_file(dir, "image", &image_ops, img);
}
