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

static char *format_u64(uint64_t val)
{
    return g_strdup_printf("%"PRIu64"\n", val);
}

static int stat_getattr(void *dentry_ctx G_GNUC_UNUSED, struct stat *st)
{
    st->st_mode = S_IFREG | 0440;
    return 0;
}

static int u64_stat_open(void *dentry_ctx, struct cloudletfs_fuse_fh *fh)
{
    struct cloudletfs_stat *stat = dentry_ctx;

    if (_cloudletfs_stat_is_closed(stat)) {
        return -EACCES;
    }
    fh->data = stat;
    fh->buf = format_u64(_cloudletfs_u64_stat_get(stat));
    fh->length = strlen(fh->buf);
    return 0;
}

static int u64_fixed_open(void *dentry_ctx, struct cloudletfs_fuse_fh *fh)
{
    uint64_t *val = dentry_ctx;

    fh->buf = format_u64(*val);
    fh->length = strlen(fh->buf);
    return 0;
}

static int chunks_open(void *dentry_ctx, struct cloudletfs_fuse_fh *fh)
{
    struct cloudletfs_image *img = dentry_ctx;
    uint64_t image_size = img->image_size;
    uint64_t len;

    if (_cloudletfs_io_image_is_closed(img)) {
        return -EACCES;
    }
    fh->data = img;
    len = (image_size + img->chunk_size - 1) / img->chunk_size;
    fh->buf = format_u64(len);
    fh->length = strlen(fh->buf);
    return 0;
}

static int stat_read(struct cloudletfs_fuse_fh *fh, void *buf, uint64_t start,
        uint64_t count)
{
    uint64_t cur;

    if (fh->length <= start) {
        return 0;
    }
    cur = MIN(count, fh->length - start);
    memcpy(buf, fh->buf + start, cur);
    return cur;
}

static void stat_release(struct cloudletfs_fuse_fh *fh)
{
    g_free(fh->buf);
}

static const struct cloudletfs_fuse_ops u64_stat_ops = {
    .getattr = stat_getattr,
    .open = u64_stat_open,
    .read = stat_read,
    .release = stat_release,
};

static const struct cloudletfs_fuse_ops u64_fixed_ops = {
    .getattr = stat_getattr,
    .open = u64_fixed_open,
    .read = stat_read,
    .release = stat_release,
};

static const struct cloudletfs_fuse_ops chunks_ops = {
    .getattr = stat_getattr,
    .open = chunks_open,
    .read = stat_read,
    .release = stat_release,
};

void _cloudletfs_fuse_stats_populate(struct cloudletfs_fuse_dentry *dir,
        struct cloudletfs_image *img)
{
    struct cloudletfs_fuse_dentry *stats;

    stats = _cloudletfs_fuse_add_dir(dir, "stats");

#define add_stat(n) _cloudletfs_fuse_add_file(stats, #n, &u64_stat_ops, img->n)
    add_stat(bytes_read);
    add_stat(bytes_written);
    add_stat(chunk_dirties);
#undef add_stat

#define add_fixed(n) _cloudletfs_fuse_add_file(stats, #n, &u64_fixed_ops, &img->n)
    add_fixed(chunk_size);
#undef add_fixed

    _cloudletfs_fuse_add_file(stats, "chunks", &chunks_ops, img);
}
