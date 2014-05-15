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

#include <errno.h>
#include "cloudletfs-private.h"

static int stream_getattr(void *dentry_ctx G_GNUC_UNUSED, struct stat *st)
{
    st->st_mode = S_IFREG | 0440;
    return 0;
}

static int stream_open(void *dentry_ctx, struct cloudletfs_fuse_fh *fh)
{
    struct cloudletfs_stream_group *sgrp = dentry_ctx;

    fh->data = _cloudletfs_stream_new(sgrp);
    return 0;
}

static int stream_read(struct cloudletfs_fuse_fh *fh, void *buf,
        uint64_t start G_GNUC_UNUSED, uint64_t count)
{
    struct cloudletfs_stream *strm = fh->data;
    GError *err = NULL;
    int ret;

    ret = _cloudletfs_stream_read(strm, buf, count, fh->blocking, &err);
    if (err) {
        if (g_error_matches(err, CLOUDLETFS_STREAM_ERROR,
                CLOUDLETFS_STREAM_ERROR_NONBLOCKING)) {
            ret = -EAGAIN;
        } else if (g_error_matches(err, CLOUDLETFS_IO_ERROR,
                CLOUDLETFS_IO_ERROR_INTERRUPTED)) {
            ret = -EINTR;
        } else if (g_error_matches(err, CLOUDLETFS_STREAM_ERROR,
                CLOUDLETFS_STREAM_ERROR_CLOSED)) {
            ret = 0;
        } else {
            ret = -EIO;
        }
        g_clear_error(&err);
    }
    return ret;
}


static void stream_release(struct cloudletfs_fuse_fh *fh)
{
    struct cloudletfs_stream *strm = fh->data;
    _cloudletfs_stream_free(strm);
}

static const struct cloudletfs_fuse_ops stream_ops = {
    .getattr = stream_getattr,
    .open = stream_open,
    .read = stream_read,
    .release = stream_release,
    .nonseekable = true,
};

void _cloudletfs_fuse_stream_populate(struct cloudletfs_fuse_dentry *dir,
        struct cloudletfs_image *img)
{
    struct cloudletfs_fuse_dentry *streams;

    streams = _cloudletfs_fuse_add_dir(dir, "streams");
    _cloudletfs_fuse_add_file(streams, "chunks_accessed", &stream_ops,
            _cloudletfs_bit_get_stream_group(img->accessed_map));
    _cloudletfs_fuse_add_file(streams, "chunks_base", &stream_ops,
            _cloudletfs_bit_get_stream_group(img->total_overlay_map));
    _cloudletfs_fuse_add_file(streams, "chunks_overlay", &stream_ops,
                _cloudletfs_bit_get_stream_group(img->current_overlay_map));
    _cloudletfs_fuse_add_file(streams, "chunks_modified", &stream_ops,
            _cloudletfs_bit_get_stream_group(img->modified_map));
    _cloudletfs_fuse_add_file(streams, "io", &stream_ops, img->io_stream);
}
