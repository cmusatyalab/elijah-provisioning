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

#include <stdarg.h>
#include <string.h>
#include "cloudletfs-private.h"

#define BLOCK_SIZE 8192

struct cloudletfs_stream_group {
    GMutex *lock;
    GList *streams;
    populate_stream_fn *populate;
    void *populate_data;
    bool closed;
};

struct cloudletfs_stream {
    struct cloudletfs_stream_group *group;
    GList *group_link;

    GMutex *lock;
    struct cloudletfs_cond *cond;
    GQueue *blocks;
    uint32_t head_read_offset;
    uint32_t tail_write_offset;
    bool closed;
};

static void *block_new(void)
{
    return g_slice_alloc(BLOCK_SIZE);
}

static void block_free(void *block)
{
    g_slice_free1(BLOCK_SIZE, block);
}

/* Notify waiters of changes to the stream.  Stream lock must be held. */
static void notify_stream(struct cloudletfs_stream *strm)
{
    _cloudletfs_cond_broadcast(strm->cond);
}

struct cloudletfs_stream_group *_cloudletfs_stream_group_new(
        populate_stream_fn *populate, void *populate_data)
{
    struct cloudletfs_stream_group *sgrp;

    sgrp = g_slice_new0(struct cloudletfs_stream_group);
    sgrp->lock = g_mutex_new();
    sgrp->populate = populate;
    sgrp->populate_data = populate_data;
    return sgrp;
}

static void close_stream(struct cloudletfs_stream *strm,
        void *user_data G_GNUC_UNUSED)
{
    g_mutex_lock(strm->lock);
    strm->closed = true;
    notify_stream(strm);
    g_mutex_unlock(strm->lock);
}

/* Cause streams in the stream group to return end-of-file rather than
   cloudletfs_STREAM_ERROR_NONBLOCKING or blocking.  This tells stream readers
   to close their file descriptors so the filesystem can be unmounted. */
void _cloudletfs_stream_group_close(struct cloudletfs_stream_group *sgrp)
{
    g_mutex_lock(sgrp->lock);
    if (!sgrp->closed) {
        sgrp->closed = true;
        g_list_foreach(sgrp->streams, (GFunc) close_stream, NULL);
    }
    g_mutex_unlock(sgrp->lock);
}

void _cloudletfs_stream_group_free(struct cloudletfs_stream_group *sgrp)
{
    g_assert(g_list_length(sgrp->streams) == 0);
    g_mutex_free(sgrp->lock);
    g_slice_free(struct cloudletfs_stream_group, sgrp);
}

struct cloudletfs_stream *_cloudletfs_stream_new(struct cloudletfs_stream_group *sgrp)
{
    struct cloudletfs_stream *strm;

    strm = g_slice_new0(struct cloudletfs_stream);
    strm->lock = g_mutex_new();
    strm->cond = _cloudletfs_cond_new();
    strm->blocks = g_queue_new();
    strm->tail_write_offset = BLOCK_SIZE;
    if (sgrp->populate != NULL) {
        sgrp->populate(strm, sgrp->populate_data);
    }

    g_mutex_lock(sgrp->lock);
    sgrp->streams = g_list_prepend(sgrp->streams, strm);
    strm->group = sgrp;
    strm->group_link = sgrp->streams;
    strm->closed = sgrp->closed;
    g_mutex_unlock(sgrp->lock);

    return strm;
}

void _cloudletfs_stream_free(struct cloudletfs_stream *strm)
{
    void *block;

    g_mutex_lock(strm->group->lock);
    strm->group->streams = g_list_delete_link(strm->group->streams,
            strm->group_link);
    g_mutex_unlock(strm->group->lock);

    while ((block = g_queue_pop_head(strm->blocks)) != NULL) {
        block_free(block);
    }
    g_queue_free(strm->blocks);
    _cloudletfs_cond_free(strm->cond);
    g_mutex_free(strm->lock);
    g_slice_free(struct cloudletfs_stream, strm);
}

uint64_t _cloudletfs_stream_read(struct cloudletfs_stream *strm, void *buf,
        uint64_t count, bool blocking, GError **err)
{
    void *block;
    uint64_t block_length;
    uint64_t cur;
    uint64_t copied = 0;

    g_mutex_lock(strm->lock);
    while (copied < count) {
        g_assert(strm->head_read_offset < BLOCK_SIZE);
        block = g_queue_peek_head(strm->blocks);
        if (block == NULL) {
            /* No more data at the moment. */
            if (copied > 0) {
                break;
            } else if (strm->closed) {
                g_set_error(err, CLOUDLETFS_STREAM_ERROR,
                        CLOUDLETFS_STREAM_ERROR_CLOSED, "Stream closed");
                break;
            } else if (blocking) {
                if (_cloudletfs_cond_wait(strm->cond, strm->lock)) {
                    g_set_error(err, CLOUDLETFS_IO_ERROR,
                            CLOUDLETFS_IO_ERROR_INTERRUPTED,
                            "Operation interrupted");
                    break;
                } else {
                    continue;
                }
            } else {
                g_set_error(err, CLOUDLETFS_STREAM_ERROR,
                        CLOUDLETFS_STREAM_ERROR_NONBLOCKING,
                        "No input available");
                break;
            }
        }
        if (g_queue_peek_tail(strm->blocks) == block) {
            block_length = strm->tail_write_offset;
        } else {
            block_length = BLOCK_SIZE;
        }
        cur = MIN(count - copied, block_length - strm->head_read_offset);
        memcpy(buf + copied, block + strm->head_read_offset, cur);
        copied += cur;
        strm->head_read_offset += cur;
        if (strm->head_read_offset == BLOCK_SIZE) {
            /* Finished a complete block */
            block_free(g_queue_pop_head(strm->blocks));
            strm->head_read_offset = 0;
        } else if (g_queue_peek_tail(strm->blocks) == block &&
                strm->head_read_offset == strm->tail_write_offset) {
            /* We're in the middle of a partial block but
               we've consumed all data.  Delete the block. */
            block_free(g_queue_pop_head(strm->blocks));
            strm->head_read_offset = 0;
            strm->tail_write_offset = BLOCK_SIZE;
        }
    }
    g_mutex_unlock(strm->lock);
    return copied;
}

static void stream_write(struct cloudletfs_stream *strm, const void *buf,
        uint64_t count)
{
    void *block;
    uint64_t cur;
    uint64_t copied = 0;

    g_mutex_lock(strm->lock);
    while (copied < count) {
        g_assert(strm->tail_write_offset <= BLOCK_SIZE);
        if (strm->tail_write_offset == BLOCK_SIZE) {
            g_queue_push_tail(strm->blocks, block_new());
            strm->tail_write_offset = 0;
        }
        block = g_queue_peek_tail(strm->blocks);
        cur = MIN(count - copied, BLOCK_SIZE - strm->tail_write_offset);
        memcpy(block + strm->tail_write_offset, buf + copied, cur);
        copied += cur;
        strm->tail_write_offset += cur;
    }
    notify_stream(strm);
    g_mutex_unlock(strm->lock);
}

void _cloudletfs_stream_write(struct cloudletfs_stream *strm, const char *fmt, ...)
{
    char *buf;
    uint64_t len;
    va_list ap;

    va_start(ap, fmt);
    buf = g_strdup_vprintf(fmt, ap);
    len = strlen(buf);
    va_end(ap);

    stream_write(strm, buf, len);
    g_free(buf);
}

void _cloudletfs_stream_group_write(struct cloudletfs_stream_group *sgrp,
        const char *fmt, ...)
{
    GList *el;
    char *buf;
    uint64_t len;
    va_list ap;

    va_start(ap, fmt);
    buf = g_strdup_vprintf(fmt, ap);
    len = strlen(buf);
    va_end(ap);

    g_mutex_lock(sgrp->lock);
    for (el = g_list_first(sgrp->streams); el != NULL; el = g_list_next(el)) {
        stream_write(el->data, buf, len);
    }
    g_mutex_unlock(sgrp->lock);
    g_free(buf);
}

