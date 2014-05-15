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
#include <sys/time.h>
#include "cloudletfs-private.h"

/* Bitmap rules:
   1. All bitmaps in a group have the same size.
   2. Memory allocations for the actual bits are always a power of 2, and
      are never reduced.
   3. Bitmaps are initially zeroed.
*/

/* A set of bitmaps, each with the same size. */
struct bitmap_group {
    GMutex *lock;
    GList *maps;
    uint64_t nbits;
    uint64_t allocated_bytes;
    struct cloudletfs_cond *cond;
};

/* struct bitmap requires external serialization to ensure that the bits
   don't change while the caller requires them to be consistent. */
struct bitmap {
    struct bitmap_group *mgrp;
    uint8_t *bits;
    struct cloudletfs_stream_group *sgrp;
};

static void populate_stream(struct cloudletfs_stream *strm, void *_map)
{
    struct bitmap *map = _map;
    uint64_t byte;
    uint8_t bit;

    g_mutex_lock(map->mgrp->lock);
    for (byte = 0; byte < (map->mgrp->nbits + 7) / 8; byte++) {
        if (map->bits[byte]) {
            for (bit = 0; bit < 8; bit++) {
                if (byte * 8 + bit >= map->mgrp->nbits) {
                    break;
                }
                if (map->bits[byte] & (1 << bit)) {
                    _cloudletfs_stream_write(strm, "%"PRIu64"\n",
                            byte * 8 + bit);
                }
            }
        }
    }
    g_mutex_unlock(map->mgrp->lock);
}

/* Return the proper allocation to hold the specified number of bits. */
static uint64_t allocation_for_bits(uint64_t bits)
{
    /* Round up to the next power of two */
    return 1 << g_bit_storage(((bits - 1) + 7) / 8);
}

static void set_bit(struct bitmap *map, uint64_t bit)
{
    g_assert(bit < map->mgrp->allocated_bytes * 8);
    map->bits[bit / 8] |= 1 << (bit % 8);
}

static void notify_bit(struct bitmap *map, uint64_t bit)
{
    GTimeVal cur_time;
    g_get_current_time(&cur_time);
    _cloudletfs_stream_group_write(map->sgrp, "%d.%d\t%"PRIu64"\n", \
	    cur_time.tv_sec, cur_time.tv_usec, bit);
}

static bool test_bit(struct bitmap *map, uint64_t bit)
{
    return !!(map->bits[bit / 8] & (1 << (bit % 8)));
}

struct bitmap_group *_cloudletfs_bit_group_new(uint64_t initial_bits)
{
    struct bitmap_group *mgrp;

    mgrp = g_slice_new0(struct bitmap_group);
    mgrp->lock = g_mutex_new();
    mgrp->nbits = initial_bits;
    mgrp->allocated_bytes = allocation_for_bits(initial_bits);
    mgrp->cond = _cloudletfs_cond_new();
    return mgrp;
}

void _cloudletfs_bit_group_free(struct bitmap_group *mgrp)
{
    g_assert(mgrp->maps == NULL);
    g_mutex_free(mgrp->lock);
    _cloudletfs_cond_free(mgrp->cond);
    g_slice_free(struct bitmap_group, mgrp);
}

void _cloudletfs_bit_group_close(struct bitmap_group *mgrp)
{
    struct bitmap *map;
    GList *el;

    g_mutex_lock(mgrp->lock);
    for (el = g_list_first(mgrp->maps); el != NULL; el = g_list_next(el)) {
        map = el->data;
        _cloudletfs_stream_group_close(map->sgrp);
    }
    g_mutex_unlock(mgrp->lock);
}

/* All bits are initially set to zero. */
struct bitmap *_cloudletfs_bit_new(struct bitmap_group *mgrp)
{
    struct bitmap *map;

    map = g_slice_new0(struct bitmap);
    map->mgrp = mgrp;
    map->sgrp = _cloudletfs_stream_group_new(populate_stream, map);

    g_mutex_lock(mgrp->lock);
    map->bits = g_malloc0(mgrp->allocated_bytes);
    mgrp->maps = g_list_prepend(mgrp->maps, map);
    g_mutex_unlock(mgrp->lock);

    return map;
}

void _cloudletfs_bit_free(struct bitmap *map)
{
    g_mutex_lock(map->mgrp->lock);
    map->mgrp->maps = g_list_remove(map->mgrp->maps, map);
    g_mutex_unlock(map->mgrp->lock);
    _cloudletfs_stream_group_free(map->sgrp);
    g_free(map->bits);
    g_slice_free(struct bitmap, map);
}

void _cloudletfs_bit_set(struct bitmap *map, uint64_t bit)
{
    _cloudletfs_bit_set_force(map, bit, false);
}

void _cloudletfs_bit_set_force(struct bitmap *map, uint64_t bit, bool is_force_notify)
{
    bool is_new = false;

    g_mutex_lock(map->mgrp->lock);
    if (bit < map->mgrp->nbits) {
        is_new = !test_bit(map, bit);
        set_bit(map, bit);
    }
    g_mutex_unlock(map->mgrp->lock);
    if (is_force_notify) {
        notify_bit(map, bit);
    }else {
	if (is_new) {
		notify_bit(map, bit);
	}
    }
}

void _cloudletfs_bit_set_nolock(struct bitmap *map, uint64_t bit)
{
    if (bit < map->mgrp->nbits) {
        set_bit(map, bit);
    }
}

bool _cloudletfs_bit_test_print(struct bitmap *map, uint64_t bit){
    bool ret = true;

    g_mutex_lock(map->mgrp->lock);
    if (bit < map->mgrp->nbits) {
        ret = test_bit(map, bit);
    }
    g_mutex_unlock(map->mgrp->lock);
    return ret;

}

bool _cloudletfs_bit_test(struct bitmap *map, uint64_t bit)
{
    bool ret = true;

    g_mutex_lock(map->mgrp->lock);
    if (bit < map->mgrp->nbits) {
        ret = test_bit(map, bit);
    }
    g_mutex_unlock(map->mgrp->lock);
    return ret;
}

struct cloudletfs_stream_group *_cloudletfs_bit_get_stream_group(struct bitmap *map)
{
    return map->sgrp;
}

void _cloudletfs_bit_update(struct bitmap *map, uint64_t bit)
{
    g_mutex_lock(map->mgrp->lock);
    _cloudletfs_bit_set_nolock(map, bit);
    _cloudletfs_cond_broadcast(map->mgrp->cond);
    g_mutex_unlock(map->mgrp->lock);
}

void _cloudletfs_wait_on_bit(struct bitmap *map, uint64_t bit)
{
    g_mutex_lock(map->mgrp->lock);
    while(!test_bit(map, bit)){
        _cloudletfs_cond_wait(map->mgrp->cond, map->mgrp->lock);
    }
    g_mutex_unlock(map->mgrp->lock);
}
