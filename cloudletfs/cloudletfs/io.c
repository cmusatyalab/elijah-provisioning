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

#include <inttypes.h>
#include <sys/time.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <stdio.h>
#include "cloudletfs-private.h"

//#define DEBUG_IO
#ifdef DEBUG_IO
#define DPRINTF(fmt, ...) \
    do { fprintf(stdout, "[DEBUG][IO] " fmt, ## __VA_ARGS__); } while (0)
#else
#define DPRINTF(fmt, ...) \
    do { } while (0)
#endif

#define CLOUDLET_IO
#ifdef CLOUDLET_IO
#define CPRINTF(fmt, ...) \
    do { fprintf(stdout, "[FUSE][IO] " fmt, ## __VA_ARGS__); fflush(stdout);} while (0)
#else
#define CPRINTF(fmt, ...) \
    do { } while (0)
#endif


struct chunk_state {
    GMutex *lock;
    GHashTable *chunk_locks;
    bool image_closed;
};

struct chunk_lock {
    uint64_t chunk;
    struct cloudletfs_cond *available;
    bool busy;
    uint32_t waiters;
};

static struct chunk_state *chunk_state_new(void)
{
    struct chunk_state *cs;

    cs = g_slice_new0(struct chunk_state);
    cs->lock = g_mutex_new();
    cs->chunk_locks = g_hash_table_new(g_int64_hash, g_int64_equal);
    return cs;
}

static void chunk_state_free(struct chunk_state *cs)
{
    g_assert(g_hash_table_size(cs->chunk_locks) == 0);

    g_hash_table_destroy(cs->chunk_locks);
    g_mutex_free(cs->lock);
    g_slice_free(struct chunk_state, cs);
}

/* Returns false if the lock was not acquired because the FUSE request
   was interrupted. */
static bool G_GNUC_WARN_UNUSED_RESULT chunk_trylock(struct cloudletfs_image *img,
        uint64_t chunk, GError **err)
{
    struct chunk_state *cs = img->chunk_state;
    struct chunk_lock *cl;
    bool ret = true;

    g_mutex_lock(cs->lock);
    cl = g_hash_table_lookup(cs->chunk_locks, &chunk);
    if (cl != NULL) {
        cl->waiters++;
        while (cl->busy &&
                !_cloudletfs_cond_wait(cl->available, cs->lock)) {}
        if (cl->busy) {
            /* We were interrupted, give up.  If we were interrupted but
               also acquired the lock, we pretend we weren't interrupted
               so that we never have to free the lock in this path. */
            g_set_error(err, CLOUDLETFS_IO_ERROR, CLOUDLETFS_IO_ERROR_INTERRUPTED,
                    "Operation interrupted");
            ret = false;
        } else {
            cl->busy = true;
        }
        cl->waiters--;
    } else {
        cl = g_slice_new0(struct chunk_lock);
        cl->chunk = chunk;
        cl->available = _cloudletfs_cond_new();
        cl->busy = true;
        g_hash_table_replace(cs->chunk_locks, &cl->chunk, cl);
    }
    g_mutex_unlock(cs->lock);
    return ret;
}

static void chunk_unlock(struct cloudletfs_image *img, uint64_t chunk)
{
    struct chunk_state *cs = img->chunk_state;
    struct chunk_lock *cl;

    g_mutex_lock(cs->lock);
    cl = g_hash_table_lookup(cs->chunk_locks, &chunk);
    g_assert(cl != NULL);
    if (cl->waiters > 0) {
        cl->busy = false;
        _cloudletfs_cond_signal(cl->available);
    } else {
        g_hash_table_remove(cs->chunk_locks, &chunk);
        _cloudletfs_cond_free(cl->available);
        g_slice_free(struct chunk_lock, cl);
    }
    g_mutex_unlock(cs->lock);
}

bool _cloudletfs_io_init(struct cloudletfs_image *img, GList* synthesis_chunk_info, GError **err)
{
    img->bitmaps = _cloudletfs_bit_group_new((img->image_size +
            img->chunk_size - 1) / img->chunk_size);
    if (!_cloudletfs_ll_pristine_init(img, synthesis_chunk_info, err)) {
        _cloudletfs_bit_group_free(img->bitmaps);
        return false;
    }
    if (!_cloudletfs_ll_modified_init(img, err)) {
        _cloudletfs_ll_pristine_destroy(img);
        _cloudletfs_bit_group_free(img->bitmaps);
        return false;
    }
    img->accessed_map = _cloudletfs_bit_new(img->bitmaps);
    img->chunk_state = chunk_state_new();
    return true;
}

void _cloudletfs_io_close(struct cloudletfs_image *img)
{
    struct chunk_state *cs = img->chunk_state;

    _cloudletfs_bit_group_close(img->bitmaps);

    g_mutex_lock(cs->lock);
    cs->image_closed = true;
    g_mutex_unlock(cs->lock);
}

bool _cloudletfs_io_image_is_closed(struct cloudletfs_image *img)
{
    struct chunk_state *cs = img->chunk_state;
    bool ret;

    g_mutex_lock(cs->lock);
    ret = cs->image_closed;
    g_mutex_unlock(cs->lock);
    return ret;
}

void _cloudletfs_io_destroy(struct cloudletfs_image *img)
{
    if (img == NULL) {
        return;
    }
    _cloudletfs_ll_modified_destroy(img);
    _cloudletfs_ll_pristine_destroy(img);
    chunk_state_free(img->chunk_state);
    _cloudletfs_bit_free(img->accessed_map);
    _cloudletfs_bit_group_free(img->bitmaps);
}

static uint64_t read_chunk_unlocked(struct cloudletfs_image *img,
        void *data, uint64_t chunk, uint32_t offset,
        uint32_t length, GError **err)
{
    uint64_t image_size = img->image_size;
    GTimer *waiting_start_time;
    gdouble time_diff;
    g_assert(offset < img->chunk_size);
    g_assert(offset + length <= img->chunk_size);

    if (chunk * img->chunk_size + offset >= image_size) {
	g_set_error(err, CLOUDLETFS_IO_ERROR, CLOUDLETFS_IO_ERROR_EOF,
		"End of file");
	return false;
    }
    length = MIN(image_size - chunk * img->chunk_size - offset, length);
    _cloudletfs_bit_set(img->accessed_map, chunk);
    if (_cloudletfs_bit_test(img->modified_map, chunk)) {
	if (!_cloudletfs_ll_modified_read_chunk(img, data, chunk,
		    offset, length, err)) {
	    return 0;
	}
    } else {
	/* If two cloudletfs instances are working out of the same pristine
	   cache, they will redundantly fetch chunks due to our failure to
	   keep the present map up to date. */

	if (_cloudletfs_bit_test(img->total_overlay_map, chunk) == false) {
	    // get it from Base VM
	    if (!_cloudlet_read_chunk(img, img->base_fd, data, chunk, 
			offset, length, err)) {
		return 0;
	    }
	}else{
	    if (_cloudletfs_bit_test(img->current_overlay_map, chunk)) {
		// get it from overlay VM
		if (!_cloudlet_read_chunk(img, img->overlay_fd, data, 
			    chunk, offset, length, err)) {
		    return 0;
		}
	    } else {
		// send message for on-demand fetching
		waiting_start_time = g_timer_new();

		CPRINTF("REQUEST,type:%s,chunk:%ld,thread:%ld\n", \
			img->type, chunk, syscall(SYS_gettid));

		// wait until get it from client
		_cloudletfs_wait_on_bit(img->current_overlay_map, chunk);
		time_diff = g_timer_elapsed(waiting_start_time, 0);
		CPRINTF("STATISTICS-WAIT,type:%s,chunk:%ld,time:%.6f\n",
			img->type, chunk, time_diff);
		// Get it from overlay VM
		if (!_cloudlet_read_chunk(img, img->overlay_fd, data, chunk,
			    offset, length, err)) {
		    return 0;	// failed to get right chunk from overlay
		}
	    }
	}
    }
    return length;
}

uint64_t _cloudletfs_io_read_chunk(struct cloudletfs_image *img, void *data,
        uint64_t chunk, uint32_t offset, uint32_t length, GError **err)
{
    uint64_t ret;

    if (!chunk_trylock(img, chunk, err)) {
        return false;
    }
    ret = read_chunk_unlocked(img, data, chunk, offset, length,
            err);
    chunk_unlock(img, chunk);
    return ret;
}

/* chunk lock must be held. */
static bool copy_to_modified(struct cloudletfs_image *img, uint64_t chunk, GError **err)
{
    uint64_t count;
    uint64_t read_count;
    void *buf;
    bool ret;
    GError *my_err = NULL;

    count = MIN(img->image_size - chunk * img->chunk_size, img->chunk_size);
    buf = g_malloc(count);

    _cloudletfs_u64_stat_increment(img->chunk_dirties, 1);
    read_count = read_chunk_unlocked(img, buf, chunk, 0, count,
            &my_err);
    if (read_count != count) {
        if (!my_err) {
            g_set_error(err, CLOUDLETFS_IO_ERROR, CLOUDLETFS_IO_ERROR_PREMATURE_EOF,
                    "Short count %"PRIu64"/%"PRIu64, read_count, count);
        } else {
            g_propagate_error(err, my_err);
        }
        g_free(buf);
        return false;
    }
    ret = _cloudletfs_ll_modified_write_chunk(img, buf, chunk, 0, count, err);

    g_free(buf);
    return ret;
}

uint64_t _cloudletfs_io_write_chunk(struct cloudletfs_image *img, const void *data,
        uint64_t chunk, uint32_t offset, uint32_t length, GError **err)
{
    uint64_t ret = 0;

    g_assert(offset < img->chunk_size);
    g_assert(offset + length <= img->chunk_size);

    if (!chunk_trylock(img, chunk, err)) {
        return false;
    }
    _cloudletfs_bit_set(img->accessed_map, chunk);
    if (!_cloudletfs_bit_test(img->modified_map, chunk)) {
        if (!copy_to_modified(img, chunk, err)) {
            goto out;
        }
    }
    if (_cloudletfs_ll_modified_write_chunk(img, data, chunk,
            offset, length, err)) {
        ret = length;
    }
out:
    chunk_unlock(img, chunk);
    return ret;
}
