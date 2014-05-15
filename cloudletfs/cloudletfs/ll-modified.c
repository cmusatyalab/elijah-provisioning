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

#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "cloudletfs-private.h"

//#define DEBUG_LL
#ifdef DEBUG_LL
#define DPRINTF(fmt, ...) \
    do { fprintf(stdout, "[DEBUG] IO: " fmt, ## __VA_ARGS__); } while (0)
#else
#define DPRINTF(fmt, ...) \
    do { } while (0)
#endif

bool _cloudletfs_ll_modified_init(struct cloudletfs_image *img, GError **err)
{
    char *file;

    file = g_strdup_printf("%s/cloudletfs-XXXXXX", g_get_tmp_dir());
    img->write_fd = mkstemp(file);
    if (img->write_fd == -1) {
        g_set_error(err, G_FILE_ERROR, g_file_error_from_errno(errno),
                "Couldn't create temporary file: %s", strerror(errno));
        g_free(file);
        return false;
    }
    unlink(file);
    g_free(file);
    img->modified_map = _cloudletfs_bit_new(img->bitmaps);
    return true;
}

void _cloudletfs_ll_modified_destroy(struct cloudletfs_image *img)
{
    _cloudletfs_bit_free(img->modified_map);
    close(img->write_fd);
}

bool _cloudletfs_ll_modified_read_chunk(struct cloudletfs_image *img,
        void *data, uint64_t chunk, uint32_t offset,
        uint32_t length, GError **err)
{
    uint64_t image_size = img->image_size;
    g_assert(_cloudletfs_bit_test(img->modified_map, chunk));
    g_assert(offset < img->chunk_size);
    g_assert(offset + length <= img->chunk_size);
    g_assert(chunk * img->chunk_size + offset + length <= image_size);

    return _cloudletfs_safe_pread("image", img->write_fd, data, length,
            chunk * img->chunk_size + offset, err);
}

bool _cloudletfs_ll_modified_write_chunk(struct cloudletfs_image *img,
        const void *data, uint64_t chunk, uint32_t offset,
        uint32_t length, GError **err)
{
    DPRINTF("krha, _cloudletfs_ll_modified_write_chunk, start(%ld), length(%ld)\n", \
	    chunk * img->chunk_size + offset, length);
    uint64_t image_size = img->image_size;
    g_assert(_cloudletfs_bit_test(img->modified_map, chunk) ||
            (offset == 0 && length == MIN(img->chunk_size,
            img->image_size - chunk * img->chunk_size)));
    g_assert(offset < img->chunk_size);
    g_assert(offset + length <= img->chunk_size);
    g_assert(chunk * img->chunk_size + offset + length <= image_size);

    if (_cloudletfs_safe_pwrite("image", img->write_fd, data, length,
            chunk * img->chunk_size + offset, err)) {
	// To get all modified chunks, every modification should be recorded.
	// However, _cloudletfs_bit_set send stream only when it is new bit map change
	// So, we force notification here.
        _cloudletfs_bit_set_force(img->modified_map, chunk, true);
        return true;
    } else {
        return false;
    }
}

