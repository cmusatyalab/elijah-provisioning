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
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <inttypes.h>
#include <errno.h>
#include "cloudletfs-private.h"

bool _cloudletfs_ll_pristine_init(struct cloudletfs_image *img,
        GList *synthesis_chunk_info, GError **err G_GNUC_UNUSED) {
    // initialize total_overlay_map with total overlay information
    img->total_overlay_map = _cloudletfs_bit_new(img->bitmaps);
    img->current_overlay_map = _cloudletfs_bit_new(img->bitmaps);

    GList *chunk_list;
    for (chunk_list = synthesis_chunk_info; chunk_list != NULL; chunk_list =
            chunk_list->next) {
        // Set bit for total_overlay_map
        struct chunk_synthesis_info *cinfo =
                (struct chunk_synthesis_info *) chunk_list->data;
        _cloudletfs_bit_set(img->total_overlay_map, cinfo->chunk_number);
        if (cinfo->valid) {
            // Set bit for current_overlay_map only when the chunk is available
            _cloudletfs_bit_set(img->current_overlay_map, cinfo->chunk_number);
        }
    }
    return true;
}


void _cloudletfs_ll_pristine_destroy(struct cloudletfs_image *img)
{
    _cloudletfs_bit_free(img->total_overlay_map);
    _cloudletfs_bit_free(img->current_overlay_map);
}


bool _cloudlet_read_chunk(struct cloudletfs_image *img, int read_fd, void *data,
		uint64_t chunk, uint32_t offset, uint32_t length, GError **err)
{
    bool ret;
    ret = _cloudletfs_safe_pread("cloudlet", read_fd, data, length, chunk * img->chunk_size + offset, err);
    return ret;
}

