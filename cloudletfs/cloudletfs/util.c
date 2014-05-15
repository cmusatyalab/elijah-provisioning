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
#include <unistd.h>
#include <errno.h>
#include "cloudletfs-private.h"

GQuark _cloudletfs_config_error_quark(void)
{
    return g_quark_from_static_string("cloudletfs-config-error-quark");
}

GQuark _cloudletfs_fuse_error_quark(void)
{
    return g_quark_from_static_string("cloudletfs-fuse-error-quark");
}

GQuark _cloudletfs_io_error_quark(void)
{
    return g_quark_from_static_string("cloudletfs-io-error-quark");
}

GQuark _cloudletfs_stream_error_quark(void)
{
    return g_quark_from_static_string("cloudletfs-stream-error-quark");
}

bool _cloudletfs_safe_pread(const char *file, int fd, void *buf, uint64_t count,
	uint64_t offset, GError **err)
{
    uint64_t cur;

    while (count > 0 && (cur = pread(fd, buf, count, offset)) > 0) {
	buf += cur;
	offset += cur;
	count -= cur;
    }
    if (count == 0) {
	return true;
    } else if (cur == 0) {
	g_set_error(err, CLOUDLETFS_IO_ERROR, CLOUDLETFS_IO_ERROR_PREMATURE_EOF,
		"Couldn't read %s: Premature end of file", file);
	return false;
    } else {
	g_set_error(err, G_FILE_ERROR, g_file_error_from_errno(errno),
		"Couldn't read %s: %s", file, strerror(errno));
	return false;
    }
}

bool _cloudletfs_safe_pwrite(const char *file, int fd, const void *buf,
	uint64_t count, uint64_t offset, GError **err)
{
    int64_t cur;

    while (count > 0 && (cur = pwrite(fd, buf, count, offset)) >= 0) {
	buf += cur;
	offset += cur;
	count -= cur;
    }
    if (count > 0) {
	g_set_error(err, G_FILE_ERROR, g_file_error_from_errno(errno),
		"Couldn't write %s: %s", file, strerror(errno));
	return false;
    }
    return true;
}
