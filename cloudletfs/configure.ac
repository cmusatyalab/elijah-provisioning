# 
# Cloudletfs - virtual filesystem for synthesized VM at Cloudlet
# 
# Copyright (C) 2006-2014 Carnegie Mellon University
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 


## Process this file with autoconf to produce a configure script.

AC_PREREQ(2.63)
AC_INIT([cloudletfs], [0.2])
AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_MACRO_DIR([m4])
AC_COPYRIGHT([Copyright (C) 2006-2014 Carnegie Mellon University])
AC_CONFIG_SRCDIR([cloudletfs/cloudletfs.c])
AC_CONFIG_HEADERS([config.h])
AC_DISABLE_STATIC

AM_INIT_AUTOMAKE([foreign 1.11 dist-xz])
AM_SILENT_RULES([yes])

# Checks for programs.
AC_PROG_CC
AC_PROG_LIBTOOL
PKG_PROG_PKG_CONFIG()

# Checks for libraries.
PKG_CHECK_MODULES([fuse], [fuse >= 2.7])
PKG_CHECK_MODULES([glib], [glib-2.0 >= 2.22])
PKG_CHECK_MODULES([gthread], [gthread-2.0])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
