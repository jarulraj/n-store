# configure.in -- Process this file with autoconf to produce configure
AC_PREREQ([2.64])

AC_INIT([n-store], [0.1], [xyz@cs.cmu.edu])
AC_CONFIG_HEADERS([config.h:config-h.in])
AC_CONFIG_AUX_DIR([config])
AC_CONFIG_MACRO_DIR([m4])

AM_INIT_AUTOMAKE([1.0 foreign])

AC_PROG_CC
AC_PROG_CXX
AC_PROG_LIBTOOL

# Check for pthreads (uses m4/acx_pthread.m4).
ACX_PTHREAD([], [AC_MSG_ERROR([failed to find pthreads])])

BOOST_REQUIRE([1.45])
BOOST_SYSTEM
BOOST_SERIALIZATION
BOOST_ASIO

SUBDIRS = src

#AC_CONFIG_FILES([Makefile 
#                 src/Makefile])
                
#[test -z "$CONFIG_HEADERS" || echo timestamp > stamp-h.in])
AC_OUTPUT
# @end 2
