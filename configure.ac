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

AC_HEADER_STDC
AC_HEADER_SYS_WAIT
AC_CHECK_HEADERS([errno.h stddef.h stdlib.h string.h strings.h unistd.h])

AC_C_CONST
AC_TYPE_PID_T
AC_TYPE_SIZE_T
AC_C_VOLATILE

AC_CHECK_FUNCS([memcpy memset], [break])
AC_CHECK_FUNCS([calloc strchr strrchr])
AC_REPLACE_FUNCS([basename strspn strcspn strerror strsignal waitpid])

save_LIBS="$LIBS"
test x"${with_readline-no}" != xno && LIBS="$LIBS -lreadline"
AC_REPLACE_FUNCS([readline])
AC_CHECK_FUNCS([add_history])
LIBS="$save_LIBS"

AC_CONFIG_FILES([Makefile 
                 src/Makefile],
                [test -z "$CONFIG_HEADERS" || echo timestamp > stamp-h.in])
AC_OUTPUT
# @end 2
