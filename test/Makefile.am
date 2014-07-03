## Makefile.am -- Process this file with automake to produce Makefile.in

AM_CPPFLAGS = -I$(top_builddir)/src/common -Wno-pointer-arith 
AM_LDFLAGS  = $(PTHREAD_CFLAGS)
LIBS = -lrt 

check_PROGRAMS = test_plist \
				 test_ptree \
				 test_wal

test_ptree_SOURCES = test_ptree.cpp 
test_ptree_LDADD = $(top_builddir)/src/libpm.a

test_plist_SOURCES = test_plist.cpp 
test_plist_LDADD = $(top_builddir)/src/libpm.a

test_wal_SOURCES = test_wal.cpp \
				$(top_builddir)/src/wal_engine.cpp \ 
				$(top_builddir)/src/ycsb_benchmark.cpp \
				$(top_builddir)/src/utils.cpp 

test_wal_LDADD = $(top_builddir)/src/libpm.a


TESTS = $(check_PROGRAMS)

clean-local:
	rm -f testfile
