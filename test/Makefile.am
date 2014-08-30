## Makefile.am -- Process this file with automake to produce Makefile.in

AM_CPPFLAGS = -I$(top_srcdir)/src/common -Wno-pointer-arith 
AM_LDFLAGS  = $(PTHREAD_CFLAGS)
LIBS = -lrt 

check_PROGRAMS = test_plist \
				 test_pbtree \
				 test_ptreap \
                 test_pmem  

test_pbtree_SOURCES = test_pbtree.cpp 
test_pbtree_LDADD = $(top_builddir)/src/libpm.a

test_plist_SOURCES = test_plist.cpp 
test_plist_LDADD = $(top_builddir)/src/libpm.a
 
test_ptreap_SOURCES = test_ptreap.cpp 
test_ptreap_LDADD = $(top_builddir)/src/libpm.a
 
test_pmem_SOURCES = test_pmem.cpp 
test_pmem_LDADD = $(top_builddir)/src/libpm.a

TESTS = $(check_PROGRAMS)

