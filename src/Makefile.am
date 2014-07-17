## Makefile.am -- Process this file with automake to produce Makefile.in

bin_PROGRAMS = nstore pmem_check

AM_CPPFLAGS = -I$(srcdir)/common -Wno-pointer-arith 
AM_LDFLAGS  = $(PTHREAD_CFLAGS)
LIBS = -lrt 

noinst_LIBRARIES = libpm.a
libpm_a_SOURCES = libpm.cpp 

#AM_CPPFLAGS = $(BOOST_CPPFLAGS) 
#AM_LDFLAGS = $(BOOST_SYSTEM_LDFLAGS) $(BOOST_THREAD_LDFLAGS) $(PTHREAD_CFLAGS)
#LIBS = $(BOOST_SYSTEM_LIBS) $(BOOST_THREAD_LIBS) 

nstore_SOURCES  =  	main.cpp \
					wal_engine.cpp  \
					aries_engine.cpp  \
					sp_engine.cpp  \
					cow_engine.cpp  \
					ycsb_benchmark.cpp \
					utils.cpp

nstore_LDADD  = libpm.a

pmem_check_SOURCES = pmem_check.cpp

pmem_check_LDADD  = libpm.a


