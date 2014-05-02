## Makefile.am -- Process this file with automake to produce Makefile.in

 
INCLUDES  = -I$(srcdir)/common

bin_PROGRAMS = wal sp

AM_CPPFLAGS = $(BOOST_CPPFLAGS) 
AM_LDFLAGS = $(BOOST_SYSTEM_LDFLAGS) $(BOOST_THREAD_LDFLAGS) $(PTHREAD_CFLAGS)

LIBS = $(BOOST_SYSTEM_LIBS) $(BOOST_THREAD_LIBS) 
 
wal_SOURCES  =  wal.cpp 
sp_SOURCES   =  sp.cpp 
