## Makefile.am -- Process this file with automake to produce Makefile.in

bin_PROGRAMS = nstore

AM_CPPFLAGS = -I$(srcdir)/common 
AM_LDFLAGS  = $(PTHREAD_CFLAGS)
LIBS = 

#AM_CPPFLAGS = $(BOOST_CPPFLAGS) 
#AM_LDFLAGS = $(BOOST_SYSTEM_LDFLAGS) $(BOOST_THREAD_LDFLAGS) $(PTHREAD_CFLAGS)
#LIBS = $(BOOST_SYSTEM_LIBS) $(BOOST_THREAD_LIBS) 

nstore_SOURCES  =  	main.cpp \
			wal.cpp  \
			sp.cpp   \
			utils.cpp

