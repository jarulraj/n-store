## Makefile.am -- Process this file with automake to produce Makefile.in

MAINTAINERCLEANFILES = Makefile.in

INCLUDES  = -I$(srcdir)/common

bin_PROGRAMS = 
 
AM_CPPFLAGS = $(BOOST_CPPFLAGS)
AM_LDFLAGS = $(BOOST_SYSTEM_LDFLAGS) $(BOOST_SERIALIZATION_LDFLAGS)  
LIBS = $(BOOST_SYSTEM_LIBS) $(BOOST_SERIALIZATION_LIBS) $(PTHREAD_CFLAGS)
 
server_SOURCES  =  server.cpp 
client_SOURCES  =  client.cpp
