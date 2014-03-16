## Makefile.am -- Process this file with automake to produce Makefile.in

MAINTAINERCLEANFILES = Makefile.in

INCLUDES  = -Icommon

bin_PROGRAMS = server client
 
AM_CPPFLAGS = $(BOOST_CPPFLAGS)
AM_LDFLAGS = $(BOOST_SYSTEM_LDFLAGS) $(BOOST_SERIALIZATION_LDFLAGS) -lpthread 
LIBS = $(BOOST_SYSTEM_LIBS) $(BOOST_SERIALIZATION_LIBS)
 
server_SOURCES  =  server.cpp 
client_SOURCES  =  client.cpp

#server_LDFLAGS = -lpthread -lboost_system -lboost_serialization
#client_LDFLAGS = -lpthread -lboost_system -lboost_serialization
