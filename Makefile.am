## Makefile.am -- Process this file with automake to produce Makefile.in
AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS	 = -I m4

SUBDIRS = src

# add autogen on distribution
EXTRA_DIST = bootstrap

clean-local:
	rm -f usertable*
	rm -f master snapshot log
	rm -f perf.*
	rm -f gmon.*
