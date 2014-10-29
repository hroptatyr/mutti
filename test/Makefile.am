include $(top_builddir)/version.mk

## Help the Developers and yourself. Just use the C locale and settings
## for the compilation. They can still be overriden by make LANG=<whatever>
## but that is general a not very good idea
LANG = C
LC_ALL = C

AM_CFLAGS = $(EXTRA_CFLAGS)
AM_CPPFLAGS = -D_POSIX_C_SOURCE=200112L -D_XOPEN_SOURCE=600 -D_BSD_SOURCE
AM_CPPFLAGS += -DTEST
AM_CPPFLAGS += $(mutti_CFLAGS)
AM_LDFLAGS =
AM_LDFLAGS += $(mutti_LIBS)

EXTRA_DIST = $(BUILT_SOURCES) $(TESTS)
TESTS =
TEST_EXTENSIONS =
BUILT_SOURCES =
check_PROGRAMS =
CLEANFILES = $(check_PROGRAMS)

mutti_CFLAGS = -I$(top_srcdir)/src
mutti_LIBS = -L$(top_builddir)/src/ -lmutti

## summon our helper
include clitoris.am

TESTS += $(bin_tests)
bin_tests =

check_PROGRAMS += addgetrem_01
addgetrem_01_LDADD = $(mutti_LIBS)
bin_tests += addgetrem_01

## Makefile.am ends here
