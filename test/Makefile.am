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
AM_CLIT_LOG_FLAGS = --builddir $(builddir) --verbose

TESTS += $(bin_tests)
bin_tests =
TESTS += $(cli_tests)
cli_tests =

check_PROGRAMS += addgetrem_01
addgetrem_01_LDADD = $(mutti_LIBS)
cli_tests += addgetrem_01.clit

check_PROGRAMS += addgetrem_02
addgetrem_02_LDADD = $(mutti_LIBS)
cli_tests += addgetrem_02.clit

check_PROGRAMS += addgetrem_03
addgetrem_03_LDADD = $(mutti_LIBS)
cli_tests += addgetrem_03.clit

check_PROGRAMS += rtr_01
rtr_01_LDADD = $(mutti_LIBS)
cli_tests += rtr_01.clit

check_PROGRAMS += rtr_02
rtr_02_LDADD = $(mutti_LIBS)
cli_tests += rtr_02.clit

check_PROGRAMS += rtr_03
rtr_03_LDADD = $(mutti_LIBS)
cli_tests += rtr_03.clit

check_PROGRAMS += scan_01
scan_01_LDADD = $(mutti_LIBS)
cli_tests += scan_01.clit

check_PROGRAMS += scan_02
scan_02_LDADD = $(mutti_LIBS)
cli_tests += scan_02.clit

check_PROGRAMS += hist_01
hist_01_LDADD = $(mutti_LIBS)
cli_tests += hist_01.clit

## Makefile.am ends here
