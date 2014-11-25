### Makefile.am
include $(top_builddir)/version.mk

AM_CFLAGS = $(EXTRA_CFLAGS)
AM_CPPFLAGS = -D_POSIX_C_SOURCE=201001L -D_XOPEN_SOURCE=700 -D_BSD_SOURCE
AM_LDFLAGS = $(XCCLDFLAGS)

bin_PROGRAMS =
noinst_PROGRAMS =
lib_LTLIBRARIES =
noinst_LTLIBRARIES =
pkglib_LTLIBRARIES =
pkginclude_HEADERS =
noinst_HEADERS =
BUILT_SOURCES =
EXTRA_DIST = $(BUILT_SOURCES)
CLEANFILES = 
SUFFIXES =

CLEANFILES += version.c
EXTRA_DIST += version.c.in


lib_LTLIBRARIES += libmutti.la
libmutti_la_SOURCES =
EXTRA_libmutti_la_SOURCES =
libmutti_la_SOURCES += instant.c instant.h
libmutti_la_SOURCES += range.c range.h
libmutti_la_SOURCES += dt-strpf.c dt-strpf.h
libmutti_la_SOURCES += bitte.c bitte.h bitte-private.h
libmutti_la_SOURCES += bitte-mem.c
libmutti_la_SOURCES += bitte-bdb.c
libmutti_la_SOURCES += bitte-dsk.c
libmutti_la_SOURCES += nifty.h
libmutti_la_LDFLAGS = $(AM_LDFLAGS)
libmutti_la_LDFLAGS += -ldb
libmutti_la_LDFLAGS += -lm
EXTRA_libmutti_la_SOURCES += wikisort.c
EXTRA_libmutti_la_SOURCES += rb.c rb.h
BUILT_SOURCES += rb-fact.c
BUILT_SOURCES += rb-instant.c

pkginclude_HEADERS += instant.h
pkginclude_HEADERS += range.h
pkginclude_HEADERS += bitte.h
pkginclude_HEADERS += dt-strpf.h

bin_PROGRAMS += mutti
mutti_LDADD = libmutti.la
BUILT_SOURCES += mutti.yucc


## version rules
version.c: version.c.in $(top_builddir)/.version
	$(AM_V_GEN) PATH="$(top_builddir)/build-aux:$${PATH}" \
		yuck$(EXEEXT) scmver --ignore-noscm --force -o $@ \
			--use-reference --reference $(top_builddir)/.version $<

## yuck rule
SUFFIXES += .yuck
SUFFIXES += .yucc
.yuck.yucc:
	$(AM_V_GEN) PATH=$(top_builddir)/build-aux:"$${PATH}" \
		yuck$(EXEEXT) gen -o $@ $<

rb-fact.c: rb.c
	$(M4) -DRBKEY_T=mut_oid_t $< > $@

rb-instant.c: rb.c
	$(M4) -DRBKEY_T=echs_instant_t $< > $@

## Create preprocessor output (debugging purposes only)
.c.i:
	$(COMPILE) -E -o $@ $<

## Create assembler output (debugging purposes only)
.c.s:
	$(COMPILE) -S -c $(AM_CFLAGS) $<
