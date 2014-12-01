/*** mutti.c -- convenience util around libmutti
 *
 * Copyright (C) 2014 Sebastian Freundt
 *
 * Author:  Sebastian Freundt <freundt@ga-group.nl>
 *
 * This file is part of mutti.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the author nor the names of any contributors
 *    may be used to endorse or promote products derived from this
 *    software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
 * IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 ***/
#if defined HAVE_CONFIG_H
# include "config.h"
#endif	/* HAVE_CONFIG_H */
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include "dt-strpf.h"
#include "bitte.h"
#include "nifty.h"


static __attribute__((format(printf, 1, 2))) void
error(const char *fmt, ...)
{
	va_list vap;
	va_start(vap, fmt);
	vfprintf(stderr, fmt, vap);
	va_end(vap);

	if (errno) {
		fputc(':', stderr);
		fputc(' ', stderr);
		fputs(strerror(errno), stderr);
	}
	fputc('\n', stderr);
	return;
}

static size_t
strf_range(char *restrict buf, size_t bsz, echs_range_t r)
{
	size_t res = 0U;

	buf[res++] = '[';
	if (echs_empty_range_p(r)) {
		goto out;
	}
	res += dt_strf(buf + res, bsz - res, r.from);
	buf[res++] = ',';
	if (!echs_end_of_time_p(r.till)) {
		res += dt_strf(buf + res, bsz - res, r.till);
	} else {
		memcpy(buf + res, "\u221e", sizeof("\u221e"));
		res += sizeof("\u221e") - 1U;
	}
out:
	buf[res++] = ')';
	buf[res] = '\0';
	return res;
}

static size_t
strf_bitmp(char *restrict buf, size_t bsz, echs_bitmp_t r)
{
	size_t res = 0U;

	res += strf_range(buf, bsz, r.valid);
	buf[res++] = 'x';
	res += strf_range(buf + res, bsz - res, r.trans);
	buf[res] = '\0';
	return res;
}

static int
_put1(mut_stor_t s, const char *fn)
{
	FILE *fp = stdin;
	char *line = NULL;
	size_t llen = 0UL;

	if (fn != NULL && (fp = fopen(fn, "r")) == NULL) {
		error("cannot read file `%s'", fn);
		return -1;
	}

	for (ssize_t nrd; (nrd = getline(&line, &llen, fp)) > 0;) {
		char *on;
		mut_oid_t x;
		echs_range_t r;

		if (!(x = strtoul(line, &on, 10)) || *on++ != '\t') {
			continue;
		}
		/* read from value */
		if (echs_nul_instant_p(r.from = dt_strp(on, &on))) {
			continue;
		}
		/* overread everything that isn't a digit */
		for (; (*on ^ '0') >= 10; on++);
		/* read till value */
		if (echs_nul_instant_p(r.till = dt_strp(on, &on))) {
			r.till = ECHS_FOREVER;
		}

		bitte_put(s, x, r);
	}

	if (line) {
		free(line);
	}
	return 0;
}


#include "mutti.yucc"

static int
cmd_put(const struct yuck_cmd_put_s argi[static 1U])
{
	static const char dfn[] = ".trans";
	const mut_stor_type_t dty = MUT_STOR_TYPE_DSK;
	mut_stor_t s;
	int rc = 0;

	if ((s = mut_stor_open(dty, dfn, O_CREAT | O_RDWR)) == NULL) {
		error("cannot open database file `%s'", dfn);
		return 1;
	}
	/* make sure we're using one timestamp for everything */
	echs_set_now(echs_now());

	for (size_t i = 0U; i < argi->nargs; i++) {
		rc -= _put1(s, argi->args[i]);
	}
	if (!argi->nargs) {
		rc -= _put1(s, NULL);
	}

	mut_stor_close(s);
	return rc;
}

static int
cmd_show(const struct yuck_cmd_show_s argi[static 1U])
{
	return 0;
}

int
main(int argc, char *argv[])
{
	yuck_t argi[1U] = {MUTTI_CMD_NONE};
	int rc = 0;

	if (yuck_parse(argi, argc, argv) < 0) {
		rc = 1;
		goto out;
	}

	switch (argi->cmd) {
	case MUTTI_CMD_PUT:
		rc = cmd_put((void*)argi);
		break;
	case MUTTI_CMD_SHOW:
		cmd_show((void*)argi);
		break;

	case MUTTI_CMD_NONE:
	default:
		break;
	}

out:
	yuck_free(argi);
	return rc;
}

/* mutti.c ends here */
