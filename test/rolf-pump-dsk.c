/** simple addgetrem test */
#if defined HAVE_CONFIG_H
# include "config.h"
#endif	/* HAVE_CONFIG_H */
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include "dt-strpf.h"
#include "bitte.h"
#include "nifty.h"


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
pr_bitmp(echs_bitmp_t r)
{
	char buf[256U];

	strf_bitmp(buf, sizeof(buf), r);
	return puts(buf);
}


int
main(int argc, char *const argv[])
{
	mut_stor_t s = mut_stor_open(MUT_STOR_TYPE_DSK, ".trans", O_CREAT | O_TRUNC | O_RDWR);
	char *line = NULL;
	size_t llen = 0UL;

	echs_set_now(dt_strp("2014-10-29T14:23:41.507", NULL));

	assert(s != NULL);
	for (ssize_t nrd; (nrd = getline(&line, &llen, stdin)) > 0;) {
		char *on;
		mut_oid_t x;
		echs_range_t r;

		if (!(x = strtoul(line, &on, 10)) || *on++ != '\t') {
			continue;
		}
		/* read range */
		if (echs_nul_instant_p(r.from = dt_strp(on, &on))) {
			continue;
		}
		for (; *on == ' ' || *on == '-'; *on++);
		if (echs_nul_instant_p(r.till = dt_strp(on, &on))) {
			continue;
		}

		bitte_put(s, x, r);
	}

	echs_instant_t this = dt_strp("2014-10-29T14:25:41.507", NULL);
	echs_set_now(dt_strp("2014-10-29T14:29:41.507", NULL));

	/* try a simple get */
	for (int i = 1; i < argc; i++) {
		mut_oid_t x;

		if ((x = strtoul(argv[i], NULL, 0))) {
			fprintf(stdout, "fact %lu\t", x);
			fflush(stdout);
			pr_bitmp(bitte_get(s, x, this));
		}
	}

	mut_stor_close(s);
	if (line) {
		free(line);
	}
	return 0;
}

/* rolf-pump.c ends here */
