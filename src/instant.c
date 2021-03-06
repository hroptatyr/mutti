/*** instant.c -- some echs_instant_t functionality
 *
 * Copyright (C) 2013-2014 Sebastian Freundt
 *
 * Author:  Sebastian Freundt <freundt@ga-group.nl>
 *
 * This file is part of echse.
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
#include <time.h>
#include <sys/time.h>
#include "instant.h"
#include "nifty.h"

static const unsigned int doy[] = {
	0U, 0U, 31U, 59U, 90U, 120U, 151U, 181U, 212U, 243U, 273U, 304U, 334U,
	365U, 396U, 424U, 455U, 485U, 516U, 546U, 577U, 608U, 638U, 669U, 699U,
};

/* all with regard to UTC, i.e. constant numbers of seconds in a day */
#define HOURS_PER_DAY	(24U)
#define MINS_PER_HOUR	(60U)
#define SECS_PER_MIN	(60U)
#define MSECS_PER_SEC	(1000U)
#define SECS_PER_DAY	(HOURS_PER_DAY * MINS_PER_HOUR * SECS_PER_MIN)
#define MSECS_PER_DAY	(SECS_PER_DAY * MSECS_PER_SEC)

static inline __attribute__((const, pure)) bool
__leapp(unsigned int y)
{
	return !(y % 4U);
}

static __attribute__((const, pure)) inline unsigned int
__get_ndom(unsigned int y, unsigned int m)
{
/* return the number of days in month M in year Y. */
	static const unsigned int mdays[] = {
		0U, 31U, 28U, 31U, 30U, 31U, 30U, 31U, 31U, 30U, 31U, 30U, 31U,
	};
	unsigned int res = mdays[m];

	if (UNLIKELY(__leapp(y) && m == 2U)) {
		res++;
	}
	return res;
}

static inline unsigned int
__doy(echs_instant_t i)
{
	unsigned int res = doy[i.m] + i.d;

	if (UNLIKELY((i.y % 4U) == 0) && i.m >= 3) {
		res++;
	}
	return res;
}

static echs_instant_t
__instant_gmtime(const time_t t)
{
	static uint16_t __mon_yday[] = {
		/* this is \sum ml,
		 * first element is a bit set of leap days to add */
		0xfff8, 0,
		31, 59, 90, 120, 151, 181,
		212, 243, 273, 304, 334, 365
	};
	register int days;
	register unsigned int yy;
	const uint16_t *ip;
	echs_instant_t res;

	/* just go to day computation */
	days = (int)(t / SECS_PER_DAY);

	/* gotta do the date now */
	yy = 1970;
	/* stolen from libc */
#define DIV(a, b)		((a) / (b))
/* we only care about 1901 to 2099 and there are no bullshit leap years */
#define LEAPS_TILL(y)		(DIV(y, 4))
	while (days < 0 || days >= (!__leapp(yy) ? 365 : 366)) {
		/* Guess a corrected year, assuming 365 days per year. */
		register unsigned int yg = yy + days / 365 - (days % 365 < 0);

		/* Adjust DAYS and Y to match the guessed year.  */
		days -= (yg - yy) * 365 +
			LEAPS_TILL(yg - 1) - LEAPS_TILL(yy - 1);
		yy = yg;
	}
	/* set the year */
	res.y = (int)yy;

	ip = __mon_yday;
	/* unrolled */
	yy = 13;
	if (days < ip[--yy] &&
	    days < ip[--yy] &&
	    days < ip[--yy] &&
	    days < ip[--yy] &&
	    days < ip[--yy] &&
	    days < ip[--yy] &&
	    days < ip[--yy] &&
	    days < ip[--yy] &&
	    days < ip[--yy] &&
	    days < ip[--yy] &&
	    days < ip[--yy]) {
		yy = 1;
	}
	/* set the rest of the tm structure */
	res.m = yy;
	res.d = days - ip[yy] + 1;
	/* fix up leap years */
	if (UNLIKELY(__leapp(res.y))) {
		if ((ip[0] >> (yy)) & 1) {
			if (UNLIKELY(days == 59)) {
				res.m = 2;
				res.d = 29;
			} else if (UNLIKELY(days == ip[yy])) {
				res.d = days - ip[--res.m];
			} else {
				res.d--;
			}
		}
	}
	with (unsigned int S = t % SECS_PER_DAY) {
		res.ms = 0U;
		res.S = S % SECS_PER_MIN;
		S /= SECS_PER_MIN;
		res.M = S % MINS_PER_HOUR;
		S /= MINS_PER_HOUR;
		res.H = S;
	}
	return res;
}


/* public API */
echs_instant_t
echs_instant_fixup(echs_instant_t e)
{
/* this is basically __ymd_fixup_d of dateutils
 * we only care about additive cockups though because instants are
 * chronologically ascending */
	unsigned int md;

	if (UNLIKELY(echs_instant_all_day_p(e))) {
		/* just fix up the day, dom and year portion */
		goto fixup_d;
	} else if (UNLIKELY(echs_instant_all_sec_p(e))) {
		/* just fix up the sec, min, ... portions */
		goto fixup_S;
	}

	if (UNLIKELY(e.ms >= MSECS_PER_SEC)) {
		unsigned int dS = e.ms / MSECS_PER_SEC;
		unsigned int ms = e.ms % MSECS_PER_SEC;

		e.ms = ms;
		e.S += dS;
	}

fixup_S:
	if (UNLIKELY(e.S >= SECS_PER_MIN)) {
		/* leap seconds? */
		unsigned int dM = e.S / SECS_PER_MIN;
		unsigned int S = e.S % SECS_PER_MIN;

		e.S = S;
		e.M += dM;
	}
	if (UNLIKELY(e.M >= MINS_PER_HOUR)) {
		unsigned int dH = e.M / MINS_PER_HOUR;
		unsigned int M = e.M % MINS_PER_HOUR;

		e.M = M;
		e.H += dH;
	}
	if (UNLIKELY(e.H >= HOURS_PER_DAY)) {
		unsigned int dd = e.H / HOURS_PER_DAY;
		unsigned int H = e.H % HOURS_PER_DAY;

		e.H = H;
		e.d += dd;
	}

fixup_d:
refix_ym:
	if (UNLIKELY(e.m > 12U)) {
		unsigned int dy = (e.m - 1) / 12U;
		unsigned int m = (e.m - 1) % 12U + 1U;

		e.m = m;
		e.y += dy;
	}

	if (UNLIKELY(e.d > (md = __get_ndom(e.y, e.m)))) {
		e.d -= md;
		e.m++;
		goto refix_ym;
	}
	return e;
}

echs_idiff_t
echs_instant_diff(echs_instant_t end, echs_instant_t beg)
{
	int extra_df;
	int intra_df;

	/* just see what the intraday part yields for the difference */
	intra_df = end.H - beg.H;
	intra_df *= MINS_PER_HOUR;
	intra_df += end.M - beg.M;
	intra_df *= SECS_PER_MIN;
	intra_df += end.S - beg.S;
	intra_df *= MSECS_PER_SEC;
	intra_df += end.ms - beg.ms;

	if (intra_df < 0) {
		intra_df += MSECS_PER_DAY;
		extra_df = -1;
	} else if ((unsigned int)intra_df < MSECS_PER_DAY) {
		extra_df = 0;
	} else {
		extra_df = 1;
	}

	{
		unsigned int dom_end = __doy(end);
		unsigned int dom_beg = __doy(beg);
		int df_y = end.y - beg.y;

		extra_df += dom_end - dom_beg;
		if (echs_instant_lt_p(beg, end) && extra_df < 0) {
			df_y--;
		}
		extra_df += df_y * 365 + (df_y - 1) / 4;
	}

	return (echs_idiff_t){extra_df, intra_df};
}

echs_instant_t
echs_instant_add(echs_instant_t bas, echs_idiff_t add)
{
	echs_instant_t res = echs_nul_instant();
	int dd = add.dd;
	int msd = add.msd;
	int df_y;
	int df_m;
	int y;
	int m;
	int d;

	if (UNLIKELY(echs_instant_all_day_p(bas))) {
		/* just fix up the day, dom and year portion */
		res.H = ECHS_ALL_DAY;
		goto fixup_d;
	} else if (UNLIKELY(echs_instant_all_sec_p(bas))) {
		/* just fix up the sec, min, ... portions */
		res.ms = ECHS_ALL_SEC;
		goto fixup_S;
	}

	res.ms = bas.ms + msd % MSECS_PER_SEC;
	msd /= MSECS_PER_SEC;
fixup_S:
	res.S = bas.S + msd % SECS_PER_MIN;
	msd /= SECS_PER_MIN;
	res.M = bas.M + msd % MINS_PER_HOUR;
	msd /= MINS_PER_HOUR;
	res.H = bas.H + msd % HOURS_PER_DAY;
	msd /= HOURS_PER_DAY;
	/* get ready for the end-of-day bit */
	dd += msd;

fixup_d:
	y = bas.y + dd / 365;
	if ((df_y = y - bas.y)) {
		dd -= df_y * 365 + (df_y - 1) / 4;
	}

	m = bas.m + dd / 31;
	if ((df_m = m - bas.m)) {
		dd -= doy[bas.m + df_m] - doy[bas.m + 1];
	}

	d = bas.d + dd;
	while (d <= 0) {
		while (--m <= 0) {
			y--;
			m = 12U;
		}
		d += __get_ndom(y, m);
	}
	while ((unsigned int)d > __get_ndom(y, m)) {
		d -= __get_ndom(y, m);
		while (++m > 12) {
			y++;
			m = 1U;
		}
	}

	res.d = d;
	res.m = m;
	res.y = y;
	return res;
}


/* metronome/system time */
static echs_instant_t _echs_now;

echs_instant_t
echs_now(void)
{
	struct timeval tv;
	echs_instant_t res;

	if (!echs_nul_instant_p(_echs_now)) {
		return _echs_now;
	} else if (UNLIKELY(gettimeofday(&tv, NULL) < 0)) {
		return ECHS_NUL_INSTANT;
	}
	res = __instant_gmtime(tv.tv_sec);
	res.ms = tv.tv_usec / 1000;
	return res;
}

void
echs_set_now(echs_instant_t i)
{
	/* guarantee monotonicity */
	if (UNLIKELY(echs_nul_instant_p(i)) ||
	    LIKELY(echs_instant_lt_p(_echs_now, i))) {
		_echs_now = i;
	}
	return;
}

/* instant.c ends here */
