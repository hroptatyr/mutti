/*** range.h -- some notion of intervals
 *
 * Copyright (C) 2012-2014 Sebastian Freundt
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
#if !defined INCLUDED_range_h_
#define INCLUDED_range_h_
#include <stdbool.h>
#include "instant.h"

typedef struct {
	echs_instant_t from;
	echs_instant_t till;
} echs_range_t;


/**
 * Sort an array IN of NIN elements stable and in-place. */
extern void echs_range_sort(echs_range_t *restrict in, size_t nin);

/**
 * Coalesce two ranges into one, or return the nul range if not possible. */
extern echs_range_t echs_range_coalesce(echs_range_t, echs_range_t);


#define ECHS_UNTIL_CHANGED	ECHS_END_OF_TIME
#define ECHS_FOREVER		ECHS_END_OF_TIME
#define ECHS_NUL_RANGE		((echs_range_t){ECHS_NUL_INSTANT, ECHS_NUL_INSTANT})
#define ECHS_ETERNAL_RANGE	((echs_range_t){ECHS_MIN_INSTANT, ECHS_MAX_INSTANT})

#define ECHS_FROM(x...)		((echs_range_t){{x}, ECHS_UNTIL_CHANGED})

static inline __attribute__((pure, const)) echs_range_t
echs_nul_range(void)
{
	return ECHS_NUL_RANGE;
}

static inline __attribute__((pure, const)) bool
echs_nul_range_p(echs_range_t r)
{
	return echs_nul_instant_p(r.from) && echs_nul_instant_p(r.till);
}

static inline __attribute__((pure, const)) bool
echs_empty_range_p(echs_range_t r)
{
	return echs_instant_lt_p(r.till, r.from) || echs_nul_range_p(r);
}

static inline __attribute__((pure, const)) echs_range_t
echs_eternal_range(void)
{
	return ECHS_ETERNAL_RANGE;
}

static inline __attribute__((pure, const)) bool
echs_eternal_range_p(echs_range_t r)
{
	return (r.till.u - r.from.u) == -1ULL;
}

/* simple interval relations, interval x instant */
static inline __attribute__((pure, const)) bool
echs_in_range_p(echs_range_t r, echs_instant_t t)
{
	return echs_instant_le_p(r.from, t) && echs_instant_lt_p(t, r.till);
}

static inline __attribute__((pure, const)) bool
echs_begins_range_p(echs_range_t r, echs_instant_t t)
{
	return echs_instant_eq_p(r.from, t);
}

static inline __attribute__((pure, const)) bool
echs_ends_range_p(echs_range_t r, echs_instant_t t)
{
	return echs_instant_eq_p(r.till, t);
}

static inline __attribute__((pure, const)) bool
echs_meets_range_p(echs_range_t r, echs_instant_t t)
{
	return echs_begins_range_p(r, t) || echs_ends_range_p(r, t);
}

static inline __attribute__((pure, const)) bool
echs_before_range_p(echs_range_t r, echs_instant_t t)
{
	return echs_instant_lt_p(t, r.from);
}

static inline __attribute__((pure, const)) bool
echs_after_range_p(echs_range_t r, echs_instant_t t)
{
	return echs_instant_le_p(r.till, t);
}

#endif	/* INCLUDED_range_h_ */
