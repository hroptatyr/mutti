/*** instant.h -- some instant notion
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
#if !defined INCLUDED_instant_h_
#define INCLUDED_instant_h_
#if defined HAVE_CONFIG_H
# include "config.h"
#endif	/* HAVE_CONFIG_H */
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

typedef union {
	struct {
#if defined WORDS_BIGENDIAN
		uint32_t y:16;
		uint32_t m:8;
		uint32_t d:8;
		uint32_t H:8;
		uint32_t M:8;
		uint32_t S:6;
		uint32_t ms:10;
#else  /* !WORDS_BIGENDIAN */
		uint32_t ms:10;
		uint32_t S:6;
		uint32_t M:8;
		uint32_t H:8;
		uint32_t d:8;
		uint32_t m:8;
		uint32_t y:16;
#endif	/* WORDS_BIGENDIAN */
	};
	struct {
#if defined WORDS_BIGENDIAN
		uint32_t dpart;
		uint32_t intra;
#else  /* !WORDS_BIGENDIAN */
		uint32_t intra;
		uint32_t dpart;
#endif	/* WORDS_BIGENDIAN */
	};
	uint64_t u;
} __attribute__((transparent_union)) echs_instant_t;

typedef struct {
	signed int dd;
	unsigned int msd;
} echs_idiff_t;


/**
 * Fix up instants like the 32 Dec to become 01 Jan of the following year. */
extern echs_instant_t echs_instant_fixup(echs_instant_t);

extern echs_idiff_t echs_instant_diff(echs_instant_t end, echs_instant_t beg);

extern echs_instant_t echs_instant_add(echs_instant_t bas, echs_idiff_t add);

/**
 * Sort an array IN of NIN elements stable and in-place. */
extern void echs_instant_sort(echs_instant_t *restrict in, size_t nin);

/**
 * Return current system time as echs_instant_t. */
extern echs_instant_t echs_now(void);


#define ECHS_ALL_DAY	(0xffU)
#define ECHS_ALL_SEC	(0x3ffU)
#define ECHS_NUL_INSTANT	((echs_instant_t){.u = 0ULL})
#define ECHS_END_OF_TIME	((echs_instant_t){.u = -1ULL})
#define ECHS_MIN_INSTANT	ECHS_NUL_INSTANT
#define ECHS_MAX_INSTANT	ECHS_END_OF_TIME
#define ECHS_NOW		(echs_now())

static inline __attribute__((const, pure)) bool
echs_instant_all_day_p(echs_instant_t i)
{
	return i.H == ECHS_ALL_DAY;
}

static inline __attribute__((const, pure)) bool
echs_instant_all_sec_p(echs_instant_t i)
{
	return i.ms == ECHS_ALL_SEC;
}

static inline __attribute__((const, pure)) bool
echs_instant_lt_p(echs_instant_t x, echs_instant_t y)
{
	x.H++, x.ms++;
	y.H++, y.ms++;
	return x.u < y.u;
}

static inline __attribute__((const, pure)) bool
echs_instant_le_p(echs_instant_t x, echs_instant_t y)
{
	x.H++, x.ms++;
	y.H++, y.ms++;
	return !(x.u > y.u);
}

static inline __attribute__((const, pure)) bool
echs_instant_eq_p(echs_instant_t x, echs_instant_t y)
{
	return x.u == y.u;
}

static inline __attribute__((const, pure)) echs_instant_t
echs_nul_instant(void)
{
	return ECHS_NUL_INSTANT;
}

static inline __attribute__((const, pure)) bool
echs_nul_instant_p(echs_instant_t x)
{
	return x.u == 0ULL;
}

static inline __attribute__((const, pure)) echs_instant_t
echs_end_of_time(void)
{
	return ECHS_END_OF_TIME;
}

static inline __attribute__((const, pure)) bool
echs_end_of_time_p(echs_instant_t x)
{
	return x.u == -1ULL;
}

static inline __attribute__((const, pure)) echs_instant_t
echs_min_instant(void)
{
	return echs_nul_instant();
}

static inline __attribute__((const, pure)) bool
echs_min_instant_p(echs_instant_t x)
{
	return echs_nul_instant_p(x);
}

static inline __attribute__((const, pure)) echs_instant_t
echs_max_instant(void)
{
	return echs_end_of_time();
}

static inline __attribute__((const, pure)) bool
echs_max_instant_p(echs_instant_t x)
{
	return echs_end_of_time_p(x);
}


#define ECHS_NUL_IDIFF	((echs_idiff_t){0U, 0U})

static inline __attribute__((const, pure)) echs_idiff_t
echs_nul_idiff(void)
{
	return ECHS_NUL_IDIFF;
}

static inline __attribute__((const, pure)) bool
echs_nul_idiff_p(echs_idiff_t x)
{
	return x.dd == 0U && x.msd == 0U;
}

static inline __attribute__((const, pure)) bool
echs_idiff_lt_p(echs_idiff_t i1, echs_idiff_t i2)
{
	return i1.dd < i2.dd || (i1.dd == i2.dd && i1.msd < i2.msd);
}

static inline __attribute__((const, pure)) bool
echs_idiff_le_p(echs_idiff_t i1, echs_idiff_t i2)
{
	return i1.dd <= i2.dd || (i1.dd == i2.dd && i1.msd <= i2.msd);
}

static inline __attribute__((const, pure)) bool
echs_idiff_eq_p(echs_idiff_t i1, echs_idiff_t i2)
{
	return i1.dd == i2.dd && i1.msd == i2.msd;
}

static inline __attribute__((const, pure)) echs_idiff_t
echs_idiff_neg(echs_idiff_t i)
{
	return (echs_idiff_t){-i.dd + (i.msd != 0), i.msd};
}

#endif	/* INCLUDED_instant_h_ */
