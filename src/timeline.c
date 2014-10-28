/*** timeline.c -- reverse timeline data structure
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
#include <sys/mman.h>
#include <string.h>
#include "timeline.h"
#include "nifty.h"

/* at the moment we operate on 4k block sizes */
#define BLKZ	(4096U)
/* number of transaction stamps per block */
#define NTPB	(BLKZ / sizeof(*trans))

#if !defined MAP_ANON && defined MAP_ANONYMOUS
# define MAP_ANON	MAP_ANONYMOUS
#elif !defined MAP_ANON
# define MAP_ANON	(0x1000U)
#endif	/* !MAP_ANON */
#define PROT_RW		(PROT_READ | PROT_WRITE)
#define MAP_MEM		(MAP_PRIVATE | MAP_ANON)

struct ioff_s {
	size_t nitems;
	mut_oid_t *items;
	size_t *offs;
};

static size_t ntrans;
static echs_instant_t *trans;
static mut_oid_t *items;
static echs_range_t *valids;
static struct ioff_s live;
static struct ioff_s dead;

#define ITEM_NOT_FOUND		((size_t)-1)
#define ITEM_NOT_FOUND_P(x)	(!~(size_t)(x))


static inline int
xfree(void *restrict p, size_t z)
{
	return munmap(p, z);
}

static inline void*
xralloc(void *restrict p_old, size_t z_old, size_t z_new)
{
/* Like realloc() but memmove the old stuff to the end of the array,
 * i.e. alloc (z_new - z_old) new bytes at the beginning.
 * Unlike realloc() this can't downsize, z_new >= z_old is implied. */
	void *p_new = mmap(p_old, z_new, PROT_RW, MAP_MEM, -1, 0);

	if (UNLIKELY(p_new == MAP_FAILED)) {
		return NULL;
	} else if (LIKELY(p_old != NULL)) {
		memcpy((char*)p_new + (z_new - z_old), p_old, z_old);
		/* munmap the old guy */
		xfree(p_old, z_old);
	}
	return p_new;
}

static inline void*
xzralloc(void *restrict p_old, size_t n_old, size_t n_new, size_t z_memb)
{
	return xralloc(p_old, n_old * z_memb, n_new * z_memb);
}

static inline void*
xfalloc(void *restrict p_old, size_t z_old, size_t z_new)
{
/* Like realloc().
 * Unlike realloc() this can't downsize, z_new >= z_old is implied. */
	void *p_new = mmap(p_old, z_new, PROT_RW, MAP_MEM, -1, 0);

	if (UNLIKELY(p_new == MAP_FAILED)) {
		return NULL;
	} else if (LIKELY(p_old != NULL)) {
		memcpy(p_new, p_old, z_old);
		/* munmap the old guy */
		xfree(p_old, z_old);
	}
	return p_new;
}

static inline void*
xzfalloc(void *restrict p_old, size_t n_old, size_t n_new, size_t z_memb)
{
	return xfalloc(p_old, n_old * z_memb, n_new * z_memb);
}


static size_t
_get_ioff(struct ioff_s v, mut_oid_t item)
{
	size_t i_fr = ITEM_NOT_FOUND;
	size_t i;

	for (i = 0U; i < v.nitems; i++) {
		if (v.items[i] == MUT_NUL_OID) {
			i_fr = i;
			break;
		} else if (v.items[i] == item) {
			return i;
		}
	}
	for (; i < v.nitems; i++) {
		if (v.items[i] == item) {
			return i;
		}
	}
	return i_fr;
}

static int
_add_ioff(struct ioff_s *tgt, struct ioff_s *src, mut_oid_t item, size_t last)
{
	size_t li, di;

	li = _get_ioff(*tgt, item);
	if (!ITEM_NOT_FOUND_P(li) && tgt->items[li] != MUT_NUL_OID) {
		/* just update him then */
		goto up_and_out;
	}
	/* otherwise try dead data */
	di = _get_ioff(*src, item);
	if (!ITEM_NOT_FOUND_P(di) && src->items[di] != MUT_NUL_OID) {
		/* found him in the cemetery, resurrect */
		src->items[di] = MUT_NUL_OID;
		if (!ITEM_NOT_FOUND_P(li)) {
			goto ass_up_and_out;
		}
	}
	/* we'll have to extend the list of live items */
	if (UNLIKELY((tgt->nitems % 512U) == 0U)) {
		const size_t ol = tgt->nitems;
		const size_t nu = ol + 512U;
		void *pi = xzfalloc(tgt->items, ol, nu, sizeof(*tgt->items));
		void *po = xzfalloc(tgt->offs, ol, nu, sizeof(*tgt->offs));

		if (UNLIKELY(pi == NULL || po == NULL)) {
			/* brill */
			return -1;
		}
		tgt->items = pi;
		tgt->offs = po;

		/* just ass our item */
		li = tgt->nitems++;
	}
ass_up_and_out:
	tgt->items[li] = item;
up_and_out:
	tgt->offs[li] = last;
	return 0;
}


int
bitte_add(mut_oid_t item, echs_range_t valid)
{
	if (UNLIKELY(!(ntrans % NTPB))) {
		const size_t znu = ntrans + NTPB;
		void *nu_t, *nu_i, *nu_v;

		nu_t = xzralloc(trans, ntrans, znu, sizeof(*trans));
		nu_i = xzralloc(items, ntrans, znu, sizeof(*items));
		nu_v = xzralloc(valids, ntrans, znu, sizeof(*valids));

		if (UNLIKELY(nu_t == NULL || nu_i == NULL || nu_v == NULL)) {
			/* try proper munmapping? */
			return -1;
		}
		/* otherwise reassign */
		trans = nu_t;
		items = nu_i;
		valids = nu_v;
	}
	/* stamp off then */
	with (echs_instant_t t = echs_now()) {
		const size_t hi = ((ntrans / NTPB) + 1U) * NTPB;
		const size_t it = ntrans++;
		/* convert to real index */
		const size_t ri = hi - it - 1U;

		trans[ri] = t;
		items[ri] = item;
		valids[ri] = valid;

		/* insert into live data if VALID is open-ended */
		if (LIKELY(echs_end_of_time_p(valid.till))) {
			_add_ioff(&live, &dead, item, it);
		}
		/* ... into dead data otherwise */
		else {
			_add_ioff(&dead, &live, item, it);
		}
	}
	return 0;
}

int
bitte_rem(mut_oid_t item)
{
	const size_t i = _get_ioff(live, item);

	if (ITEM_NOT_FOUND_P(i) || live.items[i] == MUT_NUL_OID) {
		/* he's dead already */
		return -1;
	}

	/* otherwise kill him */
	if (UNLIKELY(!(ntrans % NTPB))) {
		const size_t znu = ntrans + NTPB;
		void *nu_t, *nu_i, *nu_v;

		nu_t = xzralloc(trans, ntrans, znu, sizeof(*trans));
		nu_i = xzralloc(items, ntrans, znu, sizeof(*items));
		nu_v = xzralloc(valids, ntrans, znu, sizeof(*valids));

		if (UNLIKELY(nu_t == NULL || nu_i == NULL || nu_v == NULL)) {
			/* try proper munmapping? */
			return -1;
		}
		/* otherwise reassign */
		trans = nu_t;
		items = nu_i;
		valids = nu_v;
	}
	/* stamp him off */
	with (echs_instant_t t = echs_now()) {
		const size_t hi = ((ntrans / NTPB) + 1U) * NTPB;
		const size_t it = ntrans++;
		/* convert to real index */
		const size_t ri = hi - it - 1U;
		const size_t prev_ri = hi - live.offs[i] - 1U;

		trans[ri] = t;
		items[ri] = item;
		valids[ri] = valids[prev_ri];

		_add_ioff(&dead, &live, item, it);
	}
	return 0;
}

echs_bitmp_t
bitte_get(mut_oid_t item, echs_instant_t as_of)
{
	const size_t hi = ((ntrans / NTPB) + 1U) * NTPB;
	size_t i;

	i = _get_ioff(live, item);
	if (!ITEM_NOT_FOUND_P(i) && live.items[i] != MUT_NUL_OID) {
		/* yay */
		const size_t ri = hi - live.offs[i] - 1U;
		return (echs_bitmp_t){
			valids[ri],
			(echs_range_t){trans[ri], ECHS_UNTIL_CHANGED}
		};
	}

	i = _get_ioff(dead, item);
	if (!ITEM_NOT_FOUND_P(i) && dead.items[i] != MUT_NUL_OID) {
		/* got a dead one */
		const size_t ri = hi - dead.offs[i] - 1U;
		size_t pi;

		/* find previous version */
		for (pi = ri + 1U; pi < ntrans; pi++) {
			if (items[pi] == item) {
				break;
			}
		}
		return (echs_bitmp_t){
			valids[ri],
			(echs_range_t){trans[pi], trans[ri]}
		};
	}

	return (echs_bitmp_t){
		ECHS_EMPTY_RANGE,
		(echs_range_t){trans[hi - ntrans], ECHS_UNTIL_CHANGED}
	};
}

echs_range_t
bitte_valid(mut_oid_t item, echs_instant_t as_of)
{
	return bitte_get(item, as_of).valid;
}

echs_range_t
bitte_trans(mut_oid_t item, echs_instant_t as_of)
{
	return bitte_get(item, as_of).trans;
}

/* timeline.c ends here */
