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
	size_t nfacts;
	mut_oid_t *facts;
	size_t *offs;
};

static size_t ntrans;
static echs_instant_t *trans;
static mut_oid_t *facts;
static echs_range_t *valids;
static struct ioff_s live;

#define FACT_NOT_FOUND		((size_t)-1)
#define FACT_NOT_FOUND_P(x)	(!~(size_t)(x))

#define ECHS_RANGE_FROM(x)	((echs_range_t){x, ECHS_UNTIL_CHANGED})


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
_get_ioff(struct ioff_s v, mut_oid_t fact)
{
	size_t i_fr = FACT_NOT_FOUND;
	size_t i;

	for (i = 0U; i < v.nfacts; i++) {
		if (v.facts[i] == MUT_NUL_OID) {
			i_fr = i;
			break;
		} else if (v.facts[i] == fact) {
			return i;
		}
	}
	for (; i < v.nfacts; i++) {
		if (v.facts[i] == fact) {
			return i;
		}
	}
	return i_fr;
}

static int
_put_ioff(struct ioff_s *tgt, mut_oid_t fact, size_t last)
{
	size_t i;

	i = _get_ioff(*tgt, fact);
	if (!FACT_NOT_FOUND_P(i) && tgt->facts[i] != MUT_NUL_OID) {
		/* just update him then */
		goto up_and_out;
	}
	/* we'll have to extend the list of live facts */
	if (UNLIKELY((tgt->nfacts % NTPB) == 0U)) {
		const size_t ol = tgt->nfacts;
		const size_t nu = ol + NTPB;
		void *pi = xzfalloc(tgt->facts, ol, nu, sizeof(*tgt->facts));
		void *po = xzfalloc(tgt->offs, ol, nu, sizeof(*tgt->offs));

		if (UNLIKELY(pi == NULL || po == NULL)) {
			/* brill */
			return -1;
		}
		tgt->facts = pi;
		tgt->offs = po;
	}
	/* just ass our fact */
	i = tgt->nfacts++;
	tgt->facts[i] = fact;
up_and_out:
	tgt->offs[i] = last;
	return 0;
}


/* meta */
static echs_bitmp_t
_bitte_get_as_of_now(mut_oid_t fact)
{
	size_t i;

	i = _get_ioff(live, fact);
	if (FACT_NOT_FOUND_P(i) || live.facts[i] == MUT_NUL_OID) {
		/* must be dead */
		return ECHS_NUL_BITMP;
	}
	/* yay, dead or alive, it's in our books */
	i = live.offs[i];
	return (echs_bitmp_t){
		valids[i],
		(echs_range_t){trans[i], ECHS_UNTIL_CHANGED}
	};
}


int
bitte_put(mut_oid_t fact, echs_range_t valid)
{
	if (UNLIKELY(!(ntrans % NTPB))) {
		const size_t znu = ntrans + NTPB;
		void *nu_t, *nu_i, *nu_v;

		nu_t = xzfalloc(trans, ntrans, znu, sizeof(*trans));
		nu_i = xzfalloc(facts, ntrans, znu, sizeof(*facts));
		nu_v = xzfalloc(valids, ntrans, znu, sizeof(*valids));

		if (UNLIKELY(nu_t == NULL || nu_i == NULL || nu_v == NULL)) {
			/* try proper munmapping? */
			return -1;
		}
		/* otherwise reassign */
		trans = nu_t;
		facts = nu_i;
		valids = nu_v;
	}
	/* stamp off then */
	with (echs_instant_t t = echs_now()) {
		const size_t it = ntrans++;

		trans[it] = t;
		facts[it] = fact;
		valids[it] = valid;

		_put_ioff(&live, fact, it);
	}
	return 0;
}

int
bitte_rem(mut_oid_t fact)
{
	const size_t i = _get_ioff(live, fact);

	if (FACT_NOT_FOUND_P(i) || live.facts[i] == MUT_NUL_OID) {
		/* he's dead already */
		return -1;
	} else if (echs_nul_range_p(valids[live.offs[i]])) {
		/* dead already, just */
		return -1;
	}

	/* otherwise kill him */
	if (UNLIKELY(!(ntrans % NTPB))) {
		const size_t znu = ntrans + NTPB;
		void *nu_t, *nu_i, *nu_v;

		nu_t = xzfalloc(trans, ntrans, znu, sizeof(*trans));
		nu_i = xzfalloc(facts, ntrans, znu, sizeof(*facts));
		nu_v = xzfalloc(valids, ntrans, znu, sizeof(*valids));

		if (UNLIKELY(nu_t == NULL || nu_i == NULL || nu_v == NULL)) {
			/* try proper munmapping? */
			return -1;
		}
		/* otherwise reassign */
		trans = nu_t;
		facts = nu_i;
		valids = nu_v;
	}
	/* stamp him off */
	with (echs_instant_t t = echs_now()) {
		const size_t it = ntrans++;

		trans[it] = t;
		facts[it] = fact;
		valids[it] = echs_nul_range();

		_put_ioff(&live, fact, it);
	}
	return 0;
}

echs_bitmp_t
bitte_get(mut_oid_t fact, echs_instant_t as_of)
{
	size_t i_last_before = FACT_NOT_FOUND;
	size_t i_first_after = FACT_NOT_FOUND;

	if (UNLIKELY(!ntrans)) {
		/* no transactions in this store, trivial*/
		return ECHS_NUL_BITMP;
	}
	/* if AS_OF is >= the stamp of the last transaction, just use
	 * the live table. */
	else if (echs_instant_le_p(trans[ntrans - 1U], as_of)) {
		return _bitte_get_as_of_now(fact);
	}
	/* otherwise proceed to scan */
	with (size_t i = 0U) {
		for (; i < ntrans && echs_instant_le_p(trans[i], as_of); i++) {
			if (facts[i] == fact) {
				i_last_before = i;
			}
		}
		/* now I_LAST_BEFORE should hold FACT_NOT_FOUND or the index of
		 * the last fiddle with FACT before AS_OF */
		if (FACT_NOT_FOUND_P(i_last_before)) {
			/* must be dead */
			return ECHS_NUL_BITMP;
		}
		/* keep scanning, because the fact might have been superseded by
		 * a more recent transaction */
		for (; i < ntrans; i++) {
			if (facts[i] == fact) {
				i_first_after = i;
				break;
			}
		}
		/* now I_FIRST_AFTER should hold FACT_NOT_FOUND or the index of
		 * the next fiddle with FACT on or after AS_OF */
		if (FACT_NOT_FOUND_P(i_first_after)) {
			/* must be open-ended */
			return (echs_bitmp_t){
				valids[i_last_before],
				ECHS_RANGE_FROM(trans[i_last_before])
			};
		}
	}
	/* yay, dead or alive, it's in our books */
	/* otherwise it's bounded by trans[I_FIRST_AFTER] */
	return (echs_bitmp_t){
		valids[i_last_before],
		(echs_range_t){trans[i_last_before], trans[i_first_after]}
	};
}

int
bitte_supersede(mut_oid_t old, mut_oid_t new, echs_range_t valid)
{
	size_t oi;

	oi = _get_ioff(live, old);
	if (FACT_NOT_FOUND_P(oi) || live.facts[oi] == MUT_NUL_OID) {
		/* must be dead, cannot supersede */
		return -1;
	}

	/* otherwise make room for some (i.e. 2) insertions */
	if (UNLIKELY(!(ntrans % NTPB) || (ntrans % NTPB + 1U == NTPB))) {
		/* make sure we've got room for 2 insertions
		 * just ask for one more and kill the lsb */
		const size_t znu = ((ntrans + NTPB + 1U) >> 1U) << 1U;
		void *nu_t, *nu_i, *nu_v;

		nu_t = xzfalloc(trans, ntrans, znu, sizeof(*trans));
		nu_i = xzfalloc(facts, ntrans, znu, sizeof(*facts));
		nu_v = xzfalloc(valids, ntrans, znu, sizeof(*valids));

		if (UNLIKELY(nu_t == NULL || nu_i == NULL || nu_v == NULL)) {
			/* try proper munmapping? */
			return -1;
		}
		/* otherwise reassign */
		trans = nu_t;
		facts = nu_i;
		valids = nu_v;
	}
	/* atomically handle the supersedure */
	with (echs_instant_t t = echs_now()) {
		/* old guy first */
		with (const size_t it = ntrans++) {
			/* build new validity */
			echs_range_t nuv = {
				.from = valids[live.offs[oi]].from,
				.till = valid.from,
			};

			/* bang to timeline */
			trans[it] = t;
			facts[it] = old;
			valids[it] = nuv;

			_put_ioff(&live, old, it);
		}
		/* new guy now */
		with (const size_t it = ntrans++) {
			/* bang to timeline */
			trans[it] = t;
			facts[it] = new;
			valids[it] = valid;

			_put_ioff(&live, new, it);
		}
	}
	return 0;
}


echs_range_t
bitte_valid(mut_oid_t fact, echs_instant_t as_of)
{
	return bitte_get(fact, as_of).valid;
}

echs_range_t
bitte_trans(mut_oid_t fact, echs_instant_t as_of)
{
	return bitte_get(fact, as_of).trans;
}

/* timeline.c ends here */
