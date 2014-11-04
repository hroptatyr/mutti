/*** bitte.c -- bitemporal API using a timeline index
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
#include <assert.h>
#include "bitte.h"
#include "nifty.h"

/* at the moment we operate on 4k block sizes */
#define BLKZ	(4096U)
/* number of transaction stamps per block */
#define NTPB	(BLKZ / sizeof(*stor.trans))

#if !defined MAP_ANON && defined MAP_ANONYMOUS
# define MAP_ANON	MAP_ANONYMOUS
#elif !defined MAP_ANON
# define MAP_ANON	(0x1000U)
#endif	/* !MAP_ANON */
#define PROT_RW		(PROT_READ | PROT_WRITE)
#define MAP_MEM		(MAP_PRIVATE | MAP_ANON)

/* simple timeline index */
struct tili_s {
	size_t ntrans;
	echs_instant_t *trans;
	mut_oid_t *facts;
	echs_range_t *valids;
};

/* fact offsets into the timeline */
struct foff_s {
	size_t nfacts;
	echs_instant_t trans;
	mut_oid_t *facts;
	size_t *offs;
};

static struct tili_s stor;
static struct foff_s live;
static struct foff_s cache[64U];
static size_t ncache;

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

#define min(T, x, y)		min_##T(x, y)
#define DEFMIN(T)				\
static inline __attribute__((pure, const)) T	\
min_##T(T x, T y)				\
{						\
	return x <= y ? x : y;			\
}						\
struct __##T##_defined_s

DEFMIN(size_t);


static size_t
_get_foff(struct foff_s *v, mut_oid_t fact)
{
	size_t i_fr = FACT_NOT_FOUND;

	for (size_t i = 0U; i < v->nfacts; i++) {
		if (v->facts[i] == fact) {
			return i;
		}
	}
	return i_fr;
}

static int
_put_foff(struct foff_s *tgt, mut_oid_t fact, echs_instant_t t, size_t last)
{
	size_t i;

	i = _get_foff(tgt, fact);
	if (!FACT_NOT_FOUND_P(i)) {
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
	tgt->trans = t;
	tgt->offs[i] = last;
	return 0;
}


/* store handling */
static __attribute__((nonnull(1))) int
tili_resize(struct tili_s *restrict s, size_t nadd)
{
/* resize to NADD additional slots */
	const size_t zol = s->ntrans;
	const size_t znu = zol + nadd;
	void *nu_t, *nu_i, *nu_v;

	nu_t = xzfalloc(s->trans, zol, znu, sizeof(*s->trans));
	nu_i = xzfalloc(s->facts, zol, znu, sizeof(*s->facts));
	nu_v = xzfalloc(s->valids, zol, znu, sizeof(*s->valids));

	if (UNLIKELY(nu_t == NULL || nu_i == NULL || nu_v == NULL)) {
		/* try proper munmapping? */
		return -1;
	}
	/* otherwise reassign */
	s->trans = nu_t;
	s->facts = nu_i;
	s->valids = nu_v;
	return 0;
}


/* meta */
static echs_bitmp_t
_bitte_get_as_of_now(mut_oid_t fact)
{
	size_t i;

	i = _get_foff(&live, fact);
	if (FACT_NOT_FOUND_P(i)) {
		/* must be dead */
		return ECHS_NUL_BITMP;
	}
	/* yay, dead or alive, it's in our books */
	i = live.offs[i];
	return (echs_bitmp_t){
		stor.valids[i],
		(echs_range_t){stor.trans[i], ECHS_UNTIL_CHANGED}
	};
}

static echs_bitmp_t
_bitte_get(mut_oid_t fact, echs_instant_t as_of)
{
	size_t i_last_before = FACT_NOT_FOUND;
	size_t i_first_after = FACT_NOT_FOUND;
	size_t i = 0U;

	for (; i < stor.ntrans &&
		     echs_instant_le_p(stor.trans[i], as_of); i++) {
		if (stor.facts[i] == fact) {
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
	for (; i < stor.ntrans; i++) {
		if (stor.facts[i] == fact) {
			i_first_after = i;
			break;
		}
	}
	/* now I_FIRST_AFTER should hold FACT_NOT_FOUND or the index of
	 * the next fiddle with FACT on or after AS_OF */
	if (FACT_NOT_FOUND_P(i_first_after)) {
		/* must be open-ended */
		return (echs_bitmp_t){
			stor.valids[i_last_before],
				ECHS_RANGE_FROM(stor.trans[i_last_before])
				};
	}

	/* yay, dead or alive, it's in our books */
	/* otherwise it's bounded by trans[I_FIRST_AFTER] */
	return (echs_bitmp_t){
		stor.valids[i_last_before],
		(echs_range_t){
			stor.trans[i_last_before],
			stor.trans[i_first_after]
		}
	};
}

static size_t
_bitte_rtr_as_of_now(mut_oid_t *restrict fact, size_t nfact)
{
/* current transaction time-slice, very naive */
	const size_t n = min(size_t, nfact, live.nfacts);

	for (size_t i = 0U; i < n; i++) {
		fact[i] = (mut_oid_t)live.offs[i];
	}
	return n;
}

static size_t
_bitte_rtr(mut_oid_t *restrict fact, size_t nfact, echs_instant_t as_of)
{
	/* cache index */
	size_t ci;
	/* timeline index (aka offset) */
	size_t ti = 0U;

	/* try caches */
	for (ci = 0U; ci < ncache; ci++) {
		if (!echs_instant_lt_p(as_of, cache[ci].trans)) {
			/* found one */
			break;
		}
	}
	if (ci < ncache) {
		/* yay, found a cache we can evolve,
		 * find highest offset into trans so we know where to
		 * start scanning */
		for (size_t i = 0U; i < cache[ci].nfacts; i++) {
			if (cache[ci].offs[i] > ti) {
				ti = cache[ci].offs[i];
			}
		}
		/* we can start a bit later since the TI-th offset is covered */
		ti++;
	} else if (LIKELY(ncache < countof(cache))) {
		/* have to start a new cache object, we're guaranteed
		 * that we can insert at the back because the cache is
		 * sorted in reverse chronological order */
		assert(live.nfacts);
		assert(ci == ncache);

		ncache++;
	} else {
		/* grml, looks like we have to bin a cache item */
		return 0U;
	}
	/* now start materialising the cache */
	for (echs_instant_t t;
	     ti < stor.ntrans &&
		     (t = stor.trans[ti], echs_instant_le_p(t, as_of)); ti++) {
		_put_foff(cache + ci, stor.facts[ti], t, ti);
	}

	/* same as _bitte_rtr_as_of_now() now */
	const size_t n = min(size_t, nfact, cache[ci].nfacts);

	for (size_t i = 0U; i < n; i++) {
		fact[i] = (mut_oid_t)cache[ci].offs[i];
	}
	return n;
}

static void
_bitte_rtr_trend(echs_range_t *restrict trans, const mut_oid_t *of, size_t nof)
{
/* TRansaction ENDs, obtain offsets in OF find the transaction time range */
	/* try and lookup each of the facts in the live blob */
	for (size_t i = 0U; i < nof; i++) {
		const size_t o = of[i];
		const mut_oid_t f = stor.facts[o];
		const size_t hi = _get_foff(&live, f);
		const size_t ohi = live.offs[hi];

		assert(!FACT_NOT_FOUND_P(hi));
		if (ohi == o) {
			trans[i] = ECHS_RANGE_FROM(stor.trans[o]);
			continue;
		}
		assert(o < ohi);
		/* best known upper bound is the one in the live blob */
		trans[i] = (echs_range_t){stor.trans[o], stor.trans[ohi]};
		/* and now scan the timeline between O and HI */
		for (size_t j = o + 1U; j < ohi; j++) {
			if (stor.facts[j] == f) {
				trans[i].till = stor.trans[j];
				break;
			}
		}
	}
	return;
}


int
bitte_put(mut_oid_t fact, echs_range_t valid)
{
	if (UNLIKELY(fact == MUT_NUL_OID)) {
		return -1;
	} else if (UNLIKELY(!(stor.ntrans % NTPB))) {
		if (UNLIKELY(tili_resize(&stor, NTPB) < 0)) {
			return -1;
		}
	}
	/* stamp off then */
	with (echs_instant_t t = echs_now()) {
		const size_t it = stor.ntrans++;

		stor.trans[it] = t;
		stor.facts[it] = fact;
		stor.valids[it] = valid;

		_put_foff(&live, fact, t, it);
	}
	return 0;
}

int
bitte_rem(mut_oid_t fact)
{
	const size_t i = _get_foff(&live, fact);

	if (FACT_NOT_FOUND_P(i)) {
		/* he's dead already */
		return -1;
	} else if (echs_nul_range_p(stor.valids[live.offs[i]])) {
		/* dead already, just */
		return -1;
	}

	/* otherwise kill him */
	if (UNLIKELY(!(stor.ntrans % NTPB) && tili_resize(&stor, NTPB) < 0)) {
		return -1;
	}
	/* stamp him off */
	with (echs_instant_t t = echs_now()) {
		const size_t it = stor.ntrans++;

		stor.trans[it] = t;
		stor.facts[it] = fact;
		stor.valids[it] = echs_nul_range();

		_put_foff(&live, fact, t, it);
	}
	return 0;
}

echs_bitmp_t
bitte_get(mut_oid_t fact, echs_instant_t as_of)
{
	if (UNLIKELY(!stor.ntrans || fact == MUT_NUL_OID)) {
		/* no transactions in this store, trivial*/
		return ECHS_NUL_BITMP;
	}
	/* if AS_OF is >= the stamp of the last transaction, just use
	 * the live table. */
	else if (echs_instant_le_p(live.trans, as_of)) {
		return _bitte_get_as_of_now(fact);
	}
	/* otherwise proceed to scan */
	return _bitte_get(fact, as_of);
}

int
bitte_supersede(mut_oid_t old, mut_oid_t new, echs_range_t valid)
{
	const size_t oi = _get_foff(&live, old);

	if (FACT_NOT_FOUND_P(oi)) {
		/* must be dead, cannot supersede */
		return -1;
	}

	/* otherwise make room for some (i.e. 2) insertions */
	if (UNLIKELY(new && !((stor.ntrans + 1U) % NTPB) ||
		     !(stor.ntrans % NTPB))) {
		const size_t n = NTPB + (new && !((stor.ntrans + 1U) % NTPB));

		if (UNLIKELY(tili_resize(&stor, n) < 0)) {
			return -1;
		}
	}
	/* atomically handle the supersedure */
	with (echs_instant_t t = echs_now()) {
		/* old guy first */
		with (const size_t it = stor.ntrans++) {
			/* build new validity */
			echs_range_t nuv = {
				.from = stor.valids[live.offs[oi]].from,
				.till = valid.from,
			};

			/* bang to timeline */
			stor.trans[it] = t;
			stor.facts[it] = old;
			stor.valids[it] = nuv;

			_put_foff(&live, old, t, it);
		}
		/* new guy now */
		if (new != MUT_NUL_OID) {
			const size_t it = stor.ntrans++;

			/* bang to timeline */
			stor.trans[it] = t;
			stor.facts[it] = new;
			stor.valids[it] = valid;

			_put_foff(&live, new, t, it);
		}
	}
	return 0;
}

/* hardest one so far */
size_t
bitte_rtr(
	mut_oid_t *restrict fact, size_t nfact,
	echs_range_t *restrict valid, echs_range_t *restrict trans,
	echs_instant_t as_of)
{
/* we'll abuse the FACT array to store our offsets first,
 * then we'll copy the actual fact oids and valids and transs */
	bool currentp;
	size_t res;

	if (UNLIKELY(!stor.ntrans)) {
		/* no transactions in this store, trivial*/
		return 0U;
	}
	/* if AS_OF is >= the stamp of the last transaction, just use
	 * the live table. */
	else if ((currentp = echs_instant_le_p(live.trans, as_of))) {
		res = _bitte_rtr_as_of_now(fact, nfact);
	}
	/* otherwise just do it the hard way */
	else {
		res = _bitte_rtr(fact, nfact, as_of);
	}

	/* now we've got them offsets,
	 * reiterate and assemble the arrays */
	if (trans != NULL && currentp) {
		for (size_t i = 0U; i < res; i++) {
			const size_t o = fact[i];
			trans[i] = ECHS_RANGE_FROM(stor.trans[o]);
		}
	} else if (trans != NULL) {
		/* cluster fuck, how do we know when trans[o] ends?
		 * well, we scan again for any of the offsets in
		 * FACT and find the next transaction and put it into
		 * TRANS. */
		_bitte_rtr_trend(trans, fact, res);
	}
	if (valid != NULL) {
		for (size_t i = 0U; i < res; i++) {
			const size_t o = fact[i];
			valid[i] = stor.valids[o];
		}
	}
	/* ... and finally */
	for (size_t i = 0U; i < res; i++) {
		const size_t o = fact[i];
		fact[i] = stor.facts[o];
	}
	return res;
}

size_t
bitte_scan(
	mut_oid_t *restrict fact, size_t nfact,
	echs_range_t *restrict valid, echs_range_t *restrict trans,
	echs_instant_t vtime)
{
	size_t res = 0U;

	/* store offsets in FACT table first */
	for (size_t i = 0U; i < stor.ntrans && res < nfact; i++) {
		if (echs_in_range_p(stor.valids[i], vtime)) {
			fact[res++] = i;
		}
	}
	/* go through FACT table and bang real objects */
	if (trans != NULL) {
		_bitte_rtr_trend(trans, fact, res);
	}
	if (valid != NULL) {
		for (size_t i = 0U; i < res; i++) {
			const size_t o = fact[i];
			valid[i] = stor.valids[o];
		}
	}
	/* ... and finally */
	for (size_t i = 0U; i < res; i++) {
		const size_t o = fact[i];
		fact[i] = stor.facts[o];
	}
	return res;
}

/* bitte.c ends here */
