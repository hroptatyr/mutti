/*** bitte-mem.c -- bitemporal API using a timeline index
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
#include <stdint.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <string.h>
#include <assert.h>
#include "bitte.h"
#define IN_DSO(x)	bitte_mem_LTX_##x
#include "bitte-private.h"
#include "nifty.h"

/* at the moment we operate on 4k block sizes */
#define BLKZ	(4096U)
/* number of transaction stamps per block */
#define NTPB	(BLKZ / sizeof(echs_instant_t))

#if !defined MAP_ANON && defined MAP_ANONYMOUS
# define MAP_ANON	MAP_ANONYMOUS
#elif !defined MAP_ANON
# define MAP_ANON	(0x1000U)
#endif	/* !MAP_ANON */
#define PROT_RW		(PROT_READ | PROT_WRITE)
#define MAP_MEM		(MAP_PRIVATE | MAP_ANON)

/* a transaction id is simply an offset in the STOR SoA */
typedef uintptr_t mut_tid_t;

/* simple timeline index */
struct tmln_s {
	size_t ntrans;
	echs_instant_t *trans;
	mut_oid_t *facts;
	echs_range_t *valids;
};

/* fact offsets into the timeline */
typedef struct ftmap_s {
	size_t nfacts;
	echs_instant_t trans;
	mut_oid_t *facts;
	mut_tid_t *_1st;
	mut_tid_t *last;
} *ftmap_t;

#define FACT_NOT_FOUND		((mut_tid_t)-1)
#define FACT_NOT_FOUND_P(x)	(!~(mut_tid_t)(x))

#define ECHS_RANGE_FROM(x)	((echs_range_t){x, ECHS_UNTIL_CHANGED})

/* we promised to define the mut_stor_s struct */
typedef struct _stor_s {
	struct mut_stor_s super;
	struct tmln_s tmln;
	struct ftmap_s live;
	size_t ncache;
	struct ftmap_s cache[64U];
} *_stor_t;

/* one static stor for lazy lads */
static struct _stor_s stor;


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


static inline __attribute__((nonnull(1), const, pure)) size_t
_stor_ntrans(const struct _stor_s *s)
{
	return s->tmln.ntrans;
}

static inline __attribute__((nonnull(1))) echs_instant_t
_stor_get_trans(const struct _stor_s *s, mut_tid_t t)
{
	return s->tmln.trans[t];
}

static inline __attribute__((nonnull(1))) mut_oid_t
_stor_get_fact(const struct _stor_s *s, mut_tid_t t)
{
	return s->tmln.facts[t];
}

static inline __attribute__((nonnull(1))) echs_range_t
_stor_get_valid(const struct _stor_s *s, mut_tid_t t)
{
	return s->tmln.valids[t];
}


static __attribute__((nonnull(1))) mut_tid_t
_get_last_trans(const struct ftmap_s *m, mut_oid_t fact)
{
	for (size_t i = 0U; i < m->nfacts; i++) {
		if (m->facts[i] == fact) {
			return m->last[i];
		}
	}
	return FACT_NOT_FOUND;
	
}

static __attribute__((nonnull(1))) mut_tid_t
_get_first_trans(const struct ftmap_s *m, mut_oid_t fact)
{
	for (size_t i = 0U; i < m->nfacts; i++) {
		if (m->facts[i] == fact) {
			return m->_1st[i];
		}
	}
	return FACT_NOT_FOUND;
}

static int
_put_last_trans(ftmap_t m, mut_oid_t fact, echs_instant_t t, mut_tid_t last)
{
	size_t i;

	for (i = 0U; i < m->nfacts; i++) {
		if (m->facts[i] == fact) {
			/* just update him then */
			goto up_and_out;
		}
	}
	/* we'll have to extend the list of live facts */
	if (UNLIKELY((m->nfacts % NTPB) == 0U)) {
		const size_t ol = m->nfacts;
		const size_t nu = ol + NTPB;
		void *pi = xzfalloc(m->facts, ol, nu, sizeof(*m->facts));
		void *po = xzfalloc(m->last, ol, nu, sizeof(*m->last));
		void *p1 = xzfalloc(m->_1st, ol, nu, sizeof(*m->_1st));

		if (UNLIKELY(pi == NULL || po == NULL || p1 == NULL)) {
			/* brill */
			return -1;
		}
		m->facts = pi;
		m->last = po;
		m->_1st = p1;
	}
	/* just ass our fact */
	m->nfacts++;
	m->facts[i] = fact;
	m->_1st[i] = last;
up_and_out:
	m->trans = t;
	m->last[i] = last;
	return 0;
}

static void
ftmap_free(ftmap_t m)
{
	const size_t nf = m->nfacts;

	if (UNLIKELY(nf == 0U)) {
		assert(m->facts == NULL);
		assert(m->last == NULL);
		assert(m->_1st == NULL);
		return;
	}
	/* free these guys */
	xfree(m->facts, nf * sizeof(*m->facts));
	xfree(m->last, nf * sizeof(*m->last));
	xfree(m->_1st, nf * sizeof(*m->_1st));
	/* complete wipe out */
	memset(m, 0, sizeof(*m));
	return;
}


/* store handling */
static void
tmln_free(struct tmln_s *tl)
{
	const size_t nt = tl->ntrans;

	if (UNLIKELY(nt == 0U)) {
		assert(tl->trans == NULL);
		assert(tl->valids == NULL);
		assert(tl->facts == NULL);
		return;
	}
	/* otherwise free the guys */
	xfree(tl->trans, nt * sizeof(*tl->trans));
	xfree(tl->valids, nt * sizeof(*tl->valids));
	xfree(tl->facts, nt * sizeof(*tl->facts));
	/* complete wipe out */
	memset(tl, 0, sizeof(*tl));
	return;
}

static __attribute__((nonnull(1))) int
tmln_resize(struct tmln_s *restrict s, size_t nadd)
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
static __attribute__((nonnull(1))) echs_bitmp_t
_bitte_get_as_of_now(_stor_t s, mut_oid_t fact)
{
	mut_tid_t t = _get_last_trans(&s->live, fact);

	if (FACT_NOT_FOUND_P(t)) {
		/* must be dead */
		return ECHS_NUL_BITMP;
	}
	/* yay, dead or alive, it's in our books */
	return (echs_bitmp_t){
		_stor_get_valid(s, t),
		ECHS_RANGE_FROM(_stor_get_trans(s, t)),
	};
}

static __attribute__((nonnull(1))) echs_bitmp_t
_bitte_get(_stor_t s, mut_oid_t fact, echs_instant_t as_of)
{
	mut_tid_t i_last_before = FACT_NOT_FOUND;
	mut_tid_t i_first_after = FACT_NOT_FOUND;
	mut_tid_t i = 0U;

	for (; i < _stor_ntrans(s) &&
		     echs_instant_le_p(_stor_get_trans(s, i), as_of); i++) {
		if (fact == _stor_get_fact(s, i)) {
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
	for (; i < _stor_ntrans(s); i++) {
		if (fact == _stor_get_fact(s, i)) {
			i_first_after = i;
			break;
		}
	}
	/* now I_FIRST_AFTER should hold FACT_NOT_FOUND or the index of
	 * the next fiddle with FACT on or after AS_OF */
	if (FACT_NOT_FOUND_P(i_first_after)) {
		/* must be open-ended */
		return (echs_bitmp_t){
			_stor_get_valid(s, i_last_before),
			ECHS_RANGE_FROM(_stor_get_trans(s, i_last_before))
		};
	}

	/* yay, dead or alive, it's in our books */
	/* otherwise it's bounded by trans[I_FIRST_AFTER] */
	return (echs_bitmp_t){
		_stor_get_valid(s, i_last_before),
		(echs_range_t){
			_stor_get_trans(s, i_last_before),
			_stor_get_trans(s, i_first_after)
		}
	};
}

static __attribute__((nonnull(1))) size_t
_bitte_rtr_as_of_now(_stor_t s, mut_oid_t *restrict fact, size_t nfact)
{
/* current transaction time-slice, very naive */
	const size_t n = min(size_t, nfact, s->live.nfacts);

	for (size_t i = 0U; i < n; i++) {
		fact[i] = (mut_oid_t)s->live.last[i];
	}
	return n;
}

static __attribute__((nonnull(1))) size_t
_bitte_rtr(
	_stor_t s, mut_oid_t *restrict fact, size_t nfact,
	echs_instant_t as_of)
{
	/* cache index */
	size_t ci;
	/* timeline index */
	mut_tid_t ti = 0U;

	/* try caches */
	for (ci = 0U; ci < s->ncache; ci++) {
		if (!echs_instant_lt_p(as_of, s->cache[ci].trans)) {
			/* found one */
			goto evolv;
		}
	}
	if (0) {
	evolv:
		/* yay, found a cache we can evolve,
		 * find highest offset into trans so we know where to
		 * start scanning */
		for (size_t i = 0U; i < s->cache[ci].nfacts; i++) {
			if (s->cache[ci].last[i] > ti) {
				ti = s->cache[ci].last[i];
			}
		}
		/* we can start a bit later since the TI-th offset is covered */
		ti++;
	} else if (LIKELY(s->ncache < countof(s->cache))) {
		/* have to start a new cache object, we're guaranteed
		 * that we can insert at the back because the cache is
		 * sorted in reverse chronological order */
		assert(s->live.nfacts);
		assert(ci == s->ncache);

		s->ncache++;
	} else {
		/* grml, looks like we have to bin a cache item */
		return 0U;
	}
	/* now start materialising the cache */
	for (echs_instant_t t;
	     ti < _stor_ntrans(s) &&
		     (t = _stor_get_trans(s, ti), echs_instant_le_p(t, as_of));
	     ti++) {
		const mut_oid_t f = _stor_get_fact(s, ti);
		_put_last_trans(s->cache + ci, f, t, ti);
	}

	/* same as _bitte_rtr_as_of_now() now */
	const size_t n = min(size_t, nfact, s->cache[ci].nfacts);

	for (size_t i = 0U; i < n; i++) {
		fact[i] = (mut_oid_t)s->cache[ci].last[i];
	}
	return n;
}

static __attribute__((nonnull(1))) echs_range_t
_bitte_trend(const struct _stor_s *s, mut_tid_t t)
{
/* return the transaction interval for transaction T */
	const mut_oid_t f = _stor_get_fact(s, t);
	const mut_tid_t hi = _get_last_trans(&s->live, f);
	echs_range_t res;

	assert(!FACT_NOT_FOUND_P(hi));
	if (hi == t) {
		/* this transaction is still current, i.e. open-ended */
		return ECHS_RANGE_FROM(_stor_get_trans(s, t));
	}
	assert(t < hi);

	/* best known upper bound is the one in the live blob */
	res = (echs_range_t){_stor_get_trans(s, t), _stor_get_trans(s, hi)};
	/* and now scan the timeline between T and HI */
	for (mut_tid_t i = t + 1U; i < hi; i++) {
		if (f == _stor_get_fact(s, i)) {
			res.till = _stor_get_trans(s, i);
			break;
		}
	}
	return res;
}

static __attribute__((nonnull(1))) void
_bitte_trends(
	const struct _stor_s *s, echs_range_t *restrict tt,
	const mut_tid_t *t, size_t nt)
{
/* TRansaction ENDs, obtain offsets in OF find the transaction time range */
	/* try and lookup each of the facts in the live blob */
	for (size_t i = 0U; i < nt; i++) {
		const mut_tid_t o = t[i];

		tt[i] = _bitte_trend(s, o);
	}
	return;
}


static mut_stor_t
_open(const char *fn, int UNUSED(fl))
{
	if (fn == NULL) {
		/* mem store they want, good */
		return calloc(1, sizeof(struct _stor_s));
	}
	return NULL;
}

static void
_close(mut_stor_t s)
{
	_stor_t _s;

	if ((_s = (_stor_t)s) == NULL) {
		/* free static resources */
		_s = &stor;
	}
	tmln_free(&_s->tmln);
	ftmap_free(&_s->live);
	for (size_t i = 0U; i < _s->ncache; i++) {
		ftmap_free(_s->cache + i);
	}
	return;
}

static int
_put(mut_stor_t s, mut_oid_t fact, echs_range_t valid)
{
	_stor_t _s;

	if (UNLIKELY(fact == MUT_NUL_OID)) {
		return -1;
	} else if ((_s = (_stor_t)s) == NULL) {
		_s = &stor;
	}
	/* check for resize */
	if (UNLIKELY(!(_s->tmln.ntrans % NTPB))) {
		if (UNLIKELY(tmln_resize(&_s->tmln, NTPB) < 0)) {
			return -1;
		}
	}
	/* stamp off then */
	with (echs_instant_t t = echs_now()) {
		const size_t it = _s->tmln.ntrans++;

		_s->tmln.trans[it] = t;
		_s->tmln.facts[it] = fact;
		_s->tmln.valids[it] = valid;

		_put_last_trans(&_s->live, fact, t, it);
	}
	return 0;
}

static int
_rem(mut_stor_t s, mut_oid_t fact)
{
	_stor_t _s;

	if ((_s = (_stor_t)s) == NULL) {
		/* use static resources */
		_s = &stor;
	}

	const mut_tid_t t = _get_last_trans(&_s->live, fact);

	if (FACT_NOT_FOUND_P(t)) {
		/* he's dead already */
		return -1;
	} else if (echs_nul_range_p(_stor_get_valid(_s, t))) {
		/* dead already, just */
		return -1;
	}
	/* otherwise kill him */
	/* check for resize */
	if (UNLIKELY(!(_s->tmln.ntrans % NTPB))) {
		if (UNLIKELY(tmln_resize(&_s->tmln, NTPB) < 0)) {
			return -1;
		}
	}
	/* stamp him off */
	with (echs_instant_t now = echs_now()) {
		const size_t it = _s->tmln.ntrans++;

		_s->tmln.trans[it] = now;
		_s->tmln.facts[it] = fact;
		_s->tmln.valids[it] = echs_nul_range();

		_put_last_trans(&_s->live, fact, now, it);
	}
	return 0;
}

static echs_bitmp_t
_get(mut_stor_t s, mut_oid_t fact, echs_instant_t as_of)
{
	_stor_t _s;

	if ((_s = (_stor_t)s) == NULL) {
		/* use static resources */
		_s = &stor;
	}
	if (UNLIKELY(!_s->tmln.ntrans || fact == MUT_NUL_OID)) {
		/* no transactions in this store, trivial*/
		return ECHS_NUL_BITMP;
	}
	/* if AS_OF is >= the stamp of the last transaction, just use
	 * the live table. */
	else if (echs_instant_le_p(_s->live.trans, as_of)) {
		return _bitte_get_as_of_now(_s, fact);
	}
	/* otherwise proceed to scan */
	return _bitte_get(_s, fact, as_of);
}

static int
_supersede(
	mut_stor_t s, mut_oid_t old, mut_oid_t new, echs_range_t valid)
{
	_stor_t _s;

	if ((_s = (_stor_t)s) == NULL) {
		/* use static resources */
		_s = &stor;
	}

	const mut_tid_t ot = _get_last_trans(&_s->live, old);

	if (FACT_NOT_FOUND_P(ot)) {
		/* must be dead, cannot supersede */
		return -1;
	}

	/* otherwise make room for some (i.e. 2) insertions */
	if (UNLIKELY(new && !((_s->tmln.ntrans + 1U) % NTPB) ||
		     !(_s->tmln.ntrans % NTPB))) {
		const size_t n = NTPB +
			(new && !((_s->tmln.ntrans + 1U) % NTPB));

		if (UNLIKELY(tmln_resize(&_s->tmln, n) < 0)) {
			return -1;
		}
	}
	/* atomically handle the supersedure */
	with (echs_instant_t t = echs_now()) {
		/* old guy first */
		with (const size_t it = _s->tmln.ntrans++) {
			/* build new validity */
			echs_range_t nuv = {
				.from = _stor_get_valid(_s, ot).from,
				.till = valid.from,
			};

			/* bang to timeline */
			_s->tmln.trans[it] = t;
			_s->tmln.facts[it] = old;
			_s->tmln.valids[it] = nuv;

			_put_last_trans(&_s->live, old, t, it);
		}
		/* new guy now */
		if (new != MUT_NUL_OID) {
			const size_t it = _s->tmln.ntrans++;

			/* bang to timeline */
			_s->tmln.trans[it] = t;
			_s->tmln.facts[it] = new;
			_s->tmln.valids[it] = valid;

			_put_last_trans(&_s->live, new, t, it);
		}
	}
	return 0;
}

/* hardest one so far */
static size_t
_rtr(
	mut_stor_t s,
	mut_oid_t *restrict fact, size_t nfact,
	echs_range_t *restrict valid, echs_range_t *restrict trans,
	echs_instant_t as_of)
{
/* we'll abuse the FACT array to store our offsets first,
 * then we'll copy the actual fact oids and valids and transs */
	bool currentp;
	size_t res;
	_stor_t _s;

	if ((_s = (_stor_t)s) == NULL) {
		/* use static resources */
		_s = &stor;
	}
	if (UNLIKELY(!_stor_ntrans(_s))) {
		/* no transactions in this store, trivial*/
		return 0U;
	}
	/* if AS_OF is >= the stamp of the last transaction, just use
	 * the live table. */
	else if ((currentp = echs_instant_le_p(_s->live.trans, as_of))) {
		res = _bitte_rtr_as_of_now(_s, fact, nfact);
	}
	/* otherwise just do it the hard way */
	else {
		res = _bitte_rtr(_s, fact, nfact, as_of);
	}

	/* now we've got them offsets,
	 * reiterate and assemble the arrays */
	if (trans != NULL && currentp) {
		for (size_t i = 0U; i < res; i++) {
			const size_t o = fact[i];
			trans[i] = ECHS_RANGE_FROM(_stor_get_trans(_s, o));
		}
	} else if (trans != NULL) {
		/* cluster fuck, how do we know when trans[o] ends?
		 * well, we scan again for any of the offsets in
		 * FACT and find the next transaction and put it into
		 * TRANS. */
		_bitte_trends(_s, trans, fact, res);
	}
	if (valid != NULL) {
		for (size_t i = 0U; i < res; i++) {
			const size_t o = fact[i];
			valid[i] = _stor_get_valid(_s, o);
		}
	}
	/* ... and finally */
	for (size_t i = 0U; i < res; i++) {
		const size_t o = fact[i];
		fact[i] = _stor_get_fact(_s, o);
	}
	return res;
}

static size_t
_scan(
	mut_stor_t s,
	mut_oid_t *restrict fact, size_t nfact,
	echs_range_t *restrict valid, echs_range_t *restrict trans,
	echs_instant_t vtime)
{
	size_t res = 0U;
	_stor_t _s;

	if ((_s = (_stor_t)s) == NULL) {
		/* use static resources */
		_s = &stor;
	}

	/* store offsets in FACT table first */
	for (size_t i = 0U; i < _stor_ntrans(_s) && res < nfact; i++) {
		if (echs_in_range_p(_stor_get_valid(_s, i), vtime)) {
			fact[res++] = i;
		}
	}
	/* go through FACT table and bang real objects */
	if (trans != NULL) {
		_bitte_trends(_s, trans, fact, res);
	}
	if (valid != NULL) {
		for (size_t i = 0U; i < res; i++) {
			const size_t o = fact[i];
			valid[i] = _stor_get_valid(_s, o);
		}
	}
	/* ... and finally */
	for (size_t i = 0U; i < res; i++) {
		const size_t o = fact[i];
		fact[i] = _stor_get_fact(_s, o);
	}
	return res;
}

static size_t
_hist(
	mut_stor_t s,
	echs_range_t *restrict trans, size_t ntrans,
	echs_range_t *restrict valid, mut_oid_t fact)
{
	size_t res = 0U;
	_stor_t _s;

	if ((_s = (_stor_t)s) == NULL) {
		/* use static resources */
		_s = &stor;
	}

	const mut_tid_t t1 = _get_first_trans(&_s->live, fact);

	if (UNLIKELY(FACT_NOT_FOUND_P(t1))) {
		/* if there's no last transaction, there can be no 1st either */
		return 0U;
	}
	/* traverse the timeline and store offsets to transactions */
	for (mut_tid_t i = t1; i < _stor_ntrans(_s) && res < ntrans; i++) {
		if (fact == _stor_get_fact(_s, i)) {
			trans[res++].from.u = i;
		}
	}

	if (valid != NULL) {
		for (size_t i = 0U; i < res; i++) {
			const size_t o = trans[i].from.u;
			valid[i] = _s->tmln.valids[o];
		}
	}

	/* and lastly write down the real transactions */
	for (size_t i = 1U; i < res; i++) {
		const size_t of = trans[i - 1U].from.u;
		const size_t ot = trans[i].from.u;

		trans[i - 1U] = (echs_range_t){
			_s->tmln.trans[of], _s->tmln.trans[ot]
		};
	}
	/* last trans lasts forever */
	if (res > 0U) {
		const size_t o = trans[res - 1U].from.u;
		trans[res - 1U] = ECHS_RANGE_FROM(_s->tmln.trans[o]);
	}
	return res;
}

/* this sums up our implementation */
struct bitte_backend_s IN_DSO(backend) = {
	_open,
	_close,
	_get,
	_put,
	_supersede,
	_rem,
	_rtr,
	_scan,
	_hist,
};

/* bitte.c ends here */
