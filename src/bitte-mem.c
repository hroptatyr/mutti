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
#define BLKZ	(64U * 4096U)
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
	size_t ztrans;
	echs_instant_t *trans;
	mut_oid_t *facts;
	echs_range_t *valids;
};

/* fact nodes */
struct ftnd_s {
	mut_tid_t _1st;
	mut_tid_t last;
};

/* fact offsets into the timeline */
typedef struct ftmap_s {
	size_t nfacts;
	size_t zfacts;
	echs_instant_t trans;
	mut_oid_t *facts;
	struct ftnd_s *span;
} *ftmap_t;

#define TID_NOT_FOUND		((mut_tid_t)-1)
#define TID_NOT_FOUND_P(x)	(!~(mut_tid_t)(x))

#define FACT_NOT_FOUND		((struct ftnd_s){TID_NOT_FOUND})
#define FACT_NOT_FOUND_P(x)	(TID_NOT_FOUND_P((x)._1st))

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


/* ftmap fiddling */
#define FTMAP_FOREACH(var, map)			\
	for (size_t var = 0U; var < (1ULL << (map)->zfacts); var++)	\
		if ((map)->facts[var])

static int
init_ftmap(ftmap_t m)
{
#define FTMAP_LEAST	(9U)
#define FTMAP_RSTEP	(3U)
	assert(m->nfacts == 0U);
	assert(m->zfacts == 0U);
	assert(m->facts == NULL);
	assert(m->span == NULL);

	m->zfacts = FTMAP_LEAST;
	m->facts = xzfalloc(NULL, 0U, (1ULL << m->zfacts), sizeof(*m->facts));
	m->span = xzfalloc(NULL, 0U, (1ULL << m->zfacts), sizeof(*m->span));
	return (m->facts != NULL && m->span != NULL) - 1;
}

static void
fini_ftmap(ftmap_t m)
{
	if (UNLIKELY(m->zfacts < FTMAP_LEAST)) {
		assert(m->facts == NULL);
		assert(m->span == NULL);
		return;
	}
	/* free these guys */
	const size_t zf = 1ULL << m->zfacts;
	xfree(m->facts, zf * sizeof(*m->facts));
	xfree(m->span, zf * sizeof(*m->span));
	/* complete wipe out */
	memset(m, 0, sizeof(*m));
	return;
}

static __attribute__((noinline)) int
ftmap_rsz(struct ftmap_s *m)
{
/* extend by doubling the hash array, no rehashing will take place */
	if (UNLIKELY(!m->zfacts)) {
		return init_ftmap(m);
	}
	/* rehash */
	const size_t nu = 1ULL << (m->zfacts + 1U);
	mut_oid_t *pf = xzfalloc(NULL, 0UL, nu, sizeof(*m->facts));
	struct ftnd_s *ps = xzfalloc(NULL, 0UL, nu, sizeof(*m->span));

	if (UNLIKELY(pf == NULL || ps == NULL)) {
		/* brill */
		return -1;
	}

	/* rehash */
	FTMAP_FOREACH(i, m) {
		size_t o = m->facts[i] & (nu - 1ULL) & ~0x7ULL;
		const size_t eo = o + 8U;

		/* loop */
		for (; o < eo; o++) {
			if (!pf[o]) {
				pf[o] = m->facts[i];
				ps[o] = m->span[i];
				break;
			}
		}
		if (UNLIKELY(o == eo)) {
			/* grml */
			abort();
		}
	}

	m->zfacts++;
	m->facts = pf;
	m->span = ps;
	return 0;
}

static inline __attribute__((pure)) size_t
ftmap_off(const struct ftmap_s *m, mut_oid_t fact)
{
/* this is the offset getting routine
 * we'll check fact mod 2^m->zfacts and all slots in the 64B cache-line */
	if (LIKELY(m->zfacts)) {
		const size_t z = (1ULL << m->zfacts);

		/* just loop through him */
		for (size_t o = fact & (z - 1ULL) & ~0x7ULL,
			     eo = o + 8U; o < eo; o++) {
			if (!m->facts[o] || m->facts[o] == fact) {
				return o;
			}
		}
		/* getting here means o == 2^z, try the next 2-power
		 * iff fact > 2^z */
		if (UNLIKELY(fact < z)) {
			abort();
		}
	}
	return (size_t)-1;
}

static __attribute__((nonnull(1))) struct ftnd_s
ftmap_get(const struct ftmap_s *m, mut_oid_t fact)
{
	/* hash fact */
	const size_t o = ftmap_off(m, fact);

	/* now either o >= 2^m->zfacts or m->facts[o] == 0U
	 * or we've found him */
	if (UNLIKELY(o >= (1ULL << m->zfacts) || !m->facts[o])) {
		return FACT_NOT_FOUND;
	}
	return m->span[o];
}

static __attribute__((nonnull(1), flatten)) mut_tid_t
ftmap_get_last(const struct ftmap_s *m, mut_oid_t fact)
{
	const struct ftnd_s f = ftmap_get(m, fact);

	if (UNLIKELY(FACT_NOT_FOUND_P(f))) {
		return TID_NOT_FOUND;
	}
	return f.last;
}

static __attribute__((nonnull(1), flatten)) mut_tid_t
ftmap_get_first(const struct ftmap_s *m, mut_oid_t fact)
{
	const struct ftnd_s f = ftmap_get(m, fact);

	if (UNLIKELY(FACT_NOT_FOUND_P(f))) {
		return TID_NOT_FOUND;
	}
	return f._1st;
}

static int
ftmap_put_last(ftmap_t m, mut_oid_t fact, echs_instant_t t, mut_tid_t last)
{
	/* hash fact */
	size_t o = ftmap_off(m, fact);

	if (UNLIKELY(o >= (1ULL << m->zfacts))) {
		if (UNLIKELY(ftmap_rsz(m) < 0)) {
			/* fuck */
			return -1;
		} else if ((o = ftmap_off(m, fact)) >= (1ULL << m->zfacts)) {
			/* big big fuck */
			return -1;
		}
	} else if (LIKELY(m->facts[o] == fact)) {
		/* just update him then */
		goto up_and_out;
	}
	/* otherwise reuse the slot at hand then */
	m->nfacts++;
	m->facts[o] = fact;
	m->span[o]._1st = last;
up_and_out:
	m->span[o].last = last;
	m->trans = t;
	return 0;
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
	const size_t zol = s->ztrans;
	const size_t znu = zol * 2U ?: zol + nadd;
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
	s->ztrans = znu;
	return 0;
}

static inline __attribute__((nonnull(1))) int
tmln_chkz(struct tmln_s *restrict s, size_t nadd)
{
/* check if there's enough space to add NADD more items */
	if (UNLIKELY(s->ntrans + nadd > s->ztrans)) {
		return tmln_resize(s, NTPB);
	}
	return 0;
}


/* meta */
static __attribute__((nonnull(1))) echs_bitmp_t
_get_as_of_now(_stor_t s, mut_oid_t fact)
{
	mut_tid_t t = ftmap_get_last(&s->live, fact);

	if (TID_NOT_FOUND_P(t)) {
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
_get_as_of_then(_stor_t s, mut_oid_t fact, echs_instant_t as_of)
{
	mut_tid_t i_last_before = TID_NOT_FOUND;
	mut_tid_t i_first_after = TID_NOT_FOUND;
	mut_tid_t i = 0U;

	for (; i < _stor_ntrans(s) &&
		     echs_instant_le_p(_stor_get_trans(s, i), as_of); i++) {
		if (fact == _stor_get_fact(s, i)) {
			i_last_before = i;
		}
	}
	/* now I_LAST_BEFORE should hold FACT_NOT_FOUND or the index of
	 * the last fiddle with FACT before AS_OF */
	if (TID_NOT_FOUND_P(i_last_before)) {
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
	if (TID_NOT_FOUND_P(i_first_after)) {
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
	size_t res = 0U;

	FTMAP_FOREACH(i, &s->live) {
		fact[res++] = (mut_oid_t)s->live.span[i].last;
		if (UNLIKELY(res >= nfact)) {
			break;
		}
	}
	return res;
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
	size_t res = 0U;

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
		FTMAP_FOREACH(i, s->cache + ci) {
			if (s->cache[ci].span[i].last > ti) {
				ti = s->cache[ci].span[i].last;
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
		ftmap_put_last(s->cache + ci, f, t, ti);
	}

	/* same as _bitte_rtr_as_of_now() now */
	FTMAP_FOREACH(i, s->cache + ci) {
		fact[res++] = (mut_oid_t)s->cache[ci].span[i].last;
		if (UNLIKELY(res >= nfact)) {
			break;
		}
	}
	return res;
}

static __attribute__((nonnull(1))) echs_range_t
_bitte_trend(const struct _stor_s *s, mut_tid_t t)
{
/* return the transaction interval for transaction T */
	const mut_oid_t f = _stor_get_fact(s, t);
	const mut_tid_t hi = ftmap_get_last(&s->live, f);
	echs_range_t res;

	assert(!TID_NOT_FOUND_P(hi));
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
	struct _stor_s *res;

	if (fn != NULL) {
		return NULL;
	}
	/* mem store they want, good */
	res = calloc(1, sizeof(struct _stor_s));
	if (init_ftmap(&res->live) < 0) {
		goto fucked;
	}
	return (mut_stor_t)res;
fucked:
	free(res);
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
	fini_ftmap(&_s->live);
	for (size_t i = 0U; i < _s->ncache; i++) {
		fini_ftmap(_s->cache + i);
	}
	if (s != NULL) {
		free(_s);
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
	if (UNLIKELY(tmln_chkz(&_s->tmln, 1U) < 0)) {
		return -1;
	}
	/* stamp off then */
	with (echs_instant_t t = echs_now()) {
		const size_t it = _s->tmln.ntrans++;

		_s->tmln.trans[it] = t;
		_s->tmln.facts[it] = fact;
		_s->tmln.valids[it] = valid;

		ftmap_put_last(&_s->live, fact, t, it);
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

	const mut_tid_t t = ftmap_get_last(&_s->live, fact);

	if (TID_NOT_FOUND_P(t)) {
		/* he's dead already */
		return -1;
	} else if (echs_nul_range_p(_stor_get_valid(_s, t))) {
		/* dead already, just */
		return -1;
	}
	/* otherwise kill him */
	/* check for resize */
	if (UNLIKELY(tmln_chkz(&_s->tmln, 1U) < 0)) {
		return -1;
	}
	/* stamp him off */
	with (echs_instant_t now = echs_now()) {
		const size_t it = _s->tmln.ntrans++;

		_s->tmln.trans[it] = now;
		_s->tmln.facts[it] = fact;
		_s->tmln.valids[it] = echs_nul_range();

		ftmap_put_last(&_s->live, fact, now, it);
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
		return _get_as_of_now(_s, fact);
	}
	/* otherwise proceed to scan */
	return _get_as_of_then(_s, fact, as_of);
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

	const mut_tid_t ot = ftmap_get_last(&_s->live, old);

	if (TID_NOT_FOUND_P(ot)) {
		/* must be dead, cannot supersede */
		return -1;
	}

	/* otherwise make room for some (i.e. 2) insertions */
	if (UNLIKELY(tmln_chkz(&_s->tmln, 2U) < 0)) {
		return -1;
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

			ftmap_put_last(&_s->live, old, t, it);
		}
		/* new guy now */
		if (new != MUT_NUL_OID) {
			const size_t it = _s->tmln.ntrans++;

			/* bang to timeline */
			_s->tmln.trans[it] = t;
			_s->tmln.facts[it] = new;
			_s->tmln.valids[it] = valid;

			ftmap_put_last(&_s->live, new, t, it);
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

	const mut_tid_t t1 = ftmap_get_first(&_s->live, fact);

	if (UNLIKELY(TID_NOT_FOUND_P(t1))) {
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
