/*** bitte-dsk.c -- bitemporal API using a timeline index
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
/***
 * The disk format:
 * - pages are 64k (that's 16 4k pages)
 * +-------+-------+---------------+
 * | 1024T | 1024F |     1024V     |
 * +-------+-------+-------+-------+
 * | <=1024F+1024_ | <=1024O+1024O |
 * +---------------+---------------+
 * The bottom layer is a red-black-table really, with at most 1024 facts and
 * their first and last transaction within THAT page. */
#if defined HAVE_CONFIG_H
# include "config.h"
#endif	/* HAVE_CONFIG_H */
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <string.h>
#include <assert.h>
#include "bitte.h"
#define IN_DSO(x)	bitte_dsk_LTX_##x
#include "bitte-private.h"
#include "nifty.h"

/* let's use 64k pages */
#define PGSZ	(64U * 1024U)
/* basic unit, number of X per page */
#define NXPP	(PGSZ / sizeof(echs_instant_t) / 8U)

#if !defined MAP_ANON && defined MAP_ANONYMOUS
# define MAP_ANON	MAP_ANONYMOUS
#elif !defined MAP_ANON
# define MAP_ANON	(0x1000U)
#endif	/* !MAP_ANON */
#define PROT_RW		(PROT_READ | PROT_WRITE)
#define MAP_MEM		(MAP_PRIVATE | MAP_ANON)

/* a transaction id is simply an offset in the STOR SoA */
typedef uintptr_t mut_tid_t;
/* page number */
typedef uintptr_t mut_pno_t;

#define TID_NOT_FOUND		((mut_tid_t)-1)
#define TID_NOT_FOUND_P(x)	(!~(mut_tid_t)(x))

#define PNO_NOT_CACHED		((size_t)-1)
#define PNO_NOT_CACHED_P(x)	(!~(size_t)(x))

#define ECHS_RANGE_FROM(x)	((echs_range_t){x, ECHS_UNTIL_CHANGED})

/* fact offsets */
typedef struct {
	mut_tid_t _1st;
	mut_tid_t last;
} mut_fof_t;

/* we have to be NXPP * sizeof(echs_instant_t) long */
struct pphdr_s {
	char magic[4U];
	uint16_t ver;
	uint16_t:16;
	uint32_t ntrans;
	uint32_t nftm_facts;

	uint64_t nfacts;
	uint32_t:32;
	uint32_t:32;

	mut_oid_t space[NXPP - 4U];
};

/* this is one page in our file */
struct page_s {
	struct pphdr_s hdr;
	echs_instant_t trans[NXPP];
	echs_range_t valids[NXPP];
	mut_oid_t facts[NXPP];
	mut_oid_t ftm[NXPP];
	mut_fof_t fof[NXPP];
};

/* we promised to define the mut_stor_s struct */
typedef struct _stor_s {
	struct mut_stor_s super;
	size_t ntrans;
	/* handle */
	int fd;
	/* current page */
	struct page_s *restrict curp;
	/* fact-trans map, laters */
	struct ftmap_s *ftm;
	/* page cache */
	const struct page_s *cachp[64U];
	mut_pno_t cachn[64U];
	size_t ncach;
} *_stor_t;


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


static inline __attribute__((nonnull(1), pure)) size_t
_stor_ntrans(const struct _stor_s *s)
{
	return s->ntrans;
}

static inline __attribute__((nonnull(1), pure)) size_t
_stor_cached_p(struct _stor_s *restrict s, mut_pno_t p)
{
	for (size_t i = countof(s->cachn) - s->ncach;
	     i < countof(s->cachn); i++) {
		if (s->cachn[i] == p) {
			return i;
		}
	}
	return PNO_NOT_CACHED;
}

static const struct page_s*
_stor_load_page(struct _stor_s *restrict s, mut_pno_t p)
{
	size_t pi;

	if (PNO_NOT_CACHED_P((pi = _stor_cached_p(s, p)))) {
		const size_t of = p * PGSZ;
		void *pp = mmap(NULL, PGSZ, PROT_READ, MAP_SHARED, s->fd, of);
		if (UNLIKELY(pp == MAP_FAILED)) {
			return NULL;
		} else if (UNLIKELY(s->ncach >= countof(s->cachp))) {
			/* grml, have to evict the least recently used page */
			munmap(deconst(s->cachp[countof(s->cachp) - 1U]), PGSZ);
			/* move everything 1 item up */
			memmove(s->cachp + 1U, s->cachp,
				(countof(s->cachp) - 1U) * sizeof(*s->cachp));
			memmove(s->cachn + 1U, s->cachn,
				(countof(s->cachn) - 1U) * sizeof(*s->cachn));
			pi = 0U;
		} else {
			pi = countof(s->cachp) - ++s->ncach;
		}
		/* cache page P */
		s->cachn[pi] = p;
		s->cachp[pi] = pp;
	}
	return s->cachp[pi];
}

static inline __attribute__((nonnull(1))) echs_instant_t
_stor_get_trans(struct _stor_s *restrict s, mut_tid_t t)
{
	/* find the page first */
	const mut_pno_t pno = t / NXPP;
	const struct page_s *p = _stor_load_page(s, pno);
	return p->trans[t % NXPP];
}

static inline __attribute__((nonnull(1))) mut_oid_t
_stor_get_fact(struct _stor_s *restrict s, mut_tid_t t)
{
	/* find the page first */
	const mut_pno_t pno = t / NXPP;
	const struct page_s *p = _stor_load_page(s, pno);
	return p->facts[t % NXPP];
}

static inline __attribute__((nonnull(1))) echs_range_t
_stor_get_valid(struct _stor_s *restrict s, mut_tid_t t)
{
	/* find the page first */
	const mut_pno_t pno = t / NXPP;
	const struct page_s *p = _stor_load_page(s, pno);
	return p->valids[t % NXPP];
}


/* ftmap fiddling, materialised rb.h */
#include "rb.h"

static inline __attribute__((const, pure)) int
rb_cmp(mut_oid_t)(mut_oid_t a, mut_oid_t b)
{
	return a - b;
}

#include "rb-fact.c"

/* ftmaps on top of red-black trees */
typedef struct ftmap_s {
	size_t zfacts;
	mut_fof_t *fof;
	/* vla! */
	struct RBTR_S(mut_oid_t) rbt;
} *ftmap_t;

static int
clr_ftmap(ftmap_t m)
{
	memset(&m->rbt, -1, (m->zfacts + 1U) * sizeof(*m->rbt.base));
	m->rbt.nfacts = 0U;
	return 0;
}

static ftmap_t
make_ftmap(size_t nnd)
{
	ftmap_t res = malloc(sizeof(*res) + nnd * sizeof(*res->rbt.base));
	void *fof;

	if (UNLIKELY(res == NULL)) {
		return NULL;
	} else if (UNLIKELY((fof = malloc(nnd * sizeof(*res->fof))) == NULL)) {
		free(res);
		return NULL;
	}
	/* go ahead initialising */
	res->zfacts = nnd;
	res->fof = fof;
	clr_ftmap(res);
	return res;
}

static void
free_ftmap(ftmap_t m)
{
	if (LIKELY(m->fof != NULL)) {
		free(m->fof);
	}
	free(m);
	return;
}

static rbnd_t
ftmap_make_node(ftmap_t m)
{
	rbnd_t res = m->rbt.nfacts++;
	assert(res < (rbnd_t)NXPP);
	return res;
}

static inline mut_fof_t*
ftmap_get(ftmap_t m, mut_oid_t fact)
{
	rbnd_t ro = rb_search(mut_oid_t)(&m->rbt, fact);

	if (UNLIKELY(RBND_NIL_P(ro))) {
		return NULL;
	}
	return m->fof + ro;
}

static inline mut_fof_t*
ftmap_put(ftmap_t m, mut_oid_t fact)
{
	rbnd_t nd = ftmap_make_node(m);

	rb_insert(mut_oid_t)(&m->rbt, nd, fact);
	return m->fof + nd;
}

static __attribute__((nonnull(1), flatten)) mut_tid_t
ftmap_get_last(ftmap_t m, mut_oid_t fact)
{
	const mut_fof_t *f = ftmap_get(m, fact);

	if (UNLIKELY(f == NULL)) {
		return TID_NOT_FOUND;
	}
	return f->last;
}

static __attribute__((nonnull(1), flatten)) mut_tid_t
ftmap_get_first(ftmap_t m, mut_oid_t fact)
{
	const mut_fof_t *f = ftmap_get(m, fact);

	if (UNLIKELY(f == NULL)) {
		return TID_NOT_FOUND;
	}
	return f->_1st;
}

static int
ftmap_put_last(ftmap_t m, mut_oid_t fact, mut_tid_t last)
{
	mut_fof_t *f = ftmap_get(m, fact);

	if (UNLIKELY(f == NULL)) {
		/* prep the key */
		f = ftmap_put(m, fact);
		f->_1st = last;
	}
	f->last = last;
	return 0;
}

static int
bang_ftmap(struct page_s *restrict tgt, const struct ftmap_s *m)
{
	size_t nftm_facts;

	with (mut_oid_t *restrict tp = tgt->ftm) {
		FOREACH_KEY(mut_oid_t, f, &m->rbt) {
			*tp++ = f;
		}
		nftm_facts = tp - tgt->ftm;
	}
	with (mut_fof_t *restrict fp = tgt->fof) {
		FOREACH_RBN(n, mut_oid_t, &m->rbt) {
			*fp++ = m->fof[n];
		}
	}
	/* fiddle with headers */
	tgt->hdr.nftm_facts = nftm_facts;
	return 0;
}

static int
bang_hdr(struct page_s *restrict tgt, size_t ntrans)
{
/* better place for this? */
	memcpy(tgt->hdr.magic, "MUT\002", 4U);
	tgt->hdr.ver = 1;
	tgt->hdr.ntrans = ntrans % NXPP;
	return 0;
}


/* ftmaps in materialised pages */
static inline const mut_fof_t*
page_ftm_get(const struct page_s *p, mut_oid_t fact)
{
/* a page always has NXPP facts in sorted order
 * we now do a simple bsearch(3) till we find the right block
 * a block coincides with the cacheline size (64b) */
	uint_fast32_t lo = 0U;
	uint_fast32_t hi = p->hdr.nftm_facts;

	/* perform a bsearch(3) until we're in the range of a
	 * cacheline, then go to linear scanning */
	while (hi - lo > 64U / sizeof(fact)) {
		uint_fast32_t mid = (lo + hi) / 2U;

		if (p->ftm[mid] > fact) {
			hi = mid;
		} else {
			lo = mid;
		}
	}
	/* at last we just scan the whole cacheline, <=8 mut_oid_t objects */
	assert(hi - lo <= 64U / sizeof(fact));
	for (; lo < hi; lo++) {
		if (p->ftm[lo] == fact) {
			return p->fof + lo;
		}
	}
	return NULL;
}

static __attribute__((nonnull(1), flatten)) mut_tid_t
page_ftm_get_last(const struct page_s *p, mut_oid_t fact)
{
	const mut_fof_t *fof = page_ftm_get(p, fact);

	if (fof == NULL) {
		return TID_NOT_FOUND;
	}
	return fof->last;
}


/* meta */
static __attribute__((nonnull(1))) echs_bitmp_t
_get_as_of_now(_stor_t s, mut_oid_t fact)
{
	mut_tid_t t = ftmap_get_last(s->ftm, fact);

	if (!TID_NOT_FOUND_P(t)) {
		goto tid_found;
	}
	/* time to hit the caches now */
	for (mut_pno_t pi = s->ntrans / NXPP; pi-- > 0U;) {
		const struct page_s *p = _stor_load_page(s, pi);

		if (!TID_NOT_FOUND_P(t = page_ftm_get_last(p, fact))) {
			goto tid_found;
		}
	}
	return ECHS_NUL_BITMP;

tid_found:
	/* yay, dead or alive, it's in our books */
	return (echs_bitmp_t){
		_stor_get_valid(s, t),
		ECHS_RANGE_FROM(_stor_get_trans(s, t)),
	};
}

static __attribute__((nonnull(1))) echs_bitmp_t
_get_as_of_then(_stor_t s, mut_oid_t fact, echs_instant_t as_of)
{
/* we do a backward-forward scan:
 * on a given page, look for the first fiddle with FACT,
 * if > AS_OF, go back one page
 * if <= AS_OF traverse forwards to find the FIRST_AFTER */
	const mut_fof_t *fof = ftmap_get(s->ftm, fact);
	mut_tid_t i_last_before = TID_NOT_FOUND;
	mut_tid_t i_first_after = TID_NOT_FOUND;
	echs_instant_t ti_before;
	echs_instant_t ti_after;

	if (fof != NULL) {
		/* lucky! */
		i_last_before = fof->_1st;
		ti_before = _stor_get_trans(s, i_last_before);

		if (echs_instant_le_p(ti_before, as_of)) {
			if (i_last_before == fof->last) {
				/* oh my god, we nailed it */
				goto found;
			}
			/* knew it, it was too good to be true */
			goto fwd_scan;
		}
	}
	/* either FACT isn't featured on the current page or
	 * AS_OF < TRANS(i_last_before)
	 * either way, we have to consult the previous pages */
	for (mut_pno_t pi = s->ntrans / NXPP; pi-- > 0U;) {
		const struct page_s *p = _stor_load_page(s, pi);
		echs_instant_t ti;
		mut_tid_t i;

		if ((fof = page_ftm_get(p, fact)) == NULL) {
			continue;
		}
		/* otherwise let's see if we can initiate the forward scan */
		i_last_before = fof->_1st;
		ti_before = _stor_get_trans(s, i_last_before);

		if (!echs_instant_le_p(ti_before, as_of)) {
			/* maybe we're lucky next time
			 * however we'll track this instance on the way down
			 * so we don't have to look for the first_after
			 * bound later on */
			i_first_after = i_last_before;
			ti_after = ti_before;
			continue;
		}

	fwd_scan:
		/* scan forwards, we know that f->last >= AS_OF
		 * otherwise we'd be in _get_as_of_now() */
		for (i = i_last_before;
		     i < fof->last &&
			     (ti = _stor_get_trans(s, i),
			      echs_instant_le_p(ti, as_of)); i++) {
			if (fact == _stor_get_fact(s, i)) {
				i_last_before = i;
				ti_before = ti;
			}
		}
		/* found the definite before trans
		 * keep scanning, because the fact might have been
		 * superseded by a more recent transaction */
		for (; i < fof->last; i++) {
			if (fact == _stor_get_fact(s, i)) {
				i_first_after = i;
				ti_after = _stor_get_trans(s, i);
				break;
			}
		}
		goto found;
	}
	return ECHS_NUL_BITMP;

found:
	/* now I_FIRST_AFTER should hold FACT_NOT_FOUND or the index of
	 * the next fiddle with FACT on or after AS_OF */
	if (TID_NOT_FOUND_P(i_first_after)) {
		/* must be open-ended */
		return (echs_bitmp_t){
			_stor_get_valid(s, i_last_before),
			ECHS_RANGE_FROM(ti_before)
		};
	}

	/* yay, dead or alive, it's in our books */
	/* otherwise it's bounded by trans[I_FIRST_AFTER] */
	return (echs_bitmp_t){
		_stor_get_valid(s, i_last_before),
		(echs_range_t){ti_before, ti_after},
	};
}

static __attribute__((nonnull(1))) size_t
_bitte_rtr_as_of_now(_stor_t s, mut_oid_t *restrict fact, size_t nfact)
{
/* current transaction time-slice, very naive */
	size_t res = 0U;

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

#if 0
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
		ftmap_put_last(s->cache + ci, f, ti);
	}

	/* same as _bitte_rtr_as_of_now() now */
	FTMAP_FOREACH(i, s->cache + ci) {
		fact[res++] = (mut_oid_t)s->cache[ci].span[i].last;
		if (UNLIKELY(res >= nfact)) {
			break;
		}
	}
#endif
	return res;
}

static __attribute__((nonnull(1))) echs_range_t
_bitte_trend(const struct _stor_s *s, mut_tid_t t)
{
/* return the transaction interval for transaction T */
#if 0
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
#endif
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
_open(const char *fn, int fl)
{
	struct _stor_s *res;
	void *curp = NULL;
	struct stat st;
	int fd;

	if (UNLIKELY(fn == NULL)) {
		return NULL;
	} else if (UNLIKELY((fd = open(fn, fl, 0666)) < 0)) {
		return NULL;
	} else if (!(fl & O_RDWR)) {
		/* aaah, read-only, aye aye */
		;
	} else if ((fl & O_TRUNC) && UNLIKELY(ftruncate(fd, PGSZ) < 0)) {
		goto clo;
	} else if (UNLIKELY(fstat(fd, &st) < 0)) {
		goto clo;
	} else if (UNLIKELY(!st.st_size)) {
		goto clo;
	} else if ((
		{
			/* load last page to scribble in */
			const off_t of = (st.st_size - 1U) & ~(PGSZ - 1U);
			curp = mmap(NULL, PGSZ, PROT_RW, MAP_SHARED, fd, of);
		}) == MAP_FAILED) {
		goto clo;
	}
	/* mem store they want, good */
	res = calloc(1, sizeof(struct _stor_s));
	res->fd = fd;
	res->curp = curp;
	/* quickly guess the number of transactions */
	res->ntrans = (st.st_size - 1U) / PGSZ * NXPP;
	/* get some more initialisation work done */
	res->ftm = make_ftmap(NXPP);
	return (mut_stor_t)res;
clo:
	close(fd);
	return NULL;
}

static int
_materialise(_stor_t _s)
{
/* bring current page into form for permanent storage */
	int rc = 0;

	rc += bang_ftmap(_s->curp, _s->ftm);
	/* header fiddling */
	rc += bang_hdr(_s->curp, _s->ntrans);
	return rc;
}

static void
_close(mut_stor_t s)
{
	_stor_t _s = (_stor_t)s;

	/* materialise */
	_materialise(_s);
	/* finalise the ftmap */
	free_ftmap(_s->ftm);
	/* munmap current page */
	munmap(_s->curp, PGSZ);
	/* munmap any cached pages */
	for (size_t i = countof(_s->cachp) - _s->ncach;
	     i < countof(_s->cachp); i++) {
		(void)munmap(deconst(_s->cachp[i]), PGSZ);
		_s->cachp[i] = NULL;
		_s->cachn[i] = 0U;
	}
	if (s != NULL) {
		free(_s);
	}
	return;
}

static int
_extend(_stor_t _s)
{
/* munmap current page, extend the file and map a new current page */
	munmap(_s->curp, PGSZ);
	_s->curp = NULL;

	/* calc new size */
	with (const off_t ol = _s->ntrans / NXPP * PGSZ, nu = ol + PGSZ) {
		void *curp;

		if (UNLIKELY(ftruncate(_s->fd, nu) < 0)) {
			return -1;
		}

		/* load the trunc'd page to scribble in */
		curp = mmap(NULL, PGSZ, PROT_RW, MAP_SHARED, _s->fd, ol);
		if (UNLIKELY(curp == MAP_FAILED)) {
			return -1;
		}

		/* otherwise this is the latest shit */
		_s->curp = curp;
		clr_ftmap(_s->ftm);
	}
	return 0;
}

static int
_put(mut_stor_t s, mut_oid_t fact, echs_range_t valid)
{
	_stor_t _s = (_stor_t)s;
	int rc = -1;

	if (UNLIKELY(fact == MUT_NUL_OID)) {
		return -1;
	}
	/* stamp off then */
	with (echs_instant_t t = echs_now()) {
		const size_t it = _s->ntrans++ % NXPP;

		_s->curp->trans[it] = t;
		_s->curp->facts[it] = fact;
		_s->curp->valids[it] = valid;

		ftmap_put_last(_s->ftm, fact, it);
	}
	if (UNLIKELY(!(_s->ntrans % NXPP))) {
		/* current page needs materialising */
		rc += _materialise(_s);
		rc += _extend(_s);
	}
	return rc;
}

static int
_rem(mut_stor_t s, mut_oid_t fact)
{
	_stor_t _s = (_stor_t)s;
	const mut_tid_t t = ftmap_get_last(_s->ftm, fact);

	if (TID_NOT_FOUND_P(t)) {
		/* he's dead already */
		return -1;
	} else if (echs_nul_range_p(_stor_get_valid(_s, t))) {
		/* dead already, just */
		return -1;
	}
	/* otherwise kill him */
	with (echs_instant_t now = echs_now()) {
		const size_t it = _s->ntrans++ % NXPP;

		_s->curp->trans[it] = now;
		_s->curp->facts[it] = fact;
		_s->curp->valids[it] = echs_nul_range();

		ftmap_put_last(_s->ftm, fact, it);
	}
	return 0;
}

static echs_bitmp_t
_get(mut_stor_t s, mut_oid_t fact, echs_instant_t as_of)
{
	_stor_t _s = (_stor_t)s;

	if (UNLIKELY(!_s->ntrans || fact == MUT_NUL_OID)) {
		/* no transactions in this store, trivial*/
		return ECHS_NUL_BITMP;
	}
	/* if AS_OF is >= the stamp of the last transaction, just use
	 * the live table. */
	else if (true/*echs_instant_le_p(_s->live.trans, as_of)*/) {
		return _get_as_of_now(_s, fact);
	}
	/* otherwise proceed to scan */
	return _get_as_of_then(_s, fact, as_of);
}

static int
_supersede(
	mut_stor_t s, mut_oid_t old, mut_oid_t new, echs_range_t valid)
{
	_stor_t _s = (_stor_t)s;
	const mut_tid_t ot = ftmap_get_last(_s->ftm, old);

#if 0
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

			ftmap_put_last(&_s->live, old, it);
		}
		/* new guy now */
		if (new != MUT_NUL_OID) {
			const size_t it = _s->tmln.ntrans++;

			/* bang to timeline */
			_s->tmln.trans[it] = t;
			_s->tmln.facts[it] = new;
			_s->tmln.valids[it] = valid;

			ftmap_put_last(&_s->live, new, it);
		}
	}
#endif
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

#if 0
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
#endif
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
#if 0
	_stor_t _s;

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
#endif
	return res;
}

static size_t
_hist(
	mut_stor_t s,
	echs_range_t *restrict trans, size_t ntrans,
	echs_range_t *restrict valid, mut_oid_t fact)
{
	size_t res = 0U;
#if 0
	const mut_tid_t t1 = ftmap_get_first(&_s->live, fact);
	_stor_t _s;

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
#endif
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

/* bitte-dsk.c ends here */
