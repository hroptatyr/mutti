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

/* fact offsets */
typedef struct {
	mut_tid_t _1st;
	mut_tid_t last;
} mut_fof_t;

/* this is one page in our file */
struct page_s {
	echs_instant_t trans[NXPP];
	mut_oid_t facts[NXPP];
	echs_range_t valids[NXPP];
	mut_oid_t fht[NXPP + NXPP];
	mut_fof_t fof[NXPP];
};

#define TID_NOT_FOUND		((mut_tid_t)-1)
#define TID_NOT_FOUND_P(x)	(!~(mut_tid_t)(x))

#define FACT_NOT_FOUND		((struct ftnd_s){TID_NOT_FOUND})
#define FACT_NOT_FOUND_P(x)	(TID_NOT_FOUND_P((x)._1st))

#define PNO_NOT_CACHED		((size_t)-1)
#define PNO_NOT_CACHED_P(x)	(!~(size_t)(x))

#define ECHS_RANGE_FROM(x)	((echs_range_t){x, ECHS_UNTIL_CHANGED})

/* we promised to define the mut_stor_s struct */
typedef struct _stor_s {
	struct mut_stor_s super;
	size_t ntrans;
	/* handle */
	int fd;
	/* current page */
	struct page_s *restrict curp;
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
typedef uint32_t ftnd_t;

struct ftnd_s {
	mut_oid_t fact;
	ftnd_t left;
	ftnd_t rght:31;
	ftnd_t redp:1;
};

struct fttr_s {
	mut_oid_t nfacts;
	ftnd_t root;
	ftnd_t:32;
};

/* left accessors */
static inline struct ftnd_s*
rbt_node_new(struct ftnd_s *restrict base, ftnd_t of)
{
	base[of].left = 0U;
	base[of].rght = 0U;
	base[of].redp = 1U;
	return base + of;
}

static inline void
rb_new(struct fttr_s *restrict x)
{
	memset(x, 0, sizeof(*x));
	return;
}

/* internal util macros */
static ftnd_t
rbtn_first(const struct fttr_s *t)
{
	const struct ftnd_s *const base = (const struct ftnd_s*)t;
	ftnd_t r;

	if (UNLIKELY(!(r = t->root))) {
		return 0U;
	}
	for (ftnd_t tmp; (tmp = base[r].left); r = tmp);
	return r;
}

static ftnd_t
rbtn_last(const struct fttr_s *t)
{
	const struct ftnd_s *const base = (const struct ftnd_s*)t;
	ftnd_t r;

	if (UNLIKELY(!(r = t->root))) {
		return 0U;
	}
	for (ftnd_t tmp; (tmp = base[r].rght); r = tmp);
	return r;
}

static ftnd_t
rbtn_rot_left(struct fttr_s *restrict t, ftnd_t node)
{
	struct ftnd_s *const base = (struct ftnd_s*)t;
	ftnd_t res = base[node].rght;

	base[node].rght = base[res].left;
	base[res].left = node;
	return res;
}

static ftnd_t
rbtn_rot_rght(struct fttr_s *restrict t, ftnd_t node)
{
	struct ftnd_s *const base = (struct ftnd_s*)t;
	ftnd_t res = base[node].left;

	base[node].left = base[res].rght;
	base[res].rght = node;
	return res;
}

static inline __attribute__((const, pure)) int
rb_cmp(mut_oid_t a, mut_oid_t b)
{
	return a - b;
}

static ftnd_t
rb_search(const struct fttr_s *t, mut_oid_t fact)
{
	const struct ftnd_s *const base = (const struct ftnd_s*)t;
	ftnd_t ro = t->root;
	int cmp;

	while (ro && (cmp = rb_cmp(fact, base[ro].fact))) {
		if (cmp < 0) {
			ro = base[ro].left;
		} else /*if (cmp > 0)*/ {
			ro = base[ro].rght;
		}
	}
	return ro;
}

static void
rb_insert(struct fttr_s *restrict t, struct ftnd_s *restrict fn)
{
	struct ftnd_s *const restrict base = (struct ftnd_s*)t;
	struct {
		ftnd_t no;
		int cmp;
	} path[sizeof(void*) << 4U], *pp = path;

	/* wind */
	path->no = t->root;
	for (pp->no = t->root; pp->no; pp++) {
		int cmp = pp->cmp = rb_cmp(fn->fact, base[pp->no].fact);

		assert(cmp != 0);
		if (cmp < 0) {
			pp[1U].no = base[pp->no].left;
		} else /*if (cmp > 0)*/ {
			pp[1U].no = base[pp->no].rght;
		}
	}
	/* invariant: pp->no == 0U */
	pp->no = fn - base;

	/* unwind */
	for (pp--; pp >= path; pp--) {
		ftnd_t cur = pp->no;

		if (pp->cmp < 0) {
			ftnd_t left = pp[1].no;
			base[cur].left = left;
			if (base[left].redp) {
				ftnd_t leftleft = base[left].left;
				if (base[leftleft].redp) {
					/* blacken leftleft */
					base[leftleft].redp = 0U;
					cur = rbtn_rot_rght(t, cur);
				}
			} else {
				return;
			}
		} else /*if (cmp > 0)*/ {
			ftnd_t rght = pp[1].no;
			base[cur].rght = rght;
			if (base[rght].redp) {
				ftnd_t left = base[cur].left;
				if (base[left].redp) {
					/* split 4-node */
					base[left].redp = 0U;
					base[rght].redp = 0U;
					base[cur].redp = 1U;
				} else {
					/* lean left */
					ftnd_t tmp;

					tmp = rbtn_rot_left(t, cur);
					base[tmp].redp = base[cur].redp;
					base[cur].redp = 1U;
					cur = tmp;
				}
			} else {
				return;
			}
		}
		pp->no = cur;
	}
	/* set root and paint it black */
	t->root = path->no;
	base[t->root].redp = 0U;
	return;
}


/* ftmaps on top of red-black trees */
typedef struct {
	struct fttr_s tree[NXPP];
	mut_fof_t fof[NXPP];
} *ftmap_t;

static int
init_ftmap(ftmap_t m)
{
	rb_new(m->tree);
	return 0;
}

static void
fini_ftmap(ftmap_t UNUSED(m))
{
	return;
}

static inline mut_fof_t*
ftmap_get(ftmap_t m, mut_oid_t fact)
{
	ftnd_t ro = rb_search(m->tree, fact);

	if (UNLIKELY(!ro)) {
		return NULL;
	}
	return m->fof + ro;
}

static inline mut_fof_t*
ftmap_put(ftmap_t m, mut_oid_t fact)
{
	struct ftnd_s *f =
		rbt_node_new((struct ftnd_s*)m->tree, ++m->tree->nfacts);

	f->fact = fact;
	rb_insert(m->tree, f);
	return m->fof + m->tree->nfacts;
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
ftmap_put_last(ftmap_t m, mut_oid_t fact, echs_instant_t t, mut_tid_t last)
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


/* meta */
static __attribute__((nonnull(1))) echs_bitmp_t
_get_as_of_now(_stor_t s, mut_oid_t fact)
{
	mut_tid_t t = ftmap_get_last((void*)&s->curp->fht, fact);

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
		ftmap_put_last(s->cache + ci, f, t, ti);
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
	} else if (UNLIKELY((fd = open(fn, fl & ~O_TRUNC, 0666)) < 0)) {
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
	res->ntrans = st.st_size / PGSZ * NXPP;
	/* get some more initialisation work done */
	init_ftmap(&res->curp->fht);
	return (mut_stor_t)res;
clo:
	close(fd);
	return NULL;
}

static void
_close(mut_stor_t s)
{
	_stor_t _s = (_stor_t)s;

	/* munmap current page */
	munmap(_s->curp, PGSZ);
	/* munmap any cached pages */
	for (size_t i = countof(_s->cachp) - _s->ncach;
	     i < countof(_s->cachp); i++) {
		(void)munmap(deconst(_s->cachp[i]), PGSZ);
		_s->cachp[i] = NULL;
		_s->cachn[i] = 0U;
	}
	return;
}

static int
_put(mut_stor_t s, mut_oid_t fact, echs_range_t valid)
{
	_stor_t _s = (_stor_t)s;

	if (UNLIKELY(fact == MUT_NUL_OID)) {
		return -1;
	}
	/* stamp off then */
	with (echs_instant_t t = echs_now()) {
		const size_t it = _s->ntrans++ % NXPP;

		_s->curp->trans[it] = t;
		_s->curp->facts[it] = fact;
		_s->curp->valids[it] = valid;

		ftmap_put_last((void*)&_s->curp->fht, fact, t, it);
	}
	return 0;
}

static int
_rem(mut_stor_t s, mut_oid_t fact)
{
	_stor_t _s = (_stor_t)s;
	const mut_tid_t t = ftmap_get_last((void*)&_s->curp->fht, fact);

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

		ftmap_put_last((void*)&_s->curp->fht, fact, now, it);
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
	const mut_tid_t ot = ftmap_get_last((void*)&_s->curp->fht, old);

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
