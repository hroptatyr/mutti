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
 * - Pages are 64k (that's 16 4k pages)
 * - There are 2 page types, transaction pages and checkpoint pages
 *
 * - Transaction pages look like:
 *   0       4k        8k        16k       32k       64k
 *   +-------+---------+---------+---------+---------+
 *   |  HDR  | 1024TOF |  1024T  |  2048F  |  2048V  |
 *   +-------+---------+---------+---------+---------+
 *   - Facts per transaction are sorted.
 *
 * - Checkpoint pages look like:
 *   0       4k        8k        16k       40k       64k
 *   +-------+---------+---------+---------+---------+
 *   |  HDR  | 1024VOF | 1024Vfr |  3072F  | 3072Vti |
 *   +-------+---------+---------+---------+---------+
 */
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
#include "rb.h"

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

/* global enumerator for transactions */
typedef uint64_t mut_tid_t;
/* global page number enumerator */
typedef uint64_t mut_pno_t;
/* counter type for numbers of facts per trans */
typedef uint32_t mut_tof_t;
typedef uint32_t mut_vof_t;

#define TOF_NOT_FOUND		((mut_tof_t)-1)
#define TOF_NOT_FOUND_P(x)	(!~(mut_tof_t)(x))

#define VOF_NOT_FOUND		((mut_vof_t)-1)
#define VOF_NOT_FOUND_P(x)	(!~(mut_vof_t)(x))

#define PNO_NOT_CACHED		((size_t)-1)
#define PNO_NOT_CACHED_P(x)	(!~(size_t)(x))

#define ECHS_RANGE_FROM(x)	((echs_range_t){x, ECHS_UNTIL_CHANGED})

/* we have to be 4k long */
struct pphdr_s {
	/* magic identifier, will be MUTB for bitemporal */
	char magic[4U];
	/* version number, also endian indicator */
	uint16_t ver;
	/* page type */
	uint16_t pty:16;
#define PTY_UNK		((uint16_t)0U)
#define PTY_TRANS	((uint16_t)1U)
#define PTY_CHKPT	((uint16_t)2U)
	/* nxpp scale, implies size of page and offsets */
	uint32_t nxpp;
	uint32_t:32;
	/* 16B */

	union {
		/* for trans pages */
		uint32_t ntrans;
		/* for checkpoint pages */
		uint32_t nvalids;
	};
	uint32_t nfacts;
	uint32_t:32;
	uint32_t:32;
	/* 32B */

	union {
		/* for trans pages */
		struct {
			uint32_t:32;
			uint32_t:32;
		};
		/* for checkpoint pages */
		echs_instant_t tfrom;
	};
	uint32_t:32;
	uint32_t:32;
	/* 48B */

	uint32_t:32;
	uint32_t:32;
	uint32_t:32;
	uint32_t:32;
	/* 64B */

	mut_oid_t space[512U - 8U];
	/* 4096B */
};

/* this is one page in our file */
struct page_s {
	struct pphdr_s hdr;
	union {
		mut_tof_t tof[NXPP];
		mut_vof_t vof[NXPP];
	};
	union {
		echs_instant_t trans[NXPP];
		echs_instant_t vfrom[NXPP];
	};
	union {
		struct {
			mut_oid_t facts[2U * NXPP];
			echs_range_t valids[2U * NXPP];
		};
		struct {
			mut_oid_t vfact[3U * NXPP];
			echs_instant_t vtill[3U * NXPP];
		};
	};
};

/* convenience struct similar to echs_bitmp_t */
typedef struct {
	echs_instant_t t;
	echs_range_t v;
} sesquitmp_t;
#define SESQUITMP_NOT_FOUND		((sesquitmp_t){ECHS_NUL_INSTANT})
#define SESQUITMP_NOT_FOUND_P(x)	(echs_nul_instant_p((x).t))

/* fact cache is essentially a fixed size hash-table
 * where we don't care about collisions or resizing */
typedef struct fcache_s {
#define FCACHE_SIZE	(16384U)
	mut_oid_t f[FCACHE_SIZE];
	sesquitmp_t tv[FCACHE_SIZE];
} *fcache_t;

/* we promised to define the mut_stor_s struct */
typedef struct _stor_s {
	struct mut_stor_s super;
	/* file size */
	off_t fz;
	/* handle */
	int fd;
	/* current page */
	struct page_s *restrict curp;
	/* current transactions */
	struct tfmap_s *tfm;
	/* latest stamp */
	echs_instant_t last;
	/* page cache, eventually a MRU cache */
	const struct page_s *cachp[64U];
	mut_pno_t cachn[64U];
	size_t ncach;
	/* fact cache */
	struct fcache_s fc[1U];
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


/* transaction-fact map */
static inline __attribute__((const, pure)) int
rb_cmp(mut_oid_t)(mut_oid_t a, mut_oid_t b)
{
	/* never admit equality, this allows us to store multiple values */
	return a < b ? -1 : a > b ? 1 : 0;
}

#include "rb-fact.c"

typedef struct tfmap_s {
	size_t zfacts;
	echs_range_t *v;
	/* vla */
	struct RBTR_S(mut_oid_t) rbt;
} *tfmap_t;

static int
clr_tfmap(tfmap_t m)
{
	memset(&m->rbt, -1, (m->zfacts + 1U) * sizeof(*m->rbt.base));
	m->rbt.nfacts = 0U;
	return 0;
}

static tfmap_t
make_tfmap(size_t nnd)
{
	tfmap_t res = malloc(sizeof(*res) + nnd * sizeof(*res->rbt.base));
	void *v;

	if (UNLIKELY(res == NULL)) {
		return NULL;
	} else if (UNLIKELY((v = malloc(nnd * sizeof(*res->v))) == NULL)) {
		free(res);
		return NULL;
	}
	/* go initialising */
	res->zfacts = nnd;
	res->v = v;
	clr_tfmap(res);
	return res;
}

static __attribute__((nonnull(1))) void
free_tfmap(tfmap_t m)
{
	if (LIKELY(m->v != NULL)) {
		free(m->v);
	}
	free(m);
	return;
}

static __attribute__((nonnull(1))) rbnd_t
tfmap_make_node(tfmap_t m)
{
	rbnd_t res = m->rbt.nfacts++;
	assert(res < (rbnd_t)m->zfacts);
	return res;
}

static inline __attribute__((nonnull(1))) echs_range_t*
tfmap_get(const struct tfmap_s *m, mut_oid_t fact)
{
	rbnd_t nd = rb_search(mut_oid_t)(&m->rbt, fact);
	return !RBND_NIL_P(nd) ? m->v + nd : NULL;
}

static inline  __attribute__((nonnull(1))) int
tfmap_put(tfmap_t m, mut_oid_t fact, echs_range_t valid)
{
	echs_range_t *v = tfmap_get(m, fact);

	if (LIKELY(v == NULL)) {
		rbnd_t nd = tfmap_make_node(m);

		rb_insert(mut_oid_t)(&m->rbt, nd, fact);
		m->v[nd] = valid;
		return 0;
	}
	/* otherwise update the validity */
	*v = valid;
	return 1;
}

static __attribute__((nonnull(1, 2))) size_t
bang_tfmap(struct page_s *restrict p, const struct tfmap_s *m, size_t o)
{
	size_t i;

	i = o;
	FOREACH_KEY(mut_oid_t, f, &m->rbt) {
		p->facts[i++] = f;
	}
	i = o;
	FOREACH_RBN(n, mut_oid_t, &m->rbt) {
		p->valids[i++] = m->v[n];
	}
	return i;
}


/* fact cache */
static int
fcache_put(fcache_t restrict fc, mut_oid_t f, sesquitmp_t tv)
{
	const size_t fo = f % FCACHE_SIZE;

	fc->f[fo] = f;
	fc->tv[fo] = tv;
	return 0;
}

static sesquitmp_t
fcache_get(const struct fcache_s *fc, mut_oid_t f)
{
	const size_t fo = f % FCACHE_SIZE;

	if (fc->f[fo] == f) {
		return fc->tv[fo];
	}
	return SESQUITMP_NOT_FOUND;
}


/* administrative stuff */
static int
bang_hdr(struct page_s *restrict tgt)
{
	memcpy(tgt->hdr.magic, "MUTB", 4U);
	tgt->hdr.ver = 1U;
	tgt->hdr.pty = PTY_TRANS;
	tgt->hdr.nxpp = NXPP;
	return 0;
}

static int
_materialise(_stor_t _s)
{
/* bring current page into form for permanent storage */
	int rc = 0;

	/* header fiddling */
	rc += bang_hdr(_s->curp);

	/* finalise the current tof/trans pair */
	with (size_t ntrans = _s->curp->hdr.ntrans) {
		if (LIKELY(ntrans)) {
			const size_t of = _s->curp->tof[ntrans - 1U];
			_s->curp->tof[ntrans - 1U] =
				bang_tfmap(_s->curp, _s->tfm, of);
		}
	}
	return rc;
}

static int
_extend(_stor_t _s)
{
/* munmap current page, extend the file and map a new current page */
	munmap(_s->curp, PGSZ);
	_s->curp = NULL;

	/* calc new size */
	with (const off_t ol = _s->fz, nu = ol + PGSZ) {
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
		_s->fz = nu;
		clr_tfmap(_s->tfm);
	}
	return 0;
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


/* page-wise stuff */
struct mut_tof_s {
	mut_tof_t t;
	mut_tof_t of;
};

static __attribute__((nonnull(1), pure)) mut_tof_t
xbsearch_fact(const mut_oid_t *f, size_t lo, size_t hi, mut_oid_t fact)
{
	/* bisection */
	while (hi - lo > 64U / sizeof(fact)) {
		size_t mid = (lo + hi) / 2U;

		if (f[mid] > fact) {
			hi = mid;
		} else {
			lo = mid;
		}
	}
	/* scan the cacheline, <=8 mut_oid_t objects */
	assert(hi - lo <= 64U / sizeof(fact));
	for (; lo < hi; lo++) {
		if (f[lo] == fact) {
			return lo;
		}
	}
	return TOF_NOT_FOUND;
}

static __attribute__((nonnull(1), pure)) struct mut_tof_s
page_get_tof(const struct page_s *p, mut_oid_t fact)
{
	for (size_t i = 0U, itof = 0U; i < p->hdr.ntrans; i++) {
		/* look at [itof, etof) */
		const size_t etof = p->tof[i];
		mut_tof_t o = xbsearch_fact(p->facts, itof, etof, fact);

		if (!TOF_NOT_FOUND_P(o)) {
			return (struct mut_tof_s){i, o};
		}
	}
	return (struct mut_tof_s){TOF_NOT_FOUND, TOF_NOT_FOUND};
}


static __attribute__((nonnull(1))) echs_bitmp_t
_get_as_of_now(_stor_t s, mut_oid_t fact)
{
/* retrieve one fact as of the current time stamp */
	sesquitmp_t tv;

	/* is it in our big fact-cache? */
	if (!SESQUITMP_NOT_FOUND_P((tv = fcache_get(s->fc, fact)))) {
		goto tid_found;
	}
	/* is it very very recent then? */
	if_with (echs_range_t *v = tfmap_get(s->tfm, fact), v != NULL) {
		/* how lucky are we? */
		tv.t = s->last;
		tv.v = *v;
		goto tid_found_cch;
	}
	/* just quickly go through curp */
	with (struct mut_tof_s curtof = page_get_tof(s->curp, fact)) {
		if (!TOF_NOT_FOUND_P(curtof.of)) {
			tv = (sesquitmp_t){
				s->curp->trans[curtof.t],
				s->curp->valids[curtof.of],
			};
			goto tid_found_cch;
		}
	}
	/* disastrous fail, try previous pages */
	for (mut_pno_t pi = s->fz / PGSZ; pi-- > 0U;) {
		const struct page_s *p = _stor_load_page(s, pi);
		struct mut_tof_s cchtof = page_get_tof(p, fact);

		if (!TOF_NOT_FOUND_P(cchtof.of)) {
			tv = (sesquitmp_t){
				p->trans[cchtof.t],
				p->valids[cchtof.of],
			};
			goto tid_found_cch;
		}
	}
	/* nah, next time maybe */
	return ECHS_NUL_BITMP;
tid_found_cch:
	/* quickly cache what we've got */
	fcache_put(s->fc, fact, tv);
tid_found:
	return (echs_bitmp_t){tv.v, ECHS_RANGE_FROM(tv.t)};
}

static __attribute__((nonnull(1))) echs_bitmp_t
_get_as_of_then(_stor_t s, mut_oid_t fact, echs_instant_t as_of)
{
/* retrieve one fact as of the time stamp specified */
	/* find the most recent checkpoint before/on AS_OF */
	mut_pno_t p = PNO_NOT_CACHED;

	/* build the vfmap */
	if (!PNO_NOT_CACHED_P(p)) {
		for (; p < s->fz / PGSZ; p++) {
			const struct page_s *cp = _stor_load_page(s, p);

			if (cp->hdr.pty != PTY_CHKPT) {
				break;
			}
			/* otherwise add more stuff to the vfmap */
			;
		}
	}

	/* now look for trans pages */
	for (; p < s->fz / PGSZ; p++) {
		const struct page_s *tp = _stor_load_page(s, p);

		if (tp->hdr.pty != PTY_TRANS) {
			break;
		} else if (!tp->hdr.ntrans) {
			break;
		}
		/* otherwise go through the transactions */
		for (mut_tof_t i = 0U; i < tp->hdr.ntrans; i++) {
			if (!echs_instant_lt_p(tp->trans[i], as_of)) {
				/* big break, we're too far */
				goto bang;
			}
			/* bang onto vfmap */
			;
		}
	}
bang:
	return ECHS_NUL_BITMP;
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
	res->fz = st.st_size;
	/* initialise our maps */
	res->tfm = make_tfmap(2U * NXPP);
	return (mut_stor_t)res;
clo:
	close(fd);
	return NULL;
}

static __attribute__((nonnull(1))) void
_close(mut_stor_t s)
{
	_stor_t _s = (_stor_t)s;

	/* materialise */
	_materialise(_s);
	/* cleaning up maps */
	free_tfmap(_s->tfm);
	/* munmap current page */
	munmap(_s->curp, PGSZ);
	free(_s);
	return;
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
		const size_t ntrans = _s->curp->hdr.ntrans;

		if (!echs_instant_eq_p(t, _s->last)) {
			if (LIKELY(ntrans)) {
				const size_t o = _s->curp->tof[ntrans - 1U];
				size_t nbang = bang_tfmap(_s->curp, _s->tfm, o);

				_s->curp->tof[ntrans - 1U] = nbang;
				_s->curp->tof[ntrans] = nbang;
			}
			/* otherwise just start the whole shebang */
			_s->curp->trans[_s->curp->hdr.ntrans++] = _s->last = t;
			clr_tfmap(_s->tfm);
		}
		/* bang */
		if (tfmap_put(_s->tfm, fact, valid) == 0) {
			_s->curp->hdr.nfacts++;
		}
		/* and cache this one */
		fcache_put(_s->fc, fact, (sesquitmp_t){t, valid});

		if (UNLIKELY(_s->curp->hdr.nfacts >= 2U * NXPP) ||
		    UNLIKELY(_s->curp->hdr.ntrans >= NXPP)) {
			/* current page needs materialising */
			rc += _materialise(_s);
			rc += _extend(_s);
		}
	}
	return rc;
}

static echs_bitmp_t
_get(mut_stor_t s, mut_oid_t fact, echs_instant_t as_of)
{
	_stor_t _s = (_stor_t)s;

	if (UNLIKELY(fact == MUT_NUL_OID)) {
		/* no transactions in this store, trivial*/
		return ECHS_NUL_BITMP;
	}
	/* if AS_OF is >= the stamp of the last transaction, just use
	 * the live table. */
	else if (echs_instant_le_p(_s->last, as_of)) {
		return _get_as_of_now(_s, fact);
	}
	/* otherwise proceed to scan */
	return _get_as_of_then(_s, fact, as_of);
}

static int
_ssd(mut_stor_t s, mut_oid_t old, mut_oid_t new, echs_range_t valid)
{
/* ssd (supersede) is short for
 * v <- _get(old)
 * _put(old, (echs_range_t){[v.from, valid.from)})
 * _put(new, valid) */
	echs_bitmp_t v = _get(s, old, ECHS_SOON);
	int rc = 0;

	if (!echs_nul_range_p(v.valid)) {
		return -1;
	}
	/* invalidate old cell */
	rc += _put(s, old, (echs_range_t){v.valid.from, valid.from});
	/* punch new cell */
	rc += _put(s, new, valid);
	return rc;
}

static int
_rem(mut_stor_t s, mut_oid_t fact)
{
/* rem is short for
 * v <- _get(fact)
 * if (v) _put(fact, (echs_range_t){[)}) */
	echs_bitmp_t v = _get(s, fact, ECHS_SOON);

	if (echs_nul_range_p(v.valid)) {
		/* dead already */
		return -1;
	}
	/* otherwise kill him */
	return _put(s, fact, ECHS_NUL_RANGE);
}


/* this sums up our implementation */
struct bitte_backend_s IN_DSO(backend) = {
	.mut_stor_open_f = _open,
	.mut_stor_close_f = _close,
	.bitte_get_f = _get,
	.bitte_put_f = _put,
	.bitte_rem_f = _rem,
	.bitte_supersede_f = _ssd,
};

/* bitte-dsk.c ends here */
