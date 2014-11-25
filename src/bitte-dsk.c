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
	/* nxpp scale, implies size of page and offsets */
	uint32_t nxpp;
	uint32_t:32;
	/* 16B */

	uint32_t ntrans;
	uint32_t nfacts;
	uint32_t:32;
	uint32_t:32;
	/* 32B */

	uint32_t:32;
	uint32_t:32;
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


/* administrative stuff */
static int
bang_hdr(struct page_s *restrict tgt)
{
	memcpy(tgt->hdr.magic, "MUTB", 4U);
	tgt->hdr.ver = 1U;
	tgt->hdr.pty = 1U;
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

		if (LIKELY(ntrans)) {
			echs_instant_t last = _s->curp->trans[ntrans - 1U];

			if (!echs_instant_eq_p(t, last)) {
				const size_t o = _s->curp->tof[ntrans - 1U];
				size_t nbang = bang_tfmap(_s->curp, _s->tfm, o);

				_s->curp->tof[ntrans - 1U] = nbang;
				_s->curp->tof[ntrans] = nbang;
				goto init_trans;
			}
		} else {
		init_trans:
			_s->curp->trans[_s->curp->hdr.ntrans++] = t;
			clr_tfmap(_s->tfm);
		}
		/* bang */
		if (tfmap_put(_s->tfm, fact, valid) == 0) {
			_s->curp->hdr.nfacts++;
		}

		if (UNLIKELY(_s->curp->hdr.nfacts >= 2U * NXPP)) {
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

	if (UNLIKELY(fact == MUT_NUL_OID || !_s->curp->hdr.ntrans)) {
		/* no transactions in this store, trivial*/
		return ECHS_NUL_BITMP;
	}
	return ECHS_NUL_BITMP;
#if 0
	/* if AS_OF is >= the stamp of the last transaction, just use
	 * the live table. */
	else if (true/*echs_instant_le_p(_s->live.trans, as_of)*/) {
		return _get_as_of_now(_s, fact);
	}
	/* otherwise proceed to scan */
	return _get_as_of_then(_s, fact, as_of);
#endif
}


/* this sums up our implementation */
struct bitte_backend_s IN_DSO(backend) = {
	.mut_stor_open_f = _open,
	.mut_stor_close_f = _close,
	.bitte_get_f = _get,
	.bitte_put_f = _put,
};

/* bitte-dsk.c ends here */
