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
#include <stdint.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <string.h>
#include <fcntl.h>
#include <assert.h>
#include <db.h>
#include "bitte.h"
#define IN_DSO(x)	bitte_bdb_LTX_##x
#include "bitte-private.h"
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

#define SESQUITMP_NOT_FOUND		(ECHS_NUL_SESQUI)
#define SESQUITMP_NOT_FOUND_P(x)	(echs_nul_instant_p((x).trans))

typedef uint32_t mut_tid_t;

struct trns_s {
	echs_instant_t trins;
	mut_oid_t fact;
	echs_range_t valid;
};

struct fact_s {
	uintptr_t _1st;
	uintptr_t last;
};

#define TID_NOT_FOUND		((mut_tid_t)-1)
#define TID_NOT_FOUND_P(x)	(!~(mut_tid_t)(x))

#define FACT_NOT_FOUND		((struct fact_s){TID_NOT_FOUND})
#define FACT_NOT_FOUND_P(x)	(TID_NOT_FOUND_P((x)._1st))

#define ECHS_RANGE_FROM(x)	((echs_range_t){x, ECHS_UNTIL_CHANGED})

/* we promised to define the mut_stor_s struct */
typedef struct _stor_s {
	struct mut_stor_s super;
	DB *ft;
	DB *tr;
	echs_instant_t trins;
} *_stor_t;


static mut_tid_t
_get_tid(struct trns_s *restrict t, DB *trm, mut_tid_t i)
{
	DBT fk = {&i, sizeof(i)};
	DBT fv = {.data = t, .ulen = sizeof(*t), .flags = DB_DBT_USERMEM};

	switch (trm->get(trm, NULL, &fk, &fv, 0)) {
	case 0:
		/* found */
		return i;
	default:
		break;
	}
	return TID_NOT_FOUND;
}

static echs_sesqui_t
_get_fact(DB *ftm, mut_oid_t fact)
{
	echs_sesqui_t res;
	DBT fk = {&fact, sizeof(fact)};
	DBT fv = {.data = &res, .ulen = sizeof(res), .flags = DB_DBT_USERMEM};

	switch (ftm->get(ftm, NULL, &fk, &fv, 0)) {
	case 0:
		/* found */
		return res;
	default:
		break;
	}
	return SESQUITMP_NOT_FOUND;
}


/* meta */
static echs_bitmp_t
_get_as_of_now(_stor_t _s, mut_oid_t fact)
{
	echs_sesqui_t fs = _get_fact(_s->ft, fact);

	if (SESQUITMP_NOT_FOUND_P(fs)) {
		/* must be dead */
		return ECHS_NUL_BITMP;
	}
	return (echs_bitmp_t){ECHS_RANGE_FROM(fs.trans), fs.valid};
}

static echs_bitmp_t
_get_as_of_then(_stor_t _s, mut_oid_t fact, echs_instant_t as_of)
{
	mut_tid_t i_last_before = TID_NOT_FOUND;
	mut_tid_t i_first_after = TID_NOT_FOUND;
	echs_bitmp_t res = ECHS_NUL_BITMP;
	DBC *c;

	if (_s->tr->cursor(_s->tr, NULL, &c, DB_CURSOR_BULK) < 0) {
		return ECHS_NUL_BITMP;
	}
	for (DBT dbk = {}, dbv = {}; c->get(c, &dbk, &dbv, DB_NEXT) == 0;) {
		const struct trns_s *const t = dbv.data;

		if (UNLIKELY(dbv.size != sizeof(*t))) {
			goto clo;
		} else if (!echs_instant_le_p(t->trins, as_of)) {
			c->get(c, &dbk, &dbv, DB_PREV);
			break;
		} else if (fact == t->fact) {
			const mut_tid_t *tp = dbk.data;
			i_last_before = *tp;
		}
	}
	/* now I_LAST_BEFORE should hold FACT_NOT_FOUND or the index of
	 * the last fiddle with FACT before AS_OF */
	if (TID_NOT_FOUND_P(i_last_before)) {
		goto clo;
	}
	/* keep scanning, because the fact might have been superseded by
	 * a more recent transaction */
	for (DBT dbk = {}, dbv = {}; c->get(c, &dbk, &dbv, DB_NEXT) == 0;) {
		const struct trns_s *const t = dbv.data;

		if (UNLIKELY(dbv.size != sizeof(*t))) {
			goto clo;
		} else if (fact == t->fact) {
			const mut_tid_t *tp = dbk.data;
			i_first_after = *tp;
			break;
		}
	}
	/* now I_FIRST_AFTER should hold FACT_NOT_FOUND or the index of
	 * the next fiddle with FACT on or after AS_OF */
	with (struct trns_s bef, aft) {
		_get_tid(&bef, _s->tr, i_last_before);

		if (TID_NOT_FOUND_P(i_first_after)) {
			/* must be open-ended */
			res = (echs_bitmp_t){
				ECHS_RANGE_FROM(bef.trins),
				bef.valid,
			};
			break;
		}
		/* otherwise also get the one afterwards */
		_get_tid(&aft, _s->tr, i_first_after);
		res = (echs_bitmp_t){
			(echs_range_t){bef.trins, aft.trins},
			bef.valid,
		};
	}

clo:
	c->close(c);
	return res;
}


static mut_stor_t
_open(const char *fn, int UNUSED(fl))
{
	static const char fn2[] = ".2ndary.db";
	uint32_t flags = 0U;
	DB *ft = NULL;
	DB *tr = NULL;
	_stor_t s;

	if (UNLIKELY(fn == NULL)) {
		return NULL;
	} else if (db_create(&tr, NULL, 0) < 0 || tr == NULL) {
		return NULL;
	} else if (db_create(&ft, NULL, 0) < 0 || ft == NULL) {
		goto clo_prim;
	}

	/* open flags */
	if (fl & O_CREAT) {
		flags |= DB_CREATE;
	}
	if (fl & O_TRUNC) {
		flags |= DB_TRUNCATE;
	}
	/* set record length */
	tr->set_re_len(tr, sizeof(struct trns_s));  
	/* open the database */
	if (tr->open(tr, NULL, fn, NULL, DB_QUEUE, flags, 0) < 0) {
		goto clo;
	} else if (ft->open(ft, NULL, fn2, NULL, DB_HASH, flags, 0) < 0) {
		goto clo;
	}

	if (UNLIKELY((s = calloc(1, sizeof(*s))) == NULL)) {
		goto clo;
	}
	s->ft = ft;
	s->tr = tr;
	return (mut_stor_t)s;

clo:
	(void)ft->close(ft, 0);
clo_prim:
	(void)tr->close(tr, 0);
	return NULL;
}

static void
_close(mut_stor_t s)
{
	_stor_t _s;

	if ((_s = (_stor_t)s) == NULL) {
		return;
	} else if (_s->tr != NULL) {
		assert(_s->ft != NULL);
		/* close 2nd-ary */
		(void)_s->ft->close(_s->ft, 0);
		/* close primary */
		(void)_s->tr->close(_s->tr, 0);
	}
	memset(_s, 0, sizeof(*_s));
	free(_s);
	return;
}

static int
_put(mut_stor_t s, mut_oid_t fact, echs_range_t valid)
{
	_stor_t _s = (_stor_t)s;
	echs_sesqui_t prev;

	if (UNLIKELY(fact == MUT_NUL_OID)) {
		return -1;
	}
	/* let's see what we've got */
	if (!SESQUITMP_NOT_FOUND_P((prev = _get_fact(_s->ft, fact)))) {
		if (echs_range_eq_p(prev.valid, valid)) {
			/* do fuckall, it's in the db already */
			return 1;
		}
	}
	/* we're good to go now */
	with (echs_instant_t now = echs_now()) {
		with (struct trns_s t = {now, fact, valid}) {
			DBT v = {.data = &t, .size = sizeof(t)};

			if (_s->tr->put(_s->tr, NULL, NULL, &v, DB_APPEND) < 0) {
				return -1;
			}
		}
		/* fact mapping */
		with (echs_sesqui_t last = {now, valid}) {
			DBT f = {.data = &fact, .size = sizeof(fact)};
			DBT v = {.data = &last, .size = sizeof(last)};

			if (_s->ft->put(_s->ft, NULL, &f, &v, 0) < 0) {
				return -1;
			}
		}
		/* place last trans stamp */
		_s->trins = now;
	}
	return 0;
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
	else if (echs_instant_le_p(_s->trins, as_of)) {
		return _get_as_of_now(_s, fact);
	}
	/* otherwise proceed to scan */
	return _get_as_of_then(_s, fact, as_of);
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

static int
_ssd(
	mut_stor_t s, mut_oid_t old, mut_oid_t new, echs_range_t valid)
{
/* ssd (supersede) is short for
 * v <- _get(old)
 * _put(old, (echs_range_t){[v.from, valid.from)})
 * _put(new, valid) */
	echs_bitmp_t v = _get(s, old, ECHS_SOON);
	int rc = 0;

	if (echs_nul_range_p(v.valid)) {
		/* dead already */
		return -1;
	}
	/* invalidate old cell */
	rc += _put(s, old, (echs_range_t){v.valid.from, valid.from});
	/* punch new cell */
	rc += _put(s, new, valid);
	return rc;
}

static size_t
_hist(
	mut_stor_t s,
	echs_range_t *restrict trans, size_t ntrans,
	echs_range_t *restrict valid, mut_oid_t fact)
{
	_stor_t _s = (_stor_t)s;
	echs_sesqui_t fs = _get_fact(_s->ft, fact);
	size_t res = 0U;
	DBC *c;

	if (SESQUITMP_NOT_FOUND_P(fs)) {
		/* no last transaction, so no first either */
		return 0U;
	}
	/* prep traversal */
	if (_s->tr->cursor(_s->tr, NULL, &c, DB_CURSOR_BULK) < 0) {
		return 0U;
	}
	/* traverse the timeline and store offsets to transactions */
	for (DBT dbk = {}, dbv = {};
	     c->get(c, &dbk, &dbv, DB_NEXT) == 0 && res < ntrans;) {
		const struct trns_s *const t = dbv.data;

		if (UNLIKELY(dbv.size != sizeof(*t))) {
			break;
		} else if (fact == t->fact) {
			trans[res].from = t->trins;
			if (LIKELY(res)) {
				trans[res - 1U].till = t->trins;
			}
			if (valid != NULL) {
				valid[res] = t->valid;
			}

			/* inc */
			res++;
		}
	}
	/* last trans lasts forever */
	if (res > 0U) {
		trans[res - 1U].till = ECHS_UNTIL_CHANGED;
	}
	return res;
}

struct bitte_backend_s IN_DSO(backend) = {
	.mut_stor_open_f = _open,
	.mut_stor_close_f = _close,
	.bitte_get_f = _get,
	.bitte_put_f = _put,
	.bitte_rem_f = _rem,
	.bitte_supersede_f = _ssd,
	.bitte_hist_f = _hist,
};

/* bitte-bdb.c ends here */
