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

static struct fact_s
_get_fact(DB *ftm, mut_oid_t fact)
{
	struct fact_s f;
	DBT fk = {&fact, sizeof(fact)};
	DBT fv = {.data = &f, .ulen = sizeof(f), .flags = DB_DBT_USERMEM};

	switch (ftm->get(ftm, NULL, &fk, &fv, 0)) {
	case 0:
		/* found */
		return f;
	default:
		break;
	}
	return FACT_NOT_FOUND;
}

static int
_put_last_trans(DB *ftm, mut_oid_t fact, mut_tid_t last)
{
	/* set up fact mapping */
	struct fact_s f;

	if (FACT_NOT_FOUND_P((f = _get_fact(ftm, fact)))) {
		f._1st = last;
	}
	f.last = last;

	with (DBT fk = {&fact, sizeof(fact)}, fv = {&f, sizeof(f)}) {
		if (ftm->put(ftm, NULL, &fk, &fv, 0) < 0) {
			return -1;
		}
	}
	return 0;
}


/* meta */
static echs_bitmp_t
_get_as_of_now(_stor_t _s, mut_oid_t fact)
{
	struct fact_s f = _get_fact(_s->ft, fact);
	struct trns_s t;

	if (FACT_NOT_FOUND_P(f)) {
		/* must be dead */
		return ECHS_NUL_BITMP;
	}
	/* yay, dead or alive, it's in our books */
	if (UNLIKELY(TID_NOT_FOUND_P(_get_tid(&t, _s->tr, f.last)))) {
		/* that's impossible :O */
		return ECHS_NUL_BITMP;
	}
	return (echs_bitmp_t){t.valid, ECHS_RANGE_FROM(t.trins)};
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
				bef.valid,
				ECHS_RANGE_FROM(bef.trins)
			};
			break;
		}
		/* otherwise also get the one afterwards */
		_get_tid(&aft, _s->tr, i_first_after);
		res = (echs_bitmp_t){
			bef.valid,
			(echs_range_t){bef.trins, aft.trins},
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
	if (UNLIKELY(fact == MUT_NUL_OID)) {
		return -1;
	}
	/* stamp off then */
	with (_stor_t _s = (_stor_t)s) {
		struct trns_s t = {echs_now(), fact, valid};
		DBT k = {NULL};
		DBT v = {.data = &t, .size = sizeof(t)};

		if (_s->tr->put(_s->tr, NULL, &k, &v, DB_APPEND) < 0) {
			return -1;
		}

		/* put fact mapping */
		assert(k.size == sizeof(mut_tid_t));
		_put_last_trans(_s->ft, fact, *(mut_tid_t*)k.data);
		/* place last trans stamp */
		_s->trins = t.trins;
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
	_stor_t _s = (_stor_t)s;
	struct fact_s f = _get_fact(_s->ft, fact);
	struct trns_s t;

	if (FACT_NOT_FOUND_P(f)) {
		/* he's dead already */
		return -1;
	} else if (TID_NOT_FOUND_P(_get_tid(&t, _s->tr, f.last))) {
		/* huh? something's seriously out of sync */
		return -1;
	} else if (echs_nul_range_p(t.valid)) {
		/* dead already, just */
		return -1;
	}

	/* stamp him off */
	with (echs_instant_t now = echs_now()) {
		DBT k = {NULL};
		DBT v = {.data = &t, .size = sizeof(t)};

		t = (struct trns_s){now, fact, echs_nul_range()};
		if (_s->tr->put(_s->tr, NULL, &k, &v, DB_APPEND) < 0) {
			return -1;
		}

		/* put fact mapping */
		assert(k.size == sizeof(mut_tid_t));
		_put_last_trans(_s->ft, fact, *(mut_tid_t*)k.data);
		/* place last trans stamp */
		_s->trins = now;
	}
	return 0;
}

static int
_ssd(
	mut_stor_t s, mut_oid_t old, mut_oid_t new, echs_range_t valid)
{
	_stor_t _s = (_stor_t)s;
	const struct fact_s f = _get_fact(_s->ft, old);
	struct trns_s last;

	if (FACT_NOT_FOUND_P(f)) {
		/* must be dead, cannot supersede */
		return -1;
	} else if (TID_NOT_FOUND_P(_get_tid(&last, _s->tr, f.last))) {
		/* must be dead, cannot supersede */
		return -1;
	}
	/* atomically handle the supersedure */
	with (echs_instant_t now = echs_now()) {
		/* old guy first */
		DBT k = {NULL};
		DBT v = {
			.data = &(struct trns_s){
				now, old,
				/* new validity */
				{last.valid.from, valid.from}
			},
			.size = sizeof(struct trns_s)
		};

		if (_s->tr->put(_s->tr, NULL, &k, &v, DB_APPEND) < 0) {
			return -1;
		}

		/* put fact mapping */
		assert(k.size == sizeof(mut_tid_t));
		_put_last_trans(_s->ft, old, *(mut_tid_t*)k.data);
		/* place last trans stamp */
		_s->trins = now;

		/* new guy next */
		if (new != MUT_NUL_OID) {
			v.data = &(struct trns_s){now, new, valid};

			if (_s->tr->put(_s->tr, NULL, &k, &v, DB_APPEND) < 0) {
				return -1;
			}

			_put_last_trans(_s->ft, new, *(mut_tid_t*)k.data);
		}
	}
	return 0;
}

static size_t
_hist(
	mut_stor_t s,
	echs_range_t *restrict trans, size_t ntrans,
	echs_range_t *restrict valid, mut_oid_t fact)
{
	_stor_t _s = (_stor_t)s;
	const struct fact_s f = _get_fact(_s->ft, fact);
	struct trns_s last;
	size_t res = 0U;
	DBC *c;

	if (FACT_NOT_FOUND_P(f)) {
		/* no last transaction, so no first either */
		return 0U;
	} else if (TID_NOT_FOUND_P(_get_tid(&last, _s->tr, f.last))) {
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
