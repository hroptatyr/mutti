/*** bitte.c -- bitemporal api
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
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "bitte.h"
#include "nifty.h"

#define ITEM_NOT_FOUND		((size_t)-1)
#define ITEM_NOT_FOUND_P(x)	(!~(size_t)(x))

/* try SoA */
static size_t nitems;
static mut_oid_t *items;
static echs_range_t **valids;
static echs_instant_t **transs;
static size_t *ntrans;


static void*
recalloc(void *ptr, size_t old, size_t new, size_t membz)
{
	void *res = realloc(ptr, new * membz);
	memset((char*)res + old * membz, 0, (new - old) * membz);
	return res;
}

static size_t
_get_item(mut_oid_t item)
{
	for (size_t i = 0U; i < nitems; i++) {
		if (items[i] == item) {
			return i;
		}
	}
	return ITEM_NOT_FOUND;
}


echs_range_t
bitte_valid(mut_oid_t item, echs_instant_t as_of)
{
	/* check that we've got ITEM already */
	size_t i = _get_item(item);

	if (ITEM_NOT_FOUND_P(i)) {
		/* nothing to be done then */
		return ECHS_NUL_RANGE;
	}

	with (size_t j) {
		const size_t ntr = ntrans[i];
		const echs_instant_t *tr = transs[i];

		for (j = 0U; j < ntr && echs_instant_le_p(tr[j], as_of); j++);
		if (UNLIKELY(!j--)) {
			break;
		}
		/* otherwise we must have a trans [j, j + 1) */
		return valids[i][j];
	}
	return ECHS_EMPTY_RANGE;
}

echs_range_t
bitte_trans(mut_oid_t item, echs_instant_t as_of)
{
	/* check that we've got ITEM already */
	size_t i = _get_item(item);

	if (ITEM_NOT_FOUND_P(i)) {
		/* nothing to be done then */
		return ECHS_NUL_RANGE;
	}

	with (size_t j) {
		const size_t ntr = ntrans[i];
		const echs_instant_t *tr = transs[i];

		for (j = 0U; j < ntr && echs_instant_le_p(tr[j], as_of); j++);
		if (UNLIKELY(!j)) {
			break;
		}
		/* otherwise we must have a trans [j - 1, j) */
		return (echs_range_t){
			tr[j - 1U],
			j < ntr ? tr[j] : ECHS_UNTIL_CHANGED
		};
	}
	return ECHS_EMPTY_RANGE;
}

echs_bitmp_t
bitte_get(mut_oid_t item, echs_instant_t as_of)
{
	/* check that we've got ITEM already */
	size_t i = _get_item(item);

	if (ITEM_NOT_FOUND_P(i)) {
		/* nothing to be done then */
		return ECHS_NUL_BITMP;
	}

	with (size_t j) {
		const size_t ntr = ntrans[i];
		const echs_instant_t *tr = transs[i];

		for (j = 0U; j < ntr && echs_instant_le_p(tr[j], as_of); j++);
		if (UNLIKELY(!j)) {
			break;
		}
		/* otherwise we must have a trans [j - 1, j) */
		return (echs_bitmp_t){
			valids[i][j - 1U],
			(echs_range_t){
				tr[j - 1],
				j < ntr ? tr[j] : ECHS_UNTIL_CHANGED}
		};
	}
	return ECHS_EMPTY_BITMP;
}

int
bitte_add(mut_oid_t item, echs_range_t valid)
{
	/* check if we've got ITEM already */
	size_t i = _get_item(item);

	if (ITEM_NOT_FOUND_P(i)) {
		if ((nitems % 512U) == 0U) {
			const size_t nu = nitems + 512U;
			items = realloc(items, nu * sizeof(*items));
			valids = recalloc(valids, nitems, nu, sizeof(*valids));
			transs = recalloc(transs, nitems, nu, sizeof(*transs));
			ntrans = recalloc(ntrans, nitems, nu, sizeof(*ntrans));
		}
		/* assign new item */
		i = nitems++;
		items[i] = item;
	}
	if ((ntrans[i] % 16U) == 0U) {
		const size_t nu = ntrans[i] + 16U;
		valids[i] = realloc(valids[i], nu * sizeof(*valids[i]));
		transs[i] = realloc(transs[i], nu * sizeof(*transs[i]));
	}
	with (size_t it = ntrans[i]++) {
		/* stamp off */
		transs[i][it] = echs_now();
		/* and bang the valid time interval */
		valids[i][it] = valid;
	}
	return 0;
}

int
bitte_rem(mut_oid_t item)
{
/* this does nothing but to stamp off the last trans instant
 * nothing will be removed */
	/* check that we've got ITEM already */
	size_t i = _get_item(item);

	if (ITEM_NOT_FOUND_P(i)) {
		/* nothing to be done then */
		return -1;
	}
	if ((ntrans[i] % 16U) == 0U) {
		const size_t nu = ntrans[i] + 16U;
		valids[i] = realloc(valids[i], nu * sizeof(*valids[i]));
		transs[i] = realloc(transs[i], nu * sizeof(*transs[i]));
	}
	with (size_t it = ntrans[i]++) {
		/* stamp off */
		transs[i][it] = echs_now();
		/* invalidate the time interval before hand */
		valids[i][it] = echs_empty_range();
	}
	return 0;
}

/* bitte.c ends here */
