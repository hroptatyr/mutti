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
#include <assert.h>
#include "bitte.h"
#include "bitte-private.h"
#include "nifty.h"

#define MUT_STOR_DFLT_TYPE	(MUT_STOR_TYPE_MEM)
#define MUT_STOR_TYPE(x)	((x) ? (x)->type : MUT_STOR_DFLT_TYPE)
#define MUT_STOR_IMPL(x, f)	impls[MUT_STOR_TYPE(x)]._paste(f, _f)

static struct bitte_backend_s impls[NMUT_STOR_TYPES];


#if !defined BUILD_DSO
static __attribute__((constructor)) void
init_impls(void)
{
	extern struct bitte_backend_s bitte_mem_LTX_backend;
	extern struct bitte_backend_s bitte_bdb_LTX_backend;
	extern struct bitte_backend_s bitte_dsk_LTX_backend;

	impls[MUT_STOR_TYPE_MEM] = bitte_mem_LTX_backend;
	impls[MUT_STOR_TYPE_BDB] = bitte_bdb_LTX_backend;
	impls[MUT_STOR_TYPE_DSK] = bitte_dsk_LTX_backend;
	return;
}
#endif	/* !BUILD_DSO */


mut_stor_t
mut_stor_open(mut_stor_type_t type, const char *fn, int fl)
{
	mut_stor_t s;

	if (UNLIKELY(type >= NMUT_STOR_TYPES)) {
		return NULL;
	}
	if (type == MUT_STOR_TYPE_UNK) {
		type = MUT_STOR_DFLT_TYPE;
	}
	if (UNLIKELY((s = impls[type].mut_stor_open_f(fn, fl)) == NULL)) {
		return NULL;
	}
	s->type = type;
	return s;
}

void
mut_stor_close(mut_stor_t s)
{
	MUT_STOR_IMPL(s, mut_stor_close)(s);
	return;
}

int
bitte_put(mut_stor_t s, mut_oid_t fact, echs_range_t valid)
{
	return MUT_STOR_IMPL(s, bitte_put)(s, fact, valid);
}

int
bitte_rem(mut_stor_t s, mut_oid_t fact)
{
	return MUT_STOR_IMPL(s, bitte_rem)(s, fact);
}

echs_bitmp_t
bitte_get(mut_stor_t s, mut_oid_t fact, echs_instant_t as_of)
{
	return MUT_STOR_IMPL(s, bitte_get)(s, fact, as_of);
}

int
bitte_supersede(mut_stor_t s, mut_oid_t old, mut_oid_t new, echs_range_t valid)
{
	return MUT_STOR_IMPL(s, bitte_supersede)(s, old, new, valid);
}

size_t
bitte_rtr(
	mut_stor_t s,
	mut_oid_t *restrict fact, size_t nfact,
	echs_range_t *restrict valid, echs_range_t *restrict trans,
	echs_instant_t as_of)
{
	return MUT_STOR_IMPL(s, bitte_rtr)(s, fact, nfact, valid, trans, as_of);
}

size_t
bitte_scan(
	mut_stor_t s,
	mut_oid_t *restrict fact, size_t nfact,
	echs_range_t *restrict valid, echs_range_t *restrict trans,
	echs_instant_t vtime)
{
	return MUT_STOR_IMPL(s, bitte_scan)(s, fact, nfact, valid, trans, vtime);
}

size_t
bitte_hist(
	mut_stor_t s,
	echs_range_t *restrict trans, size_t ntrans,
	echs_range_t *restrict valid, mut_oid_t fact)
{
	return MUT_STOR_IMPL(s, bitte_hist)(s, trans, ntrans, valid, fact);
}

/* bitte.c ends here */
