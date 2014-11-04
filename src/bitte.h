/*** bitte.h -- bitemporal api
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
#if !defined INCLUDED_bitte_h_
#define INCLUDED_bitte_h_
#include <stdint.h>
#include "instant.h"
#include "range.h"

#define ECHS_AS_OF_NOW	ECHS_NOW
#define ECHS_NUL_BITMP	((echs_bitmp_t){ECHS_NUL_RANGE, ECHS_NUL_RANGE})

typedef uintptr_t mut_oid_t;
#define MUT_NUL_OID	((mut_oid_t)0U)

typedef struct {
	echs_range_t valid;
	echs_range_t trans;
} echs_bitmp_t;


/**
 * Return the full bitemporal stamp of FACT (as of AS_OF). */
extern echs_bitmp_t bitte_get(mut_oid_t fact, echs_instant_t as_of);

/**
 * Put FACT with valid time VALID. */
extern int bitte_put(mut_oid_t fact, echs_range_t valid);

/**
 * Supersede FACT1 with FACT2 and new validity VALID.
 * The current validity of FACT1 is extended/restricted to the
 * beginning of VALID.  For all modifications the same transaction
 * time is used. */
extern int
bitte_supersede(mut_oid_t old_fact, mut_oid_t new_fact, echs_range_t valid);

/**
 * Remove FACT.
 * This is equivalent to
 *   bitte_supersede(FACT, MUT_NUL_OID, ECHS_NUL_RANGE); */
extern int bitte_rem(mut_oid_t fact);

/**
 * Retrieve all known FACTs as of AS_OF (known as transaction time slice).
 * At most NFACT facts will be put into the FACT array.
 * Put validity times into VALID array if non-NULL.
 * Put transaction times into TRANS array if non-NULL.
 * VALID and/or TRANS in that case should have at least NFACT slots.
 * Return the number of facts retrievable. */
extern size_t
bitte_rtr(
	mut_oid_t *restrict fact, size_t nfact,
	echs_range_t *restrict valid, echs_range_t *restrict trans,
	echs_instant_t as_of);

/**
 * Retrieve FACTs that were valid on VTIME (known as valid time slice).
 * At most NFACT facts will be put into the FACT array.
 * Put validity times into VALID array if non-NULL.
 * Put transaction times into TRANS array if non-NULL.
 * VALID and/or TRANS in that case should have at least NFACT slots.
 * Return the number of facts retrievable. */
extern size_t
bitte_scan(
	mut_oid_t *restrict fact, size_t nfact,
	echs_range_t *restrict valid, echs_range_t *restrict trans,
	echs_instant_t vtime);

/**
 * Retrieve history of FACT.
 * At most NTRANS transaction time ranges will be put into the TRANS array.
 * If non-NULL put corresponding valid time ranges into VALID.
 * Return the number of transactions. */
extern size_t
bitte_hist(
	echs_range_t *restrict trans, size_t ntrans,
	echs_range_t *restrict valid, mut_oid_t fact);

#endif	/* INCLUDED_bitte_h_ */
