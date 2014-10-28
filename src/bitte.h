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
#define ECHS_EMPTY_BITMP					\
	((echs_bitmp_t){ECHS_EMPTY_RANGE, ECHS_EMPTY_RANGE})

typedef uintptr_t mut_oid_t;

typedef struct {
	echs_range_t valid;
	echs_range_t trans;
} echs_bitmp_t;


/**
 * Return the valid time of ITEM (as of AS_OF). */
extern echs_range_t bitte_valid(mut_oid_t item, echs_instant_t as_of);

/**
 * Return the transaction time of ITEM (as of AS_OF). */
extern echs_range_t bitte_trans(mut_oid_t item, echs_instant_t as_of);

/**
 * Return the full bitemporal stamp of ITEM (as of AS_OF). */
extern echs_bitmp_t bitte_get(mut_oid_t item, echs_instant_t as_of);

/**
 * Add ITEM with valid time VALID. */
extern int bitte_add(mut_oid_t item, echs_range_t valid);

/**
 * Remove ITEM. */
extern int bitte_rem(mut_oid_t item);

#endif	/* INCLUDED_bitte_h_ */
