/*** bitte-private.h -- backend interface for bitte API
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
#if !defined INCLUDED_bitte_private_h_
#define INCLUDED_bitte_private_h_

struct bitte_backend_s {
	mut_stor_t(*mut_stor_open_f)(const char *fn, int flags);
	void(*mut_stor_close_f)(mut_stor_t);
	echs_bitmp_t(*bitte_get_f)(
		mut_stor_t, mut_oid_t fact, echs_instant_t as_of);
	int(*bitte_put_f)(mut_stor_t, mut_oid_t fact, echs_range_t valid);
	int(*bitte_supersede_f)(
		mut_stor_t, mut_oid_t oldf, mut_oid_t newf, echs_range_t valid);
	int(*bitte_rem_f)(mut_stor_t, mut_oid_t fact);
	size_t(*bitte_rtr_f)(
		mut_stor_t,
		mut_oid_t *restrict fact, size_t nfact,
		echs_range_t *restrict valid, echs_range_t *restrict trans,
		echs_instant_t as_of);
	size_t(*bitte_scan_f)(
		mut_stor_t,
		mut_oid_t *restrict fact, size_t nfact,
		echs_range_t *restrict valid, echs_range_t *restrict trans,
		echs_instant_t vtime);
	size_t(*bitte_hist_f)(
		mut_stor_t,
		echs_range_t *restrict trans, size_t ntrans,
		echs_range_t *restrict valid, mut_oid_t fact);
};

#if !defined IN_DSO
# define IN_DSO(x)	x
#endif	/* !IN_DSO */

extern struct bitte_backend_s IN_DSO(backend);

#endif	/* INCLUDED_bitte_private_h_ */
