/** simple addgetrem test */
#if defined HAVE_CONFIG_H
# include "config.h"
#endif	/* HAVE_CONFIG_H */
#include <string.h>
#include <stdio.h>
#include "dt-strpf.h"
#include "bitte.h"
#include "nifty.h"


static size_t
strf_range(char *restrict buf, size_t bsz, echs_range_t r)
{
	size_t res = 0U;

	buf[res++] = '[';
	if (echs_empty_range_p(r)) {
		goto out;
	}
	res += dt_strf(buf + res, bsz - res, r.from);
	buf[res++] = ',';
	if (!echs_end_of_time_p(r.till)) {
		res += dt_strf(buf + res, bsz - res, r.till);
	} else {
		memcpy(buf + res, "\u221e", sizeof("\u221e"));
		res += sizeof("\u221e") - 1U;
	}
out:
	buf[res++] = ')';
	buf[res] = '\0';
	return res;
}

static size_t
strf_bitmp(char *restrict buf, size_t bsz, echs_bitmp_t r)
{
	size_t res = 0U;

	res += strf_range(buf, bsz, r.valid);
	buf[res++] = 'x';
	res += strf_range(buf + res, bsz - res, r.trans);
	buf[res] = '\0';
	return res;
}

static int
pr_bitmp(echs_bitmp_t r)
{
	char buf[256U];

	strf_bitmp(buf, sizeof(buf), r);
	return puts(buf);
}


int
main(int argc, char *const argv[])
{
	echs_set_now(dt_strp("2014-10-29T13:23:41.507", NULL));
	bitte_put(NULL, (mut_oid_t)"Alice_$200",
		  ECHS_FROM(.y = 2012, .m = 01, .d = 01, .H = ECHS_ALL_DAY));
	bitte_put(NULL, (mut_oid_t)"Ann_$300",
		  ECHS_FROM(.y = 2012, .m = 01, .d = 02, .H = ECHS_ALL_DAY));
	pr_bitmp(bitte_get(NULL, (mut_oid_t)"Alice_$200", ECHS_SOON));
	pr_bitmp(bitte_get(NULL, (mut_oid_t)"Ann_$300", ECHS_SOON));

	bitte_put(NULL, (mut_oid_t)"Carl_$100",
		  ECHS_FROM(.y = 2012, .m = 01, .d = 03, .H = ECHS_ALL_DAY));
	bitte_put(NULL, (mut_oid_t)"Alice_$200",
		  (echs_range_t){
			  {.y = 2012, .m = 01, .d = 01, .H = ECHS_ALL_DAY},
			  {.y = 2012, .m = 01, .d = 03, .H = ECHS_ALL_DAY}});
	pr_bitmp(bitte_get(NULL, (mut_oid_t)"Carl_$100", ECHS_SOON));
	pr_bitmp(bitte_get(NULL, (mut_oid_t)"Alice_$200", ECHS_SOON));

	bitte_put(NULL, (mut_oid_t)"Alice_$500",
		  ECHS_FROM(.y = 2012, .m = 01, .d = 03, .H = ECHS_ALL_DAY));
	bitte_put(NULL, (mut_oid_t)"Ellen_$700",
		  ECHS_FROM(.y = 2012, .m = 01, .d = 05, .H = ECHS_ALL_DAY));
	pr_bitmp(bitte_get(NULL, (mut_oid_t)"Carl_$100", ECHS_SOON));
	pr_bitmp(bitte_get(NULL, (mut_oid_t)"Ellen_$700", ECHS_SOON));
	pr_bitmp(bitte_get(NULL, (mut_oid_t)"Alice_$200", ECHS_SOON));
	pr_bitmp(bitte_get(NULL, (mut_oid_t)"Alice_$500", ECHS_SOON));

	bitte_put(NULL, (mut_oid_t)"John_$400",
		  ECHS_FROM(.y = 2012, .m = 01, .d = 05, .H = ECHS_ALL_DAY));
	bitte_put(NULL, (mut_oid_t)"John_$400",
		  (echs_range_t){
			  {.y = 2012, .m = 01, .d = 05, .H = ECHS_ALL_DAY},
			  {.y = 2012, .m = 01, .d = 06, .H = ECHS_ALL_DAY}});
	pr_bitmp(bitte_get(NULL, (mut_oid_t)"John_$400", ECHS_SOON));
	bitte_put(NULL, (mut_oid_t)"Alice_$500",
		  (echs_range_t){
			  {.y = 2012, .m = 01, .d = 03, .H = ECHS_ALL_DAY},
			  {.y = 2012, .m = 01, .d = 06, .H = ECHS_ALL_DAY}});
	pr_bitmp(bitte_get(NULL, (mut_oid_t)"Alice_$200", ECHS_SOON));
	pr_bitmp(bitte_get(NULL, (mut_oid_t)"Alice_$500", ECHS_SOON));

	bitte_rem(NULL, (mut_oid_t)"Ellen_$700");
	pr_bitmp(bitte_get(NULL, (mut_oid_t)"Carl_$100", ECHS_SOON));
	pr_bitmp(bitte_get(NULL, (mut_oid_t)"Alice_$200", ECHS_SOON));
	pr_bitmp(bitte_get(NULL, (mut_oid_t)"Ellen_$700", ECHS_SOON));
	return 0;
}

/* addremget_01.c ends here */
