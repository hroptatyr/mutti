#if defined HAVE_CONFIG_H
# include "config.h"
#endif	/* HAVE_CONFIG_H */
#include "rb.h"

struct RBND_S(RBKEY_T) {
	union {
		RBKEY_T key;
		unsigned int pad;
	};
	rbnd_t left;
#if defined WORDS_BIGENDIAN
	rbnd_t rght:31;
	rbnd_t redp:1;
#else  /* !WORDS_BIGENDIAN */
	rbnd_t redp:1;
	rbnd_t rght:31;
#endif	/* WORDS_BIGENDIAN */
};

struct RBTR_S(RBKEY_T) {
	/* mimic a struct rbnd_s here */
	struct {
		union {
			uint32_t nfacts;
			RBKEY_T key;
		};
		rbnd_t root;
		rbnd_t pad;
	};
	struct RBND_S(RBKEY_T) base[];
};

static void
rbti_fill(RBKEY_T)(struct rbstk_s *restrict s, const struct RBTR_S(RBKEY_T) *t, rbnd_t of)
{
/* treat OF as root and fill S with left nodes */
	const struct RBND_S(RBKEY_T) *const base = t->base;
	for (; !RBND_NIL_P(of); s->n[s->depth++] = of, of = base[of].left);
	return;
}

static inline rbnd_t
rbti_pop(RBKEY_T)(struct rbstk_s *restrict s, const struct RBTR_S(RBKEY_T) *t)
{
	if (LIKELY(s->depth)) {
		rbnd_t nd = s->n[--s->depth];

		if ((s->k ^= 1U)) {
			rbti_fill(RBKEY_T)(s, t, t->base[nd].rght);
		}
		return nd;
	}
	return RBND_NIL;
}

#define FOREACH_RBN(_nd, _t, _rbt)					\
	with (rbnd_t _nd)						\
		with (struct rbstk_s __stk = {0U})			\
		for (rbti_fill(_t)(&__stk, _rbt, (_rbt)->root);		\
		     !RBND_NIL_P(_nd = rbti_pop(_t)(&__stk, _rbt));)

#define FOREACH_RBN_KEY(_nd, _t, _k, _rbt)				\
	FOREACH_RBN(_nd, _t, _rbt)					\
	with (_t _k = (_rbt)->base[_nd].key)

#define FOREACH_KEY(_t, _k, _rbt)					\
	FOREACH_RBN(_nd, _t, _rbt)					\
	with (_t _k = (_rbt)->base[_nd].key)

static rbnd_t
rb_search(RBKEY_T)(const struct RBTR_S(RBKEY_T) *t, RBKEY_T key)
{
	const struct RBND_S(RBKEY_T) *const base = t->base;
	rbnd_t ro = t->root;
	int cmp;

	while (!RBND_NIL_P(ro) && (cmp = rb_cmp(RBKEY_T)(key, base[ro].key))) {
		if (cmp < 0) {
			ro = base[ro].left;
		} else /*if (cmp > 0)*/ {
			ro = base[ro].rght;
		}
	}
	return ro;
}

static void
rb_insert(RBKEY_T)(struct RBTR_S(RBKEY_T) *restrict t, rbnd_t nd, RBKEY_T key)
{
	struct RBND_S(RBKEY_T) *const restrict base = t->base;
	struct {
		rbnd_t no;
		int cmp;
	} path[sizeof(void*) << 4U], *pp = path;

#define rbtn_rot_left(base, nd)				\
	({						\
		rbnd_t __res = base[nd].rght;		\
		base[nd].rght = base[__res].left;	\
		base[__res].left = nd;			\
		__res;					\
	})

#define rbtn_rot_rght(base, nd)				\
	({						\
		rbnd_t __res = base[nd].left;		\
		base[nd].left = base[__res].rght;	\
		base[__res].rght = nd;			\
		__res;					\
	})

	/* wind */
	for (pp->no = t->root; !RBND_NIL_P(pp->no); pp++) {
		int cmp = pp->cmp = rb_cmp(RBKEY_T)(key, base[pp->no].key);

		assert(cmp != 0);
		if (cmp < 0) {
			pp[1U].no = base[pp->no].left;
		} else /*if (cmp > 0)*/ {
			pp[1U].no = base[pp->no].rght;
		}
	}
	/* invariant: pp->no == NIL */
	pp->no = nd;
	/* also assign fact and init the node */
	base[nd].key = key;

	/* unwind */
	for (pp--; pp >= path; pp--) {
		rbnd_t cur = pp->no;

		if (pp->cmp < 0) {
			rbnd_t left = pp[1U].no;
			base[cur].left = left;
			if (base[left].redp) {
				rbnd_t leftleft = base[left].left;
				if (base[leftleft].redp) {
					/* blacken leftleft */
					base[leftleft].redp = 0U;
					cur = rbtn_rot_rght(base, cur);
				}
			} else {
				return;
			}
		} else /*if (cmp > 0)*/ {
			rbnd_t rght = pp[1U].no;
			base[cur].rght = rght;
			if (base[rght].redp) {
				rbnd_t left = base[cur].left;
				if (base[left].redp) {
					/* split 4-node */
					base[left].redp = 0U;
					base[rght].redp = 0U;
					base[cur].redp = 1U;
				} else {
					/* lean left */
					rbnd_t tmp;

					tmp = rbtn_rot_left(base, cur);
					base[tmp].redp = base[cur].redp;
					base[cur].redp = 1U;
					cur = tmp;
				}
			} else {
				return;
			}
		}
		pp->no = cur;
	}
#undef rbtn_rot_left
#undef rbtn_rot_rght
	/* set root and paint it black */
	t->root = path->no;
	base[t->root].redp = 0U;
	return;
}

#undef RBKEY_T
