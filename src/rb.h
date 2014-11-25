#if !defined INCLUDED_rb_h_
#define INCLUDED_rb_h_

typedef int32_t rbnd_t;

#define RBND_NIL	((rbnd_t)-1)
#define RBND_NIL_P(x)	(!~(x))

#define RBND_S(x)	x ## _rbnd_s
#define RBTR_S(x)	x ## _rbtr_s
#define rbti_fill(x)	rbti_ ## x ## _fill
#define rbti_pop(x)	rbti_ ## x ## _pop
#define rb_search(x)	rb_ ## x ## _search
#define rb_insert(x)	rb_ ## x ## _insert
#define rb_cmp(x)	rb_ ## x ## _cmp

struct rbstk_s {
	size_t depth;
	rbnd_t n[sizeof(void*) << 4U];
};

#endif	/* !INCLUDED_rb_h_ */
