/*
 * Copyright 2015-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/*
 * An XDR conflict resolver, the same merge as the Rust and Go resolvers: visits take the larger count, tags
 * unite in LUT order, the profile is whichever side wrote it last, a bin deleted by the later writer stays
 * deleted, and everything else is the newer side's. See aerospike-udf-sdk-wasm/examples/resolver for why
 * each rule is the way it is.
 */

#include "aerospike_udf.h"

#define MAX_BINS 32
#define NAME_SZ 32

/* The bins already decided, so the "everything else" pass skips them. */
typedef struct done_s {
	char names[MAX_BINS][NAME_SZ];
	int32_t n;
} done;

static bool done_has(const done* d, const char* name)
{
	for (int32_t i = 0; i < d->n; i++) {
		const char* a = d->names[i];
		int32_t j = 0;
		while (a[j] != '\0' && a[j] == name[j]) j++;
		if (a[j] == '\0' && name[j] == '\0') return true;
	}
	return false;
}

static void done_add(done* d, const char* name)
{
	if (d->n == MAX_BINS) return;
	int32_t i = 0;
	for (; i < NAME_SZ - 1 && name[i] != '\0'; i++) d->names[d->n][i] = name[i];
	d->names[d->n][i] = '\0';
	d->n++;
}

/* A stored string value into a NUL-terminated buffer. */
static bool name_of(const as_udf_value* v, char* out, int32_t cap)
{
	int32_t n = v->kind == AS_UDF_STR ? as_udf_bytes(v, out, cap - 1) : -1;
	if (n < 0) return false;
	out[n] = '\0';
	return true;
}

/* Walk a stored list of strings, calling `fn` with each name. */
typedef void (*name_fn)(const char* name, const as_udf_value* v, void* udata);

static void each_name(const as_udf_value* list, name_fn fn, void* udata)
{
	if (list->kind != AS_UDF_LIST) return;
	int32_t cur = as_udf_cursor_enter_at(as_udf_value_src(list), as_udf_value_at(list));
	if (cur <= 0) return;
	as_udf_value w[32];
	for (int32_t n; (n = as_udf_cursor_values(cur, w, (int32_t)sizeof(w))) > 0;) {
		for (int32_t i = 0; i < n; i++) {
			char name[NAME_SZ];
			if (name_of(&w[i], name, (int32_t)sizeof(name))) fn(name, &w[i], udata);
		}
		if (n < 32) break;
	}
	as_udf_cursor_close(cur);
}

/* The tags union: the older side's tags, then the newer side's tags that are new. */
typedef struct tags_s {
	int32_t list;
	as_udf_value first; /* the older side's tags, for the "already there" check */
	bool second;
} tags;

static void push_tag(const char* name, const as_udf_value* v, void* udata)
{
	tags* t = (tags*)udata;
	if (t->second && as_udf_list_has_str(&t->first, name)) return;
	as_udf_list_push_at(t->list, as_udf_value_src(v), as_udf_value_at(v));
}

/* Deletes: a bin the later writer deleted goes out as nil. */
typedef struct deletes_s {
	const as_udf_conflict* c;
	int32_t out;
	done* d;
	bool by_remote;
} deletes;

static void apply_delete(const char* name, const as_udf_value* v, void* udata)
{
	(void)v;
	deletes* x = (deletes*)udata;
	/* A put replaces, so a bin already decided above (the profile) is overridden here, as it should be. */
	if (as_udf_event_remote_newer(x->c, name) == x->by_remote) {
		as_udf_map_put_str_handle(x->out, name, as_udf_strlen(name), as_udf_val_nil());
		done_add(x->d, name);
	}
}

/* Everything else: the newer side's value, by position. */
typedef struct rest_s {
	const as_udf_conflict* c;
	int32_t out;
	done* d;
} rest;

static void put_newer(const char* name, const as_udf_value* v, void* udata)
{
	(void)v;
	rest* x = (rest*)udata;
	if (done_has(x->d, name)) return;
	const as_udf_event_record* side = as_udf_event_remote_newer(x->c, name) ? &x->c->remote : &x->c->local;
	const as_udf_event_record* other = side == &x->c->remote ? &x->c->local : &x->c->remote;
	as_udf_value val;
	if (as_udf_event_bin(side, name, &val) != AS_UDF_OK || val.kind == AS_UDF_NIL) {
		if (as_udf_event_bin(other, name, &val) != AS_UDF_OK || val.kind == AS_UDF_NIL) return;
	}
	as_udf_map_put_value(x->out, name, &val);
	done_add(x->d, name);
}

AS_UDF_EVENT_XDR_MERGE(merge_account)
{
	as_udf_conflict c;
	if (as_udf_event_decode(args, argc, &c) != AS_UDF_OK) return as_udf_failf("merge_account: not a conflict");

	int32_t out = as_udf_map_new(8);
	if (out <= 0) return as_udf_failf("merge_account: could not build the record");
	done d = { .n = 0 };

	/* visits: the larger of the two counts. */
	as_udf_value lv, rv;
	bool has_l = as_udf_event_bin(&c.local, "visits", &lv) == AS_UDF_OK && lv.kind == AS_UDF_INT;
	bool has_r = as_udf_event_bin(&c.remote, "visits", &rv) == AS_UDF_OK && rv.kind == AS_UDF_INT;
	if (has_l || has_r) {
		int64_t visits = has_l && has_r ? (rv.payload > lv.payload ? rv.payload : lv.payload) : has_l ? lv.payload : rv.payload;
		as_udf_map_put_str_int(out, "visits", 6, visits);
		done_add(&d, "visits");
	}

	/* tags: the union, older side first - the order both DCs agree on. */
	bool remote_newer = as_udf_event_remote_newer(&c, "tags");
	const as_udf_event_record* older = remote_newer ? &c.local : &c.remote;
	const as_udf_event_record* newer = remote_newer ? &c.remote : &c.local;
	tags t = { .list = as_udf_list_new(16), .second = false };
	if (t.list <= 0) return as_udf_failf("merge_account: could not build the tags");
	as_udf_value older_tags = { AS_UDF_ABSENT, 0, 0 }, newer_tags = { AS_UDF_ABSENT, 0, 0 };
	as_udf_event_bin(older, "tags", &older_tags);
	as_udf_event_bin(newer, "tags", &newer_tags);
	t.first = older_tags;
	each_name(&older_tags, push_tag, &t);
	t.second = true;
	each_name(&newer_tags, push_tag, &t);
	as_udf_map_put_str_handle(out, "tags", 4, t.list);
	done_add(&d, "tags");

	/* profile: last writer wins, by the bin's LUT. */
	{
		const as_udf_event_record* side = as_udf_event_remote_newer(&c, "profile") ? &c.remote : &c.local;
		const as_udf_event_record* other = side == &c.remote ? &c.local : &c.remote;
		as_udf_value p;
		if ((as_udf_event_bin(side, "profile", &p) == AS_UDF_OK && p.kind != AS_UDF_NIL) ||
				(as_udf_event_bin(other, "profile", &p) == AS_UDF_OK && p.kind != AS_UDF_NIL)) {
			as_udf_map_put_value(out, "profile", &p);
			done_add(&d, "profile");
		}
	}

	/* A bin deleted by the later writer stays deleted. */
	deletes dl = { .c = &c, .out = out, .d = &d, .by_remote = false };
	each_name(&c.local.deleted, apply_delete, &dl);
	dl.by_remote = true;
	each_name(&c.remote.deleted, apply_delete, &dl);

	/* Everything else: the newer side's. */
	rest r = { .c = &c, .out = out, .d = &d };
	each_name(&c.local.bins, put_newer, &r);
	each_name(&c.remote.bins, put_newer, &r);

	return as_udf_resolve_record(out, -1);
}
