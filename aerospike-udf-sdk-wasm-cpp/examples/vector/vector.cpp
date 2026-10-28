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
 * Exact top-k over packed float vectors, as a WASM UDF in C++.
 *
 * The same module as the Rust, C, Go and Java ones, step for step - see the Rust module's documentation
 * for the shared algorithm and why each step is the way it is. In short: the host copies the blob
 * straight into a float array; distances accumulate in double from float elements in one fixed order;
 * larger is always worse; ties break on the digest; the arguments are read once per stream.
 *
 * The kernels are the C module's; what C++ changes is the exports: UDF_STREAM_RAW and UDF_FUNCTION_RAW
 * declare the raw (args, argc) form, since the arguments are read once per stream and kept, and give the
 * signatures. The hot path stays on the C natives and allocates nothing.
 *
 *   topk("v", <query blob>, "cosine", 10)     stream: the 10 nearest, one partial per stream
 *   vec_distance("v", <query blob>, "dot")    record: one number
 */

#include "aerospike_udf.hpp"

#define MAX_DIM 2048
/* Bounded by the per-call handle table: each hit of the partial is one built map. */
#define MAX_K 200
#define DIGEST 20
#define KEY_MAX 64

enum metric { COSINE, DOT, SQEUCLIDEAN, EUCLIDEAN };

typedef struct hit_s {
	double score;
	uint8_t digest[DIGEST];
	uint32_t key_len;
	char key[KEY_MAX];
} hit;

static struct {
	bool configured;
	uint32_t bin_len;
	char bin[32];
	metric metric;
	uint32_t dim;
	float query[MAX_DIM];
	double qnorm;
	uint32_t k;
	uint32_t n;
	hit hits[MAX_K];
	int64_t skipped_dim;
	int64_t skipped_none;
} S;

/* The record's vector, copied by the host straight into the float array. */
static float VEC[MAX_DIM];

static bool
str_eq(const char* s, uint32_t len, const char* lit)
{
	uint32_t n = (uint32_t)as_udf_strlen(lit);
	if (n != len) return false;
	for (uint32_t i = 0; i < n; i++) if (s[i] != lit[i]) return false;
	return true;
}

static bool
parse_metric(const char* s, uint32_t len, enum metric* out)
{
	if (len == 0 || str_eq(s, len, "cosine")) { *out = COSINE; return true; }
	if (str_eq(s, len, "dot") || str_eq(s, len, "ip") || str_eq(s, len, "inner_product")) { *out = DOT; return true; }
	if (str_eq(s, len, "sqeuclidean") || str_eq(s, len, "l2sq")) { *out = SQEUCLIDEAN; return true; }
	if (str_eq(s, len, "euclidean") || str_eq(s, len, "l2")) { *out = EUCLIDEAN; return true; }
	return false;
}

/* Read the arguments once: bin, query, metric, k. Returns NULL, or the failure message. */
static const char*
configure(int32_t args, int32_t argc)
{
	as_udf_value a[4];
	int32_t n = as_udf_args(args, argc, a, 4);

	if (n < 2 || a[0].kind != AS_UDF_STR) return "topk: bin name missing";

	int32_t len = as_udf_bytes(&a[0], S.bin, sizeof(S.bin));

	if (len < 0) return "topk: bin name too long";
	S.bin_len = (uint32_t)len;

	if (a[1].kind != AS_UDF_BLOB) return "topk: the query must be a blob of f32";
	if (a[1].len % 4 != 0 || a[1].len / 4 > MAX_DIM) return "topk: the query is not a vector of f32 this module can hold";

	S.dim = a[1].len / 4;

	if (as_udf_bytes(&a[1], S.query, sizeof(S.query)) < 0) return "topk: could not read the query";

	double qn = 0.0;

	for (uint32_t i = 0; i < S.dim; i++) {
		double b = (double)S.query[i];
		qn += b * b;
	}

	S.qnorm = __builtin_sqrt(qn);

	char m[32];
	int32_t mlen = 0;

	if (n > 2 && a[2].kind == AS_UDF_STR) {
		mlen = as_udf_bytes(&a[2], m, sizeof(m));
		if (mlen < 0) return "topk: unknown metric";
	}

	if (! parse_metric(m, (uint32_t)mlen, &S.metric)) return "topk: unknown metric";

	S.k = 10;

	if (n > 3 && a[3].kind == AS_UDF_INT) {
		int64_t k = a[3].payload < 1 ? 1 : a[3].payload;
		S.k = k > MAX_K ? MAX_K : (uint32_t)k;
	}

	S.n = 0;
	S.configured = true;

	return NULL;
}

/*
 * The measured distance (or similarity, for dot) between the record's vector and the query.
 *
 * Four accumulators, lane k holding the elements whose index is k mod 4: a single double sum cannot be
 * vectorised (floating-point addition does not reassociate), four independent ones can live in SIMD registers
 * (-msimd128), and they are folded in one fixed order at the end. The same order is in the Rust and Go
 * modules and in the test's local computation, which keeps the four bit-identical.
 */
static double
measure(void)
{
	const float* v = VEC;
	const float* q = S.query;
	uint32_t n = S.dim;
	uint32_t full = n - n % 4;

	switch (S.metric) {
	case COSINE: {
		double d[4] = { 0.0, 0.0, 0.0, 0.0 };
		double s[4] = { 0.0, 0.0, 0.0, 0.0 };

		for (uint32_t i = 0; i < full; i += 4) {
			for (uint32_t k = 0; k < 4; k++) {
				double a = (double)v[i + k];
				double b = (double)q[i + k];

				d[k] += a * b;
				s[k] += a * a;
			}
		}

		for (uint32_t k = 0; k < n - full; k++) {
			double a = (double)v[full + k];
			double b = (double)q[full + k];

			d[k] += a * b;
			s[k] += a * a;
		}

		double dot = ((d[0] + d[1]) + d[2]) + d[3];
		double na = ((s[0] + s[1]) + s[2]) + s[3];

		if (na == 0.0 || S.qnorm == 0.0) return 1.0;

		return 1.0 - dot / (__builtin_sqrt(na) * S.qnorm);
	}
	case DOT: {
		double d[4] = { 0.0, 0.0, 0.0, 0.0 };

		for (uint32_t i = 0; i < full; i += 4) {
			for (uint32_t k = 0; k < 4; k++) d[k] += (double)v[i + k] * (double)q[i + k];
		}

		for (uint32_t k = 0; k < n - full; k++) d[k] += (double)v[full + k] * (double)q[full + k];

		return ((d[0] + d[1]) + d[2]) + d[3];
	}
	default: {
		double s[4] = { 0.0, 0.0, 0.0, 0.0 };

		for (uint32_t i = 0; i < full; i += 4) {
			for (uint32_t k = 0; k < 4; k++) {
				double e = (double)v[i + k] - (double)q[i + k];
				s[k] += e * e;
			}
		}

		for (uint32_t k = 0; k < n - full; k++) {
			double e = (double)v[full + k] - (double)q[full + k];
			s[k] += e * e;
		}

		double sum = ((s[0] + s[1]) + s[2]) + s[3];

		return S.metric == EUCLIDEAN ? __builtin_sqrt(sum) : sum;
	}
	}
}

/* Larger is worse. */
static double
score_of(double measured)
{
	return S.metric == DOT ? -measured : measured;
}

/* The record's vector into VEC: 1 read, 0 skipped and counted, -1 a failure. */
static int32_t
read_vector(void)
{
	as_udf_value v;
	int32_t rc = as_udf_bin_value(S.bin, (int32_t)S.bin_len, &v, (int32_t)sizeof(v));

	if (rc == AS_UDF_NOT_FOUND || (rc == AS_UDF_OK && v.kind != AS_UDF_BLOB)) {
		S.skipped_none++;
		return 0;
	}

	if (rc != AS_UDF_OK) return -1;

	if (v.len != S.dim * 4) {
		S.skipped_dim++;
		return 0;
	}

	if (as_udf_bytes(&v, VEC, (int32_t)sizeof(VEC)) < 0) return -1;

	return 1;
}

static uint32_t
itoa(int64_t n, char* out)
{
	char tmp[20];
	uint32_t i = 0, len = 0;
	bool negative = n < 0;

	if (n == 0) { out[0] = '0'; return 1; }

	while (n != 0) {
		int64_t d = n % 10;
		tmp[i++] = (char)('0' + (d < 0 ? -d : d));
		n /= 10;
	}

	if (negative) out[len++] = '-';
	while (i > 0) out[len++] = tmp[--i];

	return len;
}

static int
digest_cmp(const uint8_t* a, const uint8_t* b)
{
	for (uint32_t i = 0; i < DIGEST; i++) {
		if (a[i] != b[i]) return a[i] < b[i] ? -1 : 1;
	}
	return 0;
}

/* (score, digest) < (score, digest) */
static bool
better(const hit* a, const hit* b)
{
	if (a->score != b->score) return a->score < b->score;
	return digest_cmp(a->digest, b->digest) < 0;
}

/* Offer a candidate: kept if it beats the worst of the k, ties broken on the digest. */
static bool
offer(double score)
{
	if (S.n == S.k && score > S.hits[S.n - 1].score) return true;

	hit h;

	h.score = score;

	if (as_udf_digest(h.digest, DIGEST) != DIGEST) return false;

	if (S.n == S.k && score == S.hits[S.n - 1].score && digest_cmp(h.digest, S.hits[S.n - 1].digest) >= 0) {
		return true;
	}

	h.key_len = 0;

	switch (as_udf_key_type()) {
	case AS_UDF_INT:
		h.key_len = itoa(as_udf_key_int(), h.key);
		break;
	case AS_UDF_STR:
	case AS_UDF_BLOB: {
		int32_t n = as_udf_key_bytes(h.key, KEY_MAX);
		h.key_len = n < 0 ? 0 : (uint32_t)n;
		break;
	}
	default:
		break;
	}

	uint32_t at = S.n;

	while (at > 0 && better(&h, &S.hits[at - 1])) at--;

	uint32_t last = S.n < S.k ? S.n : S.k - 1;

	if (at > last) return true;

	for (uint32_t i = last; i > at; i--) S.hits[i] = S.hits[i - 1];

	S.hits[at] = h;

	if (S.n < S.k) S.n++;

	return true;
}

UDF_STREAM_RAW(topk, "(bin: str, query: bytes, metric: str, k?: int)", "()")

static int32_t
topk_record_udf_impl(int32_t args, int32_t argc)
{
	if (! S.configured) {
		const char* err = configure(args, argc);
		if (err != NULL) return as_udf_failf(err);
	}

	int32_t read = read_vector();

	if (read < 0) return as_udf_failf("topk: could not read the vector");
	if (read == 0) return 0;

	if (! offer(score_of(measure()))) return as_udf_failf("topk: could not read the digest");

	return 0;
}

static int32_t
topk_end_udf_impl(int32_t args, int32_t argc)
{
	(void)args;
	(void)argc;

	int32_t out = as_udf_map_new(3);
	int32_t hits = as_udf_list_new((int32_t)S.n);

	if (out <= 0 || hits <= 0) return as_udf_failf("topk: could not build the partial");

	for (uint32_t i = 0; i < S.n; i++) {
		const hit* h = &S.hits[i];
		int32_t entry = as_udf_map_new(3);
		as_udf_value pairs[6] = {
			as_udf_v_str("score", 5), as_udf_v_double(h->score),
			as_udf_v_str("digest", 6), as_udf_v_blob(h->digest, DIGEST),
			as_udf_v_str("key", 3), as_udf_v_str(h->key, (int32_t)h->key_len),
		};

		if (entry <= 0 || as_udf_map_put_entries(entry, pairs, (int32_t)sizeof(pairs)) != 3 ||
				as_udf_list_push(hits, entry) != AS_UDF_OK) {
			return as_udf_failf("topk: could not build a hit");
		}
	}

	as_udf_map_put_str_handle(out, "hits", 4, hits);
	as_udf_map_put_str_int(out, "skipped_dim", 11, S.skipped_dim);
	as_udf_map_put_str_int(out, "skipped_none", 12, S.skipped_none);

	return as_udf_result_handle(out) == AS_UDF_OK ? 0 : as_udf_failf("topk: could not return the partial");
}

/* The distance from this record's vector to the query: one record, one number, nil for no such vector. */
UDF_FUNCTION_RAW(vec_distance, "(bin: str, query: bytes, metric: str)")
{
	const char* err = configure(args, argc);

	if (err != NULL) return as_udf_failf(err);

	int32_t read = read_vector();

	if (read < 0) return as_udf_failf("vec_distance: could not read the vector");

	if (read == 0) {
		as_udf_result_nil();
	}
	else {
		as_udf_result_double(measure());
	}

	return 0;
}
