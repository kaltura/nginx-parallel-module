#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <nginx.h>

// macros
#define DIV_CEIL(nom, denom) (((nom) + (denom) - 1) / (denom))

#define RANGE_FORMAT "bytes=%O-%O"
#define CONTENT_RANGE_FORMAT "bytes %O-%O/%O"

// typedefs
typedef struct {
	off_t start;
	off_t end;
	ngx_flag_t is_range_request;
} ngx_http_range_t;

typedef struct {
	ngx_str_t uri_prefix;
	ngx_uint_t fiber_count;
	size_t min_chunk_size;
	size_t max_chunk_size;
	size_t max_headers_size;
	ngx_uint_t max_buffer_count;
	ngx_flag_t consistency_check_etag;
	ngx_flag_t consistency_check_last_modified;

	// derived
	size_t initial_requested_size;
} ngx_http_parallel_loc_conf_t;

typedef struct ngx_http_parallel_fiber_ctx_s {
	ngx_http_headers_in_t headers_in;
	ngx_list_t upstream_headers;
	ngx_http_post_subrequest_t *psr;
	uint64_t chunk_index;
	ngx_http_request_t *sr;
	ngx_chain_t* cl;
	struct ngx_http_parallel_fiber_ctx_s* next;
} ngx_http_parallel_fiber_ctx_t;

typedef struct {
	ngx_str_t sr_uri;
	ngx_http_headers_in_t original_headers_in;
	ngx_http_range_t range;
	ngx_flag_t proxy_as_is;

	ngx_chain_t** chunks;
	uint64_t chunk_count;
	uint64_t missing_chunks;		// a bitmask of chunks that returned 416
	uint64_t next_send_chunk;		// next chunk to send to the client
	uint64_t next_request_chunk;	// next chunk to request from upstream
	size_t chunk_size;
	size_t last_chunk_size;

	ngx_chain_t *free;				// list of free buffers
	ngx_chain_t *busy;				// list of busy buffers (being sent to client)
	ngx_uint_t allocated_count;		// number of allocated buffers

	ngx_int_t error_code;
	ngx_http_event_handler_pt original_write_event_handler;

	ngx_str_t etag;
	ngx_str_t last_modified;

	ngx_uint_t active_fibers;
	ngx_http_parallel_fiber_ctx_t* completed_fiber;
	ngx_http_parallel_fiber_ctx_t* free_fibers;		// a linked list of free fibers
	ngx_http_parallel_fiber_ctx_t fibers[1];		// must be last
} ngx_http_parallel_ctx_t;

// forward declarations
static ngx_int_t ngx_http_parallel_subrequest_finished_handler(
	ngx_http_request_t *r, void *data, ngx_int_t rc);
static char *ngx_http_parallel(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static ngx_int_t ngx_http_parallel_filter_init(ngx_conf_t *cf);
static void *ngx_http_parallel_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_parallel_merge_loc_conf(
	ngx_conf_t *cf, void *parent, void *child);

// constants
static ngx_conf_num_bounds_t  ngx_http_parallel_fiber_count_bounds = {
	ngx_conf_check_num_bounds, 2, 64
};

static ngx_command_t ngx_http_parallel_commands[] = {

	{ ngx_string("parallel"), 
	NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
	ngx_http_parallel,
	NGX_HTTP_LOC_CONF_OFFSET,
	0,
	NULL},

	{ ngx_string("parallel_fiber_count"),
	NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
	ngx_conf_set_num_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_parallel_loc_conf_t, fiber_count),
	&ngx_http_parallel_fiber_count_bounds },

	{ ngx_string("parallel_min_chunk_size"),
	NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
	ngx_conf_set_size_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_parallel_loc_conf_t, min_chunk_size),
	NULL },

	{ ngx_string("parallel_max_chunk_size"),
	NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
	ngx_conf_set_size_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_parallel_loc_conf_t, max_chunk_size),
	NULL },

	{ ngx_string("parallel_max_headers_size"),
	NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
	ngx_conf_set_size_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_parallel_loc_conf_t, max_headers_size),
	NULL },

	{ ngx_string("parallel_max_buffer_count"),
	NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
	ngx_conf_set_num_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_parallel_loc_conf_t, max_buffer_count),
	NULL },

	{ ngx_string("parallel_consistency_check_etag"),
	NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
	ngx_conf_set_flag_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_parallel_loc_conf_t, consistency_check_etag),
	NULL },

	{ ngx_string("parallel_consistency_check_last_modified"),
	NGX_HTTP_MAIN_CONF | NGX_HTTP_SRV_CONF | NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
	ngx_conf_set_flag_slot,
	NGX_HTTP_LOC_CONF_OFFSET,
	offsetof(ngx_http_parallel_loc_conf_t, consistency_check_last_modified),
	NULL },

	ngx_null_command
};

static ngx_http_module_t ngx_http_parallel_module_ctx = {
	NULL,								/* preconfiguration */
	ngx_http_parallel_filter_init,		/* postconfiguration */

	NULL,								/* create main configuration */
	NULL,								/* init main configuration */

	NULL,								/* create server configuration */
	NULL,								/* merge server configuration */

	ngx_http_parallel_create_loc_conf,	/* create location configuration */
	ngx_http_parallel_merge_loc_conf	/* merge location configuration */
};

ngx_module_t ngx_http_parallel_module = {
	NGX_MODULE_V1,
	&ngx_http_parallel_module_ctx, /* module context */
	ngx_http_parallel_commands,    /* module directives */
	NGX_HTTP_MODULE,               /* module type */
	NULL,                          /* init master */
	NULL,                          /* init module */
	NULL,                          /* init process */
	NULL,                          /* init thread */
	NULL,                          /* exit thread */
	NULL,                          /* exit process */
	NULL,                          /* exit master */
	NGX_MODULE_V1_PADDING
};

static ngx_uint_t content_range_hash = 
	ngx_hash(ngx_hash(ngx_hash(ngx_hash(ngx_hash(ngx_hash(
	ngx_hash(ngx_hash(ngx_hash(ngx_hash(ngx_hash(ngx_hash(
	'c', 'o'), 'n'), 't'), 'e'), 'n'), 't'), '-'), 'r'), 'a'), 'n'), 'g'), 'e');
static ngx_str_t content_range_name = ngx_string("content-range");

// globals
static ngx_http_output_body_filter_pt ngx_http_next_body_filter;

// Note: the code below is a slightly modified version of ngx_http_range_parse, 
//	it has a few limitations compared to the code found there:
//	1. only a single range is supported
//	2. suffix ranges (e.g -100) are not supported
static ngx_int_t
ngx_http_parallel_range_parse(ngx_http_request_t *r, ngx_http_range_t* result)
{
	u_char            *p;
	off_t              start, end, cutoff, cutlim;

	if (r->headers_in.range->value.len < 7 || 
		ngx_strncasecmp(r->headers_in.range->value.data, 
		                (u_char *) "bytes=", 6) != 0) {
		return NGX_HTTP_RANGE_NOT_SATISFIABLE;
	}

	p = r->headers_in.range->value.data + 6;

	cutoff = NGX_MAX_OFF_T_VALUE / 10;
	cutlim = NGX_MAX_OFF_T_VALUE % 10;

	start = 0;
	end = 0;

	while (*p == ' ') { p++; }

	if (*p < '0' || *p > '9') {
		return NGX_HTTP_RANGE_NOT_SATISFIABLE;
	}

	while (*p >= '0' && *p <= '9') {
		if (start >= cutoff && (start > cutoff || *p - '0' > cutlim)) {
			return NGX_HTTP_RANGE_NOT_SATISFIABLE;
		}

		start = start * 10 + *p++ - '0';
	}

	while (*p == ' ') { p++; }

	if (*p++ != '-') {
		return NGX_HTTP_RANGE_NOT_SATISFIABLE;
	}

	while (*p == ' ') { p++; }

	if (*p == '\0') {
		end = 0;		// 0 signifies no limit
		goto found;
	}

	if (*p < '0' || *p > '9') {
		return NGX_HTTP_RANGE_NOT_SATISFIABLE;
	}

	while (*p >= '0' && *p <= '9') {
		if (end >= cutoff && (end > cutoff || *p - '0' > cutlim)) {
			return NGX_HTTP_RANGE_NOT_SATISFIABLE;
		}

		end = end * 10 + *p++ - '0';
	}

	while (*p == ' ') { p++; }

	if (*p != '\0') {
		return NGX_HTTP_RANGE_NOT_SATISFIABLE;
	}

	end++;

found:

	if (end != 0 && start >= end) {
		return NGX_HTTP_RANGE_NOT_SATISFIABLE;
	}
	
	result->start = start;
	result->end = end;

	return NGX_OK;
}

static void *
ngx_http_parallel_memrchr(const u_char *s, int c, size_t n)
{
	const u_char *cp;

	for (cp = s + n; cp > s;)
	{
		if (*(--cp) == (u_char)c)
		{
			return (void*)cp;
		}
	}
	return NULL;
}

static ngx_uint_t
ngx_http_parallel_list_get_count(ngx_list_t* list)
{
	ngx_list_part_t *part;
	ngx_uint_t result;

	result = 0;
	for (part = &list->part; part; part = part->next)
	{
		result += part->nelts;
	}
	return result;
}

static ngx_int_t
ngx_http_parallel_list_copy(
	ngx_http_request_t* r, 
	ngx_list_t* dest, 
	ngx_list_t* src, 
	ngx_uint_t n, 
	size_t size)
{
	ngx_list_part_t *part;
	ngx_int_t rc;
	u_char* p;

	rc = ngx_list_init(dest, r->pool, n, size);
	if (rc != NGX_OK)
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"ngx_http_parallel_list_copy: ngx_list_init failed");
		return NGX_ERROR;
	}

	p = (u_char*)dest->last->elts;

	for (part = &r->headers_in.headers.part; part; part = part->next)
	{
		p = ngx_copy(p, part->elts, size * part->nelts);
		dest->last->nelts += part->nelts;
	}

	return NGX_OK;
}

// the following function updates the pointers in headers_in (e.g. range) 
//	to point to the respective elements inside the headers_in->headers array
static void
ngx_http_parallel_update_headers(
	ngx_http_request_t* r, 
	ngx_http_headers_in_t* headers_in)
{
	ngx_http_core_main_conf_t  *cmcf;
	ngx_http_header_t *hh;
	ngx_list_part_t *part;
	ngx_table_elt_t  **ph;
	ngx_table_elt_t *h;
	ngx_uint_t i;

	cmcf = ngx_http_get_module_main_conf(r, ngx_http_core_module);

	part = &headers_in->headers.part;
	h = part->elts;

	for (i = 0; /* void */; i++)
	{
		if (i >= part->nelts)
		{
			if (part->next == NULL)
			{
				break;
			}

			part = part->next;
			h = part->elts;
			i = 0;
		}

		hh = ngx_hash_find(&cmcf->headers_in_hash, h[i].hash,
			h[i].lowcase_key, h[i].key.len);
		if (!hh)
		{
			continue;
		}

		ph = (ngx_table_elt_t **)((char *)headers_in + hh->offset);

		*ph = &h[i];
	}
}

static ngx_str_t*
ngx_http_parallel_header_get_value(
	ngx_list_t* headers, 
	ngx_str_t* name, 
	ngx_uint_t hash)
{
	ngx_list_part_t *part;
	ngx_table_elt_t *h;
	ngx_uint_t i;

	part = &headers->part;
	h = part->elts;

	for (i = 0; /* void */; i++)
	{
		if (i >= part->nelts)
		{
			if (part->next == NULL)
			{
				break;
			}

			part = part->next;
			h = part->elts;
			i = 0;
		}

		if (h[i].hash == hash &&
			h[i].key.len == name->len &&
			ngx_memcmp(h[i].lowcase_key, name->data, name->len) == 0)
		{
			return &h[i].value;
		}
	}

	return NULL;
}

static void
ngx_http_parallel_header_clear_value(
	ngx_list_t* headers, 
	ngx_str_t* name, 
	ngx_uint_t hash)
{
	ngx_list_part_t *part;
	ngx_table_elt_t *h;
	ngx_uint_t i = 0;

	part = &headers->part;
	h = part->elts;

	for (;;)
	{
		if (i >= part->nelts)
		{
			if (part->next == NULL)
			{
				break;
			}

			part = part->next;
			h = part->elts;
			i = 0;
		}

		if (h[i].hash != hash ||
			h[i].key.len != name->len ||
			ngx_memcmp(h[i].lowcase_key, name->data, name->len) != 0)
		{
			i++;
			continue;
		}

		ngx_memmove(h + i, h + i + 1, (part->nelts - i - 1) * sizeof(*h));
		part->nelts--;
	}
}

static off_t
ngx_http_parallel_get_instance_length(ngx_str_t *content_range, ngx_log_t* log)
{
	u_char* last_slash;
	off_t content_length;

	last_slash = ngx_http_parallel_memrchr(
		content_range->data, '/', content_range->len);
	if (last_slash == NULL)
	{
		ngx_log_error(NGX_LOG_ERR, log, 0,
			"ngx_http_parallel_get_instance_length: "
			"no slash in content-range value \"%V\"", content_range);
		return -1;
	}

	content_length = ngx_atoof(
		last_slash + 1, 
		content_range->data + content_range->len - last_slash - 1);
	if (content_length < 0)
	{
		ngx_log_error(NGX_LOG_ERR, log, 0,
			"ngx_http_parallel_get_instance_length: failed to parse "
			"instance length from content-range \"%V\"", content_range);
		return -1;
	}

	return content_length;
}

static ngx_int_t
ngx_http_parallel_init_fiber(
	ngx_http_request_t *r,
	ngx_uint_t header_in_count, 
	ngx_flag_t proxy_as_is,
	ngx_http_parallel_fiber_ctx_t* fiber)
{
	ngx_table_elt_t *h;
	ngx_int_t rc;

	// create the post-subrequest
	fiber->psr = ngx_palloc(r->pool, sizeof(ngx_http_post_subrequest_t));
	if (fiber->psr == NULL) 
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"ngx_http_parallel_init_fiber: ngx_palloc failed");
		return NGX_ERROR;
	}

	fiber->psr->handler = ngx_http_parallel_subrequest_finished_handler;
	fiber->psr->data = fiber;

	// copy the input headers
	fiber->headers_in = r->headers_in;
	
	if (proxy_as_is)
	{
		return NGX_OK;
	}

	// copy the headers list in order to update it
	rc = ngx_http_parallel_list_copy(
		r,
		&fiber->headers_in.headers,
		&r->headers_in.headers,
		header_in_count + 1,
		sizeof(ngx_table_elt_t));
	if (rc != NGX_OK)
	{
		return rc;
	}

	ngx_http_parallel_update_headers(r, &fiber->headers_in);

	// add a range header
	if (fiber->headers_in.range == NULL)
	{
		h = ngx_list_push(&fiber->headers_in.headers);
		if (h == NULL)
		{
			ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
				"ngx_http_parallel_init_fiber: ngx_list_push failed");
			return NGX_ERROR;
		}

		h->hash = ngx_hash(ngx_hash(ngx_hash(ngx_hash('r', 'a'), 'n'), 'g'), 'e');

		h->key.data = (u_char*)"Range";
		h->key.len = sizeof("Range") - 1;

		h->lowcase_key = (u_char*)"range";

		fiber->headers_in.range = h;
	}
	else
	{
		h = fiber->headers_in.range;
	}

	h->value.data = ngx_pnalloc(r->pool, sizeof(RANGE_FORMAT) + 2 * NGX_OFF_T_LEN);
	if (h->value.data == NULL)
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"ngx_http_parallel_init_fiber: ngx_pnalloc failed");
		return NGX_ERROR;
	}
	
	return NGX_OK;
}

// this function is required in order to support buffer reuse - it hooks the 
//	initial call to ngx_http_handler. after this function returns the upstream 
//	is already allocated by the proxy module, so it's possible to set its buffer
static void
ngx_http_parallel_fiber_initial_wev_handler(ngx_http_request_t *r)
{
	ngx_http_parallel_fiber_ctx_t* fiber;
	ngx_http_upstream_t* u;
	ngx_connection_t    *c;

	c = r->connection;

	// call the default request handler
	r->write_event_handler = ngx_http_handler;
	ngx_http_handler(r);

	// if request was destoryed ignore
	if (c->destroyed)
	{
		return;
	}

	// at this point the upstream should have been allocated by the proxy module
	u = r->upstream;
	if (u == NULL)
	{
		return;
	}

	// initialize the upstream buffer
	fiber = ngx_http_get_module_ctx(r, ngx_http_parallel_module);
	u->buffer = *fiber->cl->buf;

	// initialize the headers list
	u->headers_in.headers = fiber->upstream_headers;
	u->headers_in.headers.last = &u->headers_in.headers.part;
}

static ngx_int_t 
ngx_http_parallel_start_fiber(
	ngx_http_request_t *r,
	ngx_http_parallel_fiber_ctx_t* fiber, 
	uint64_t chunk_index)
{
	ngx_http_parallel_loc_conf_t *conf;
	ngx_http_parallel_ctx_t *ctx;
	ngx_http_request_t* sr;
	ngx_str_t args = ngx_null_string;
	ngx_table_elt_t *h;
	ngx_int_t rc;
	size_t alloc_size;
	off_t start_offset;
	off_t end_offset;
	ngx_chain_t* cl;
	ngx_buf_t* b;

	ctx = ngx_http_get_module_ctx(r, ngx_http_parallel_module);
	conf = ngx_http_get_module_loc_conf(r, ngx_http_parallel_module);

	// get a buffer for the response
	cl = ngx_chain_get_free_buf(r->pool, &ctx->free);
	if (cl == NULL) 
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"ngx_http_parallel_start_fiber: ngx_chain_get_free_buf failed");
		return NGX_ERROR;
	}

	b = cl->buf;

	if (cl->buf->start == NULL)
	{
		alloc_size = conf->max_chunk_size + conf->max_headers_size;
		b->start = ngx_palloc(r->pool, alloc_size);
		if (b->start == NULL) 
		{
			ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
				"ngx_http_parallel_start_fiber: ngx_palloc failed");
			return NGX_ERROR;
		}

		b->pos = b->start;
		b->last = b->start;
		b->end = b->last + alloc_size;
		b->temporary = 1;
		b->tag = (ngx_buf_tag_t)&ngx_http_parallel_module;

		ctx->allocated_count++;
	}

	// allocate a list for the input headers
	if (ngx_list_init(&fiber->upstream_headers, r->pool, 8, 
					  sizeof(ngx_table_elt_t)) != NGX_OK)
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"ngx_http_parallel_start_fiber: ngx_list_init failed");
		return NGX_ERROR;
	}

	// set the range
	if (!ctx->proxy_as_is)
	{
		start_offset = ctx->range.start;
		if (chunk_index < conf->fiber_count)
		{
			start_offset += chunk_index * ctx->chunk_size;
		}
		else
		{
			start_offset += conf->initial_requested_size + 
				(chunk_index - conf->fiber_count) * ctx->chunk_size;
		}

		end_offset = start_offset + ctx->chunk_size;
		if (ctx->range.end != 0 && ctx->range.end < end_offset)
		{
			end_offset = ctx->range.end;
		}

		h = fiber->headers_in.range;
		h->value.len = ngx_sprintf(
			h->value.data, 
			RANGE_FORMAT, 
			start_offset, 
			end_offset - 1) - h->value.data;
		h->value.data[h->value.len] = '\0';
	}
	
	// start the request
	r->headers_in = fiber->headers_in;
	rc = ngx_http_subrequest(r, &ctx->sr_uri, &args, &sr, fiber->psr, 
		NGX_HTTP_SUBREQUEST_WAITED | NGX_HTTP_SUBREQUEST_IN_MEMORY);
	r->headers_in = ctx->original_headers_in;
	
	if (rc != NGX_OK)
	{
		ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"ngx_http_parallel_start_fiber: ngx_http_subrequest failed %i", rc);
		return rc;
	}

	ngx_http_set_ctx(sr, fiber, ngx_http_parallel_module);

	ctx->active_fibers++;

	sr->write_event_handler = ngx_http_parallel_fiber_initial_wev_handler;
	sr->method = r->method;				// copy the method to the subrequest
	sr->method_name = r->method_name;	// (ngx_http_subrequest always uses GET)
	sr->header_in = r->header_in;

	// fix the last pointer in headers_in (from echo-nginx-module)
	if (fiber->headers_in.headers.last == &fiber->headers_in.headers.part) {
		sr->headers_in.headers.last = &sr->headers_in.headers.part;
	}
	
	fiber->chunk_index = chunk_index;
	fiber->cl = cl;
	fiber->sr = sr;
	
	ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, 
		"ngx_http_parallel_start_fiber: started fiber %uL", chunk_index);

	return NGX_OK;
}

static ngx_int_t
ngx_http_parallel_start_fibers(
	ngx_http_parallel_ctx_t *ctx,
	ngx_http_request_t *r, 
	ngx_http_parallel_loc_conf_t *conf)
{
	ngx_http_parallel_fiber_ctx_t* fiber;
	ngx_int_t rc;

	while (ctx->free_fibers && 
		(ctx->free || ctx->allocated_count < conf->max_buffer_count) &&	
		ctx->next_request_chunk < ctx->chunk_count)
	{
		// remove from free list
		fiber = ctx->free_fibers;
		ctx->free_fibers = fiber->next;

		// start the fiber
		rc = ngx_http_parallel_start_fiber(r, fiber, ctx->next_request_chunk);
		if (rc != NGX_OK)
		{
			return rc;
		}

		ctx->next_request_chunk++;
	}

	return NGX_OK;
}

static ngx_int_t 
ngx_http_parallel_init_chunks(
	ngx_http_parallel_ctx_t *ctx,
	ngx_http_request_t *r, 
	ngx_http_headers_out_t* headers_out)
{
	ngx_http_parallel_loc_conf_t *conf;
	ngx_table_elt_t *h;
	ngx_str_t* content_range;
	uint64_t missing_chunks_mask;
	off_t instance_length;
	off_t remaining_length;
	off_t content_length;
	ngx_int_t rc;

	// find the content length
	content_range = ngx_http_parallel_header_get_value(
		&headers_out->headers, 
		&content_range_name, 
		content_range_hash);
	if (content_range == NULL)
	{
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
			"ngx_http_parallel_init_chunks: failed to get content-range header");
		return NGX_HTTP_BAD_GATEWAY;
	}

	instance_length = ngx_http_parallel_get_instance_length(
		content_range, 
		r->connection->log);
	if (instance_length < 0)
	{
		return NGX_HTTP_BAD_GATEWAY;
	}

	content_length = instance_length;

	if (ctx->range.end != 0 && ctx->range.end < content_length)
	{
		content_length = ctx->range.end;
	}

	if (content_length < ctx->range.start)
	{
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
			"ngx_http_parallel_init_chunks: "
			"unexpected, content length %O less than range start %O", 
			content_length, ctx->range.start);
		return NGX_HTTP_BAD_GATEWAY;
	}

	content_length -= ctx->range.start;

	// find the chunk size and count
	conf = ngx_http_get_module_loc_conf(r, ngx_http_parallel_module);

	if (content_length <= (off_t)conf->initial_requested_size)
	{
		ctx->chunk_count = DIV_CEIL(content_length, ctx->chunk_size);
		ctx->last_chunk_size = content_length + ctx->chunk_size - 
			ctx->chunk_count * ctx->chunk_size;
	}
	else
	{
		remaining_length = content_length - conf->initial_requested_size;
		ctx->chunk_size = DIV_CEIL(remaining_length, conf->fiber_count);
		if (ctx->chunk_size < conf->min_chunk_size)
		{
			ctx->chunk_size = conf->min_chunk_size;
		}
		else if (ctx->chunk_size > conf->max_chunk_size)
		{
			ctx->chunk_size = conf->max_chunk_size;
		}
		ctx->chunk_count = DIV_CEIL(remaining_length, ctx->chunk_size);
		ctx->last_chunk_size = remaining_length + ctx->chunk_size - 
			ctx->chunk_count * ctx->chunk_size;
		ctx->chunk_count += conf->fiber_count;
	}

	ngx_log_debug3(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
		"ngx_http_parallel_init_chunks: "
		"chunk count %uL chunk size %uz last chunk size %uz", 
		ctx->chunk_count, ctx->chunk_size, ctx->last_chunk_size);

	// check for missing chunks
	missing_chunks_mask = ULLONG_MAX;
	if (ctx->chunk_count < 64)
	{
		missing_chunks_mask = ((1ULL << ctx->chunk_count) - 1);
	}

	if ((ctx->missing_chunks & missing_chunks_mask) != 0)
	{
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
			"ngx_http_parallel_init_chunks: "
			"missing chunks 0x%uxL chunk count %uL", 
			ctx->missing_chunks, ctx->chunk_count);
		return NGX_HTTP_BAD_GATEWAY;
	}

	// initialize the chunks array (null terminated)
	ctx->chunks = ngx_pcalloc(r->pool, sizeof(ctx->chunks[0]) * (ctx->chunk_count + 1));
	if (ctx->chunks == NULL)
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"ngx_http_parallel_init_chunks: ngx_pcalloc failed");
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}

	// save headers for consistency check
	if (conf->consistency_check_etag)
	{
		h = headers_out->etag;
		if (h != NULL)
		{
			ctx->etag.data = ngx_pstrdup(r->pool, &h->value);
			if (ctx->etag.data == NULL)
			{
				ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
					"ngx_http_parallel_init_chunks: ngx_pstrdup failed (1)");
				return NGX_HTTP_INTERNAL_SERVER_ERROR;
			}

			ctx->etag.len = h->value.len;
		}
	}

	if (conf->consistency_check_last_modified)
	{
		h = headers_out->last_modified;
		if (h != NULL)
		{
			ctx->last_modified.data = ngx_pstrdup(r->pool, &h->value);
			if (ctx->last_modified.data == NULL)
			{
				ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
					"ngx_http_parallel_init_chunks: ngx_pstrdup failed (2)");
				return NGX_HTTP_INTERNAL_SERVER_ERROR;
			}

			ctx->last_modified.len = h->value.len;
		}
	}

	// build the response headers
	r->headers_out = *headers_out;

	if (ctx->range.is_range_request)
	{
		// leave the status as 206 and update the content range
		content_range->data = ngx_pnalloc(
			r->pool, sizeof(CONTENT_RANGE_FORMAT) + 3 * NGX_OFF_T_LEN);
		if (content_range->data == NULL)
		{
			ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
				"ngx_http_parallel_init_chunks: ngx_pnalloc failed");
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}

		content_range->len = ngx_sprintf(content_range->data,
			CONTENT_RANGE_FORMAT,
			ctx->range.start,
			ctx->range.start + content_length - 1,
			instance_length) - content_range->data;
	}
	else
	{
		// change status to 200 and clear the content range
		r->headers_out.status = NGX_HTTP_OK;
		r->headers_out.status_line.len = 0;

		ngx_http_parallel_header_clear_value(
			&r->headers_out.headers, &content_range_name, content_range_hash);
	}

	ngx_http_clear_content_length(r);
	r->headers_out.content_length_n = content_length;

	// send the response headers
	rc = ngx_http_send_header(r);
	if (rc == NGX_ERROR || rc > NGX_OK)
	{
		ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"ngx_http_parallel_init_chunks: ngx_http_send_header failed %i", rc);
		return rc;
	}

	return NGX_OK;
}

static ngx_flag_t
ngx_http_parallel_check_consistency(
	ngx_http_parallel_ctx_t *ctx,
	ngx_http_request_t *r, 
	ngx_http_parallel_loc_conf_t *conf)
{
	ngx_table_elt_t* h;

	if (conf->consistency_check_etag)
	{
		h = r->headers_out.etag;
		if (h != NULL)
		{
			if (ctx->etag.len != h->value.len ||
				ngx_memcmp(ctx->etag.data, h->value.data, ctx->etag.len) != 0)
			{
				ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
					"ngx_http_parallel_check_consistency: "
					"inconsistent etag, original=\"%V\" current=\"%V\"",
					&ctx->etag, &h->value);
				return 0;
			}
		}
		else
		{
			if (ctx->etag.len != 0)
			{
				ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
					"ngx_http_parallel_check_consistency: "
					"inconsistent etag, original=\"%V\" current=\"\"",
					&ctx->etag);
				return 0;
			}
		}
	}

	if (conf->consistency_check_last_modified)
	{
		h = r->headers_out.last_modified;
		if (h != NULL)
		{
			if (ctx->last_modified.len != h->value.len ||
				ngx_memcmp(ctx->last_modified.data, h->value.data, 
						ctx->last_modified.len) != 0)
			{
				ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
					"ngx_http_parallel_check_consistency: "
					"inconsistent last-modified, original=\"%V\" current=\"%V\"",
					&ctx->last_modified, &h->value);
				return 0;
			}
		}
		else
		{
			if (ctx->last_modified.len != 0)
			{
				ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
					"ngx_http_parallel_check_consistency: "
					"inconsistent last-modified, original=\"%V\" current=\"\"",
					&ctx->last_modified);
				return 0;
			}
		}
	}

	return 1;
}

static ngx_int_t
ngx_http_parallel_send_buffer(
	ngx_http_parallel_ctx_t *ctx,
	ngx_http_request_t *r, 
	ngx_chain_t* out)
{
	ngx_chain_t* cl;
	ngx_int_t rc;

	// add current buffer to chain
	cl = out;
	ctx->next_send_chunk++;

	// add any previously fetched contiguous buffers to the chain
	for (; ctx->chunks[ctx->next_send_chunk]; ctx->next_send_chunk++)
	{
		cl->next = ctx->chunks[ctx->next_send_chunk];
		cl = cl->next;
	}

	// set the last buf flag if needed
	if (ctx->next_send_chunk >= ctx->chunk_count)
	{
		cl->buf->last_buf = 1;
		ctx->error_code = NGX_OK;
	}

	// send the chain
	rc = ngx_http_output_filter(r, out);
	if (rc != NGX_OK && rc != NGX_AGAIN)
	{
		ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"ngx_http_parallel_send_buffer: "
			"ngx_http_output_filter failed %i", rc);
		return rc;
	}

	// update the free / busy chains
	ngx_chain_update_chains(r->pool, &ctx->free, &ctx->busy, &out, 
		(ngx_buf_tag_t)&ngx_http_parallel_module);

	return NGX_OK;
}

static ngx_int_t
ngx_http_parallel_body_filter(ngx_http_request_t *r, ngx_chain_t *in)
{
	ngx_http_parallel_loc_conf_t *conf;
	ngx_http_parallel_ctx_t *ctx;
	ngx_http_request_t *pr;
	ngx_chain_t* out;
	ngx_int_t rc;

	// ignore any requests that were not handled by this module
	ctx = ngx_http_get_module_ctx(r, ngx_http_parallel_module);
	if (ctx == NULL)
	{
		return ngx_http_next_body_filter(r, in);
	}

	// ignore subrequests of requests handled by this module
	pr = r->parent;
	if (pr && ngx_http_get_module_ctx(pr, ngx_http_parallel_module))
	{
		return ngx_http_next_body_filter(r, in);
	}

	rc = ngx_http_next_body_filter(r, in);

	// check whether a buffer was finished
	if (ctx->busy && ngx_buf_size(ctx->busy->buf) == 0 && 
		ctx->free_fibers &&
		ctx->next_request_chunk < ctx->chunk_count)
	{
		// move the buffer to the free chain
		out = NULL;
		ngx_chain_update_chains(r->pool, &ctx->free, &ctx->busy, &out, 
			(ngx_buf_tag_t)&ngx_http_parallel_module);

		// start fibers if possible
		conf = ngx_http_get_module_loc_conf(r, ngx_http_parallel_module);
		rc = ngx_http_parallel_start_fibers(ctx, r, conf);
		if (rc != NGX_OK)
		{
			return NGX_ERROR;
		}
	}

	return rc;
}

static ngx_int_t
ngx_http_parallel_handle_request_complete(
	ngx_http_parallel_ctx_t *ctx, 
	ngx_http_request_t *r, 
	ngx_http_parallel_fiber_ctx_t* fiber)
{
	ngx_http_parallel_loc_conf_t *conf;
	ngx_http_upstream_t* u;
	ngx_http_request_t* pr;
	ngx_buf_t* b;
	ngx_int_t rc;
	off_t expected_size;

	ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0, 
		"ngx_http_parallel_handle_request_complete: "
		"fiber finished %uL", fiber->chunk_index);

	ctx->active_fibers--;

	// add the fiber to the free list
	fiber->next = ctx->free_fibers;
	ctx->free_fibers = fiber;

	if (ctx->error_code != NGX_AGAIN)
	{
		return NGX_OK;
	}

	if (r->headers_out.status == NGX_HTTP_RANGE_NOT_SATISFIABLE && 
		fiber->chunk_index != 0)
	{
		if (ctx->chunks == NULL)
		{
			// don't have the response length yet, flag this chunk as missing
			ctx->missing_chunks |= (1 << fiber->chunk_index);
		}
		else
		{
			if (fiber->chunk_index < ctx->chunk_count)
			{
				ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
					"ngx_http_parallel_handle_request_complete: "
					"got 416 error for a required chunk %uL/%uL", 
					fiber->chunk_index, ctx->chunk_count);
				return NGX_HTTP_BAD_GATEWAY;
			}
		}
		return NGX_OK;
	}
	
	pr = r->parent;

	if (r->headers_out.status != NGX_HTTP_PARTIAL_CONTENT && pr->header_sent)
	{
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
			"ngx_http_parallel_handle_request_complete: "
			"unexpected status %ui after getting 206");
		return NGX_HTTP_BAD_GATEWAY;
	}

	// update the buffer pointers from the upstream buffer
	u = r->upstream;
	if (u == NULL)
	{
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
			"ngx_http_parallel_handle_request_complete: "
			"unexpected, subrequest has no upstream");
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}

	b = fiber->cl->buf;
	*b = u->buffer;
	b->last = b->pos + u->state->response_length;

	if (r->headers_out.status != NGX_HTTP_PARTIAL_CONTENT || 
		ctx->proxy_as_is)
	{
		// copy the response headers from upstream
		pr->headers_out = r->headers_out;

		rc = ngx_http_send_header(pr);
		if (rc == NGX_ERROR || rc > NGX_OK)
		{
			ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
				"ngx_http_parallel_handle_request_complete: "
				"ngx_http_send_header failed %i", rc);
			return rc;
		}

		// send the buffer
		b->last_buf = 1;
		ctx->error_code = NGX_OK;

		if (ngx_buf_size(b) == 0)
		{
			b->temporary = b->memory = 0;
		}

		rc = ngx_http_output_filter(pr, fiber->cl);
		if (rc != NGX_OK && rc != NGX_AGAIN)
		{
			ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
				"ngx_http_parallel_handle_request_complete: "
				"ngx_http_output_filter failed %i", rc);
			return rc;
		}

		return NGX_OK;
	}

	// initialize the chunks array (on first completed chunk)
	conf = ngx_http_get_module_loc_conf(pr, ngx_http_parallel_module);

	if (ctx->chunks == NULL)
	{
		rc = ngx_http_parallel_init_chunks(ctx, pr, &r->headers_out);
		if (rc != NGX_OK)
		{
			return rc;
		}
	}
	else
	{
		if (!ngx_http_parallel_check_consistency(ctx, r, conf))
		{
			return NGX_HTTP_BAD_GATEWAY;
		}
	}

	if (fiber->chunk_index >= ctx->chunk_count)
	{
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
			"ngx_http_parallel_handle_request_complete: "
			"unexpected, chunk index %uL exceeds chunk count %uL", 
			fiber->chunk_index, ctx->chunk_count);
		return NGX_HTTP_BAD_GATEWAY;
	}

	// validate content length
	if (fiber->chunk_index + 1 == ctx->chunk_count)
	{
		expected_size = ctx->last_chunk_size;
	}
	else if (fiber->chunk_index < conf->fiber_count)
	{
		expected_size = conf->min_chunk_size;
	}
	else
	{
		expected_size = ctx->chunk_size;
	}

	if (u->state->response_length != expected_size)
	{
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
			"ngx_http_parallel_handle_request_complete: "
			"unexpected response length %O in chunk %uL/%uL, expected %O", 
			u->state->response_length, fiber->chunk_index, ctx->chunk_count, 
			expected_size);
		return NGX_HTTP_BAD_GATEWAY;
	}

	if (fiber->chunk_index == ctx->next_send_chunk)
	{
		// send the buffer and any queued buffers
		rc = ngx_http_parallel_send_buffer(ctx, pr, fiber->cl);
		if (rc != NGX_OK)
		{
			return rc;
		}
	}
	else
	{
		// save the buffer to be sent later
		ctx->chunks[fiber->chunk_index] = fiber->cl;
	}

	// start fibers if possible
	rc = ngx_http_parallel_start_fibers(ctx, pr, conf);
	if (rc != NGX_OK)
	{
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}

	return NGX_OK;
}

static void
ngx_http_parallel_wev_handler(ngx_http_request_t *r)
{
	ngx_http_parallel_fiber_ctx_t* fiber;
	ngx_http_parallel_ctx_t *ctx;
	ngx_int_t rc;

	ctx = ngx_http_get_module_ctx(r, ngx_http_parallel_module);

	// restore the write event handler
	r->write_event_handler = ctx->original_write_event_handler;
	ctx->original_write_event_handler = NULL;

	// get the fiber
	fiber = ctx->completed_fiber;
	ctx->completed_fiber = NULL;

	if (fiber == NULL)
	{
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
			"ngx_http_parallel_wev_handler: unexpected, fiber is null");
		return;
	}

	// code taken from echo-nginx-module to work around nginx subrequest issues
	if (r == r->connection->data && r->postponed) {

		if (r->postponed->request) {
			r->connection->data = r->postponed->request;

#if defined(nginx_version) && nginx_version >= 8012
			ngx_http_post_request(r->postponed->request, NULL);
#else
			ngx_http_post_request(r->postponed->request);
#endif

		}
		else {
			ngx_http_output_filter(r, NULL);
		}
	}

	rc = ngx_http_parallel_handle_request_complete(ctx, fiber->sr, fiber);
	if (rc != NGX_OK && ctx->error_code == NGX_AGAIN)
	{
		ctx->error_code = rc;
	}

	if (ctx->error_code != NGX_AGAIN && ctx->active_fibers == 0)
	{
		ngx_http_finalize_request(r, ctx->error_code);
	}
}

static ngx_int_t
ngx_http_parallel_subrequest_finished_handler(
	ngx_http_request_t *r, 
	void *data, 
	ngx_int_t rc)
{
	ngx_http_request_t          *pr;
	ngx_http_parallel_ctx_t *ctx;

	// make sure we are not called twice for the same request
	r->post_subrequest = NULL;

	pr = r->parent;

	// save the completed fiber in the context for the write event handler
	ctx = ngx_http_get_module_ctx(pr, ngx_http_parallel_module);

	ctx->completed_fiber = data;
	if (rc != NGX_OK && ctx->error_code == NGX_AGAIN)
	{
		ctx->error_code = rc;
	}

	if (ctx->original_write_event_handler != NULL)
	{
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
			"ngx_http_parallel_subrequest_finished_handler: "
			"unexpected original_write_event_handler not null");
		return NGX_ERROR;
	}

	ctx->original_write_event_handler = pr->write_event_handler;
	pr->write_event_handler = ngx_http_parallel_wev_handler;

	/* work-around issues in nginx's event module */

	if (r != r->connection->data
		&& r->postponed
		&& (r->main->posted_requests == NULL
		|| r->main->posted_requests->request != pr))
	{
#if defined(nginx_version) && nginx_version >= 8012
		ngx_http_post_request(pr, NULL);
#else
		ngx_http_post_request(pr);
#endif
	}

	return NGX_OK;
}

static ngx_int_t 
ngx_http_parallel_handler(ngx_http_request_t *r)
{
	ngx_http_parallel_loc_conf_t *conf;
	ngx_http_parallel_ctx_t* ctx;
	ngx_http_range_t range = { 0, 0, 0 };
	ngx_uint_t header_in_count;
	ngx_uint_t fiber_count;
	ngx_uint_t i;
	ngx_int_t rc;
	u_char* p;

	// validate method
	if (!(r->method & (NGX_HTTP_GET | NGX_HTTP_HEAD)))
	{
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
			"ngx_http_parallel_handler: "
			"unsupported method %ui", r->method);
		return NGX_HTTP_NOT_ALLOWED;
	}

	// discard request body, since we don't need it here
	rc = ngx_http_discard_request_body(r);
	if (rc != NGX_OK)
	{
		ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"ngx_http_parallel_handler: "
			"ngx_http_discard_request_body failed %i", rc);
		return rc;
	}

	// get fiber count
	conf = ngx_http_get_module_loc_conf(r, ngx_http_parallel_module);

	fiber_count = conf->fiber_count;

	if (r->method == NGX_HTTP_HEAD)
	{
		fiber_count = 1;
	}
	else if (r->headers_in.range != NULL)
	{
		rc = ngx_http_parallel_range_parse(r, &range);
		if (rc != NGX_OK)
		{
			ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
				"ngx_http_parallel_handler: "
				"ngx_http_parallel_range_parse failed \"%V\", "
				"proxying the request as is", &r->headers_in.range->value);
			fiber_count = 1;
		}
		else
		{
			range.is_range_request = 1;

			if (range.end != 0 && 
				range.end - range.start < (off_t)conf->initial_requested_size)
			{
				fiber_count = DIV_CEIL(range.end - range.start, conf->min_chunk_size);
			}
		}
	}

	// allocate context
	ctx = ngx_pcalloc(r->pool, 
		sizeof(*ctx) + sizeof(ngx_http_parallel_fiber_ctx_t) * (fiber_count - 1));
	if (ctx == NULL)
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"ngx_http_parallel_handler: ngx_pcalloc failed");
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}
	ctx->error_code = NGX_AGAIN;
	ctx->chunk_size = conf->min_chunk_size;
	ctx->range = range;
	ctx->proxy_as_is = fiber_count == 1;
	
	ngx_http_set_ctx(r, ctx, ngx_http_parallel_module);
	
	// count the number of input headers
	header_in_count = ngx_http_parallel_list_get_count(&r->headers_in.headers);
	ctx->original_headers_in = r->headers_in;
	
	// build the subrequest uri
	ctx->sr_uri.data = ngx_pnalloc(r->pool, conf->uri_prefix.len + r->uri.len + 1);
	if (ctx->sr_uri.data == NULL)
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"ngx_http_parallel_handler: ngx_pnalloc failed");
		return NGX_HTTP_INTERNAL_SERVER_ERROR;
	}
	p = ngx_copy(ctx->sr_uri.data, conf->uri_prefix.data, conf->uri_prefix.len);
	p = ngx_copy(p, r->uri.data, r->uri.len);
	*p = '\0';
	ctx->sr_uri.len = p - ctx->sr_uri.data;

	// init and start the fibers
	for (i = 0; i < fiber_count; i++)
	{
		rc = ngx_http_parallel_init_fiber(
			r, header_in_count, ctx->proxy_as_is, &ctx->fibers[i]);
		if (rc != NGX_OK)
		{
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}

		rc = ngx_http_parallel_start_fiber(r, &ctx->fibers[i], i);
		if (rc != NGX_OK)
		{
			return NGX_HTTP_INTERNAL_SERVER_ERROR;
		}
	}
	
	ctx->next_request_chunk = fiber_count;
	
	return NGX_AGAIN;
}

static void *
ngx_http_parallel_create_loc_conf(ngx_conf_t *cf)
{
	ngx_http_parallel_loc_conf_t  *conf;
	
	conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_parallel_loc_conf_t));
	if (conf == NULL)
	{
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, cf->log, 0,
			"ngx_http_parallel_create_loc_conf: ngx_pcalloc failed");
		return NGX_CONF_ERROR;
	}

	conf->fiber_count = NGX_CONF_UNSET_UINT;
	conf->min_chunk_size = NGX_CONF_UNSET_SIZE;
	conf->max_chunk_size = NGX_CONF_UNSET_SIZE;
	conf->max_headers_size = NGX_CONF_UNSET_SIZE;
	conf->max_buffer_count = NGX_CONF_UNSET_UINT;
	conf->consistency_check_etag = NGX_CONF_UNSET;
	conf->consistency_check_last_modified = NGX_CONF_UNSET;

	return conf;
}

static char *
ngx_http_parallel_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
	ngx_http_parallel_loc_conf_t *prev = parent;
	ngx_http_parallel_loc_conf_t *conf = child;

	ngx_conf_merge_str_value(conf->uri_prefix, prev->uri_prefix, "");
	ngx_conf_merge_uint_value(conf->fiber_count, prev->fiber_count, 8);
	ngx_conf_merge_size_value(conf->min_chunk_size, prev->min_chunk_size, 128 * 1024);
	ngx_conf_merge_size_value(conf->max_chunk_size, prev->max_chunk_size, 512 * 1024);
	ngx_conf_merge_size_value(conf->max_headers_size, prev->max_headers_size, 4 * 1024);
	ngx_conf_merge_uint_value(conf->max_buffer_count, prev->max_buffer_count, 16);
	ngx_conf_merge_value(conf->consistency_check_etag, prev->consistency_check_etag, 1);
	ngx_conf_merge_value(conf->consistency_check_last_modified, prev->consistency_check_last_modified, 1);

	conf->initial_requested_size = conf->min_chunk_size * conf->fiber_count;

	if (conf->min_chunk_size > conf->max_chunk_size)
	{
		ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
			"\"parallel_min_chunk_size\" %uz cannot be greater than "
			"\"parallel_max_chunk_size\" %uz",
			conf->min_chunk_size, conf->max_chunk_size);
		return NGX_CONF_ERROR;
	}

	if (conf->max_buffer_count < conf->fiber_count)
	{
		ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
			"\"parallel_max_buffer_count\" %ui cannot be less than "
			"\"parallel_fiber_count\" %ui",
			conf->max_buffer_count, conf->fiber_count);
		return NGX_CONF_ERROR;
	}

	return NGX_CONF_OK;
}


static char *
ngx_http_parallel(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_core_loc_conf_t *clcf;
	ngx_str_t        *field, *value;

	// set up handler
    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_parallel_handler;

	// save the uri prefix param
	field = &((ngx_http_parallel_loc_conf_t*)conf)->uri_prefix;

	if (field->data) {
		return "is duplicate";
	}

	value = cf->args->elts;

	*field = value[1];

    return NGX_CONF_OK;
}

static ngx_int_t
ngx_http_parallel_filter_init(ngx_conf_t *cf)
{
	ngx_http_next_body_filter = ngx_http_top_body_filter;
	ngx_http_top_body_filter = ngx_http_parallel_body_filter;

	return NGX_OK;
}
