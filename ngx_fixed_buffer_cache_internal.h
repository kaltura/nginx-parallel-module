#ifndef _ngx_fixed_buffer_cache_INTERNAL_H_INCLUDED_
#define _ngx_fixed_buffer_cache_INTERNAL_H_INCLUDED_

#include "ngx_fixed_buffer_cache.h"
#include "ngx_queue.h"

// macros
#ifndef container_of
#define container_of(ptr, type, member) (type *)((char *)(ptr) - offsetof(type, member))
#endif

// typedefs
typedef struct {
	ngx_rbtree_node_t node;
	ngx_queue_t queue_node;
	u_char key[NGX_FIXED_BUFFER_CACHE_KEY_SIZE];
} ngx_fixed_buffer_cache_entry_t;

typedef struct {
	ngx_atomic_t reset;
	ngx_rbtree_t rbtree;
	ngx_rbtree_node_t sentinel;
	ngx_queue_t used_queue;
	size_t buffer_size;
	u_char* entries_start;
	u_char* entries_pos;
	u_char* entries_end;
	ngx_fixed_buffer_cache_stats_t stats;
} ngx_fixed_buffer_cache_t;

#endif // _ngx_fixed_buffer_cache_INTERNAL_H_INCLUDED_
