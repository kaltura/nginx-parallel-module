#include "ngx_fixed_buffer_cache_internal.h"

#define ZONE_NAME_FORMAT "%uz-%V"

// Note: code taken from ngx_str_rbtree_insert_value, updated the node comparison
static void
ngx_fixed_buffer_cache_rbtree_insert_value(
	ngx_rbtree_node_t *temp, 
	ngx_rbtree_node_t *node, 
	ngx_rbtree_node_t *sentinel)
{
	ngx_fixed_buffer_cache_entry_t *n, *t;
	ngx_rbtree_node_t **p;

	for (;;) 
	{
		n = (ngx_fixed_buffer_cache_entry_t *)node;
		t = (ngx_fixed_buffer_cache_entry_t *)temp;

		if (node->key != temp->key) 
		{
			p = (node->key < temp->key) ? &temp->left : &temp->right;
		}
		else 
		{
			p = (ngx_memcmp(n->key, t->key, NGX_FIXED_BUFFER_CACHE_KEY_SIZE) < 0)
				? &temp->left : &temp->right;
		}

		if (*p == sentinel) 
		{
			break;
		}

		temp = *p;
	}

	*p = node;
	node->parent = temp;
	node->left = sentinel;
	node->right = sentinel;
	ngx_rbt_red(node);
}

// Note: code taken from ngx_str_rbtree_lookup, updated the node comparison
static ngx_fixed_buffer_cache_entry_t *
ngx_fixed_buffer_cache_rbtree_lookup(ngx_rbtree_t *rbtree, const u_char* key, uint32_t hash)
{
	ngx_fixed_buffer_cache_entry_t *n;
	ngx_rbtree_node_t *node, *sentinel;
	ngx_int_t rc;

	node = rbtree->root;
	sentinel = rbtree->sentinel;

	while (node != sentinel) 
	{
		n = (ngx_fixed_buffer_cache_entry_t *)node;

		if (hash != node->key) 
		{
			node = (hash < node->key) ? node->left : node->right;
			continue;
		}

		rc = ngx_memcmp(key, n->key, NGX_FIXED_BUFFER_CACHE_KEY_SIZE);
		if (rc < 0) 
		{
			node = node->left;
			continue;
		}

		if (rc > 0) 
		{
			node = node->right;
			continue;
		}

		return n;
	}

	return NULL;
}

static void
ngx_fixed_buffer_cache_reset(ngx_fixed_buffer_cache_t *cache)
{
	cache->entries_pos = cache->entries_start;
	ngx_rbtree_init(&cache->rbtree, &cache->sentinel, ngx_fixed_buffer_cache_rbtree_insert_value);
	ngx_queue_init(&cache->used_queue);

	// update stats (everything is evicted)
	cache->stats.evicted = cache->stats.store_ok;
}

static ngx_int_t
ngx_fixed_buffer_cache_init(ngx_shm_zone_t *shm_zone, void *data)
{
	ngx_fixed_buffer_cache_t *cache;
	ngx_slab_pool_t *shpool;
	ssize_t buffer_size;
	u_char* dash_pos;
	u_char* p;

	if (data) 
	{
		shm_zone->data = data;
		return NGX_OK;
	}

	shpool = (ngx_slab_pool_t *)shm_zone->shm.addr;

	if (shm_zone->shm.exists) 
	{
		shm_zone->data = shpool->data;
		return NGX_OK;
	}

	// start following the ngx_slab_pool_t that was allocated at the beginning of the chunk
	p = shm_zone->shm.addr + sizeof(ngx_slab_pool_t);

	// initialize the log context
	shpool->log_ctx = p;
	p = ngx_sprintf(shpool->log_ctx, " in buffer cache \"%V\"%Z", &shm_zone->shm.name);

	// allocate the cache state
	cache = (ngx_fixed_buffer_cache_t*)p;
	p += sizeof(*cache);

	// initialize fixed cache fields
	cache->entries_start = p;
	cache->entries_end = shm_zone->shm.addr + shm_zone->shm.size;
	
	// parse the buffer size from the shm name
	dash_pos = memchr(shm_zone->shm.name.data, '-', shm_zone->shm.name.len);
	if (dash_pos == NULL)
	{
		return NGX_ERROR;		// unexpected
	}

	buffer_size = ngx_atosz(shm_zone->shm.name.data, dash_pos - shm_zone->shm.name.data);
	if (buffer_size < 0)
	{
		return NGX_ERROR;		// unexpected
	}
	cache->buffer_size = buffer_size;

	// reset the stats
	ngx_memzero(&cache->stats, sizeof(cache->stats));

	// reset the cache status
	ngx_fixed_buffer_cache_reset(cache);
	cache->reset = 0;

	// set the cache struct as the data of the shared pool
	shpool->data = cache;

	return NGX_OK;
}

/* Note: must be called with the mutex locked */
static ngx_fixed_buffer_cache_entry_t*
ngx_fixed_buffer_cache_get_free_entry(ngx_fixed_buffer_cache_t *cache)
{
	ngx_fixed_buffer_cache_entry_t* entry;
	u_char* next_entries_pos;

	next_entries_pos = cache->entries_pos + sizeof(*entry) + cache->buffer_size;
	if (next_entries_pos <= cache->entries_end)
	{
		// enlarge the entries buffer
		entry = (ngx_fixed_buffer_cache_entry_t*)cache->entries_pos;
		cache->entries_pos = next_entries_pos;
		return entry;
	}
	
	// verify we have an entry to free
	if (ngx_queue_empty(&cache->used_queue))
	{
		return NULL;
	}

	// get oldest entry
	entry = container_of(ngx_queue_head(&cache->used_queue), ngx_fixed_buffer_cache_entry_t, queue_node);

	// remove from rb tree and used queue
	ngx_queue_remove(&entry->queue_node);
	ngx_rbtree_delete(&cache->rbtree, &entry->node);

	// update stats
	cache->stats.evicted++;

	return entry;
}

ngx_flag_t
ngx_fixed_buffer_cache_fetch(
	ngx_shm_zone_t *shm_zone,
	u_char* key,
	u_char* buffer)
{
	ngx_fixed_buffer_cache_entry_t* entry;
	ngx_fixed_buffer_cache_t *cache;
	ngx_slab_pool_t *shpool;
	ngx_flag_t result = 0;
	uint32_t hash;

	hash = ngx_crc32_short(key, NGX_FIXED_BUFFER_CACHE_KEY_SIZE);

	shpool = (ngx_slab_pool_t *)shm_zone->shm.addr;
	cache = shpool->data;

	ngx_shmtx_lock(&shpool->mutex);

	if (!cache->reset)
	{
		entry = ngx_fixed_buffer_cache_rbtree_lookup(&cache->rbtree, key, hash);
		if (entry != NULL)
		{
			result = 1;

			// copy buffer
			ngx_memcpy(buffer, entry + 1, cache->buffer_size);

			// update stats
			cache->stats.fetch_hit++;
		}
		else
		{
			// update stats
			cache->stats.fetch_miss++;
		}
	}

	ngx_shmtx_unlock(&shpool->mutex);

	return result;
}

ngx_flag_t
ngx_fixed_buffer_cache_store(
	ngx_shm_zone_t *shm_zone,
	u_char* key,
	u_char* buffer, 
	ngx_flag_t overwrite)
{
	ngx_fixed_buffer_cache_entry_t* entry;
	ngx_fixed_buffer_cache_t *cache;
	ngx_slab_pool_t *shpool;
	uint32_t hash;

	hash = ngx_crc32_short(key, NGX_FIXED_BUFFER_CACHE_KEY_SIZE);

	shpool = (ngx_slab_pool_t *)shm_zone->shm.addr;
	cache = shpool->data;

	ngx_shmtx_lock(&shpool->mutex);

	if (cache->reset)
	{
		// reset the cache, leave the reset flag enabled
		ngx_fixed_buffer_cache_reset(cache);

		// update stats
		cache->stats.reset++;
	}
	else
	{
		// check whether the entry already exists
		entry = ngx_fixed_buffer_cache_rbtree_lookup(&cache->rbtree, key, hash);
		if (entry != NULL)
		{
			if (overwrite)
			{
				cache->reset = 1;

				// push to the end of the used queue
				ngx_queue_remove(&entry->queue_node);
				ngx_queue_insert_tail(&cache->used_queue, &entry->queue_node);

				// update buffer
				memcpy(entry + 1, buffer, cache->buffer_size);

				cache->reset = 0;

				// update stats
				cache->stats.store_ok++;
			}
			else
			{
				// update stats
				cache->stats.store_exists++;
			}
			ngx_shmtx_unlock(&shpool->mutex);
			return overwrite;
		}

		// enable the reset flag before we start making any changes
		cache->reset = 1;
	}

	// allocate a new entry
	entry = ngx_fixed_buffer_cache_get_free_entry(cache);
	if (entry == NULL)
	{
		cache->reset = 0;
		cache->stats.store_err++;
		ngx_shmtx_unlock(&shpool->mutex);
		return 0;
	}

	// initialize the entry
	entry->node.key = hash;
	memcpy(entry->key, key, NGX_FIXED_BUFFER_CACHE_KEY_SIZE);

	// add to rbtree and used queue
	ngx_rbtree_insert(&cache->rbtree, &entry->node);
	ngx_queue_insert_tail(&cache->used_queue, &entry->queue_node);

	// copy the buffer
	memcpy(entry + 1, buffer, cache->buffer_size);

	cache->reset = 0;

	// update stats
	cache->stats.store_ok++;

	ngx_shmtx_unlock(&shpool->mutex);

	return 1;
}

void
ngx_fixed_buffer_cache_get_stats(
	ngx_shm_zone_t *shm_zone,
	ngx_fixed_buffer_cache_stats_t* stats)
{
	ngx_fixed_buffer_cache_t *cache;
	ngx_slab_pool_t *shpool;

	shpool = (ngx_slab_pool_t *)shm_zone->shm.addr;
	cache = shpool->data;

	ngx_shmtx_lock(&shpool->mutex);

	memcpy(stats, &cache->stats, sizeof(cache->stats));

	stats->entries = (cache->entries_pos - cache->entries_start) / (sizeof(ngx_fixed_buffer_cache_entry_t) + cache->buffer_size);

	ngx_shmtx_unlock(&shpool->mutex);
}

void
ngx_fixed_buffer_cache_reset_stats(ngx_shm_zone_t *shm_zone)
{
	ngx_fixed_buffer_cache_t *cache;
	ngx_slab_pool_t *shpool;

	shpool = (ngx_slab_pool_t *)shm_zone->shm.addr;
	cache = shpool->data;

	ngx_shmtx_lock(&shpool->mutex);

	ngx_memzero(&cache->stats, sizeof(cache->stats));

	ngx_shmtx_unlock(&shpool->mutex);
}

ngx_shm_zone_t* 
ngx_fixed_buffer_cache_create_zone(
	ngx_conf_t *cf, 
	ngx_str_t *name, 
	size_t size, 
	size_t buffer_size, 
	void *tag)
{
	ngx_shm_zone_t* result;
	ngx_str_t updated_name;

	// encode the buffer size on the zone name (no way to pass context to the init function)
	updated_name.data = ngx_pnalloc(cf->pool, sizeof(ZONE_NAME_FORMAT) + name->len + NGX_SIZE_T_LEN);
	if (updated_name.data == NULL)
	{
		return NULL;
	}
	updated_name.len = ngx_sprintf(updated_name.data, ZONE_NAME_FORMAT, buffer_size, name) - updated_name.data;

	// create the shared memory
	result = ngx_shared_memory_add(cf, &updated_name, size, tag);
	if (result == NULL)
	{
		return NULL;
	}

	result->init = ngx_fixed_buffer_cache_init;
	return result;
}
