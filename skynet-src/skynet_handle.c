#include "skynet.h"

#include "skynet_handle.h"
#include "skynet_server.h"
#include "rwlock.h"

#include <stdlib.h>
#include <assert.h>
#include <string.h>

#define DEFAULT_SLOT_SIZE 4
#define MAX_SLOT_SIZE 0x40000000

// 这个结构用于记录，服务对应的别名，当应用层为某个服务命名时，会写到这里来
struct handle_name {
	char * name;        // 服务别名
	uint32_t handle;    // 服务id
};

struct handle_storage {
	struct rwlock lock;                 // 读写锁
	// 应该是这个节点的id。高8位
	uint32_t harbor;                    // harbor id
	uint32_t handle_index;              // 创建下一个服务时，该服务的slot idx，一般会先判断该slot是否被占用，后面会详细讨论
	int slot_size;                      // slot的大小，一定是2^n，初始值是4
	// 是一个数组
	struct skynet_context ** slot;      // skynet_context list
	
	int name_cap;                       // 别名列表大小/容量，大小为2^n
	int name_count;                     // 别名数量
	struct handle_name *name;           // 别名列表
};

static struct handle_storage *H = NULL;

// 
uint32_t
skynet_handle_register(struct skynet_context *ctx) {
	// 
	struct handle_storage *s = H;
	// 读写锁的写锁
	rwlock_wlock(&s->lock);
	
	for (;;) {
		int i;
		uint32_t handle = s->handle_index;
		// slot_size 初始值是4
		for (i = 0; i < ( s->slot_size); i ++, handle ++) {
			// 如果超过最大值，从0开始
			if (handle > HANDLE_MASK) {
				// 0 is reserved
				handle = 1;
			}
			// slot_size 一定是2^n，可以知道（slot_size - 1） 的二进制。所有位都是1
			// 避免最大值。确保小于slot_size
			int hash = handle & (s->slot_size-1);
			if (s->slot[hash] == NULL) {
				// 赋值
				s->slot[hash] = ctx;
				s->handle_index = handle + 1;

				rwlock_wunlock(&s->lock);

				// 这个是或。harbor 是高8位的
				handle |= s->harbor;
				return handle;
			}
		}
		assert((s->slot_size*2 - 1) <= HANDLE_MASK);
		// 扩大一倍
		struct skynet_context ** new_slot = skynet_malloc(s->slot_size * 2 * sizeof(struct skynet_context *));
		// 初始化
		memset(new_slot, 0, s->slot_size * 2 * sizeof(struct skynet_context *));
		// 迁移
		for (i=0;i<s->slot_size;i++) {
			if (s->slot[i]) {
				int hash = skynet_context_handle(s->slot[i]) & (s->slot_size * 2 - 1);
				assert(new_slot[hash] == NULL);
				new_slot[hash] = s->slot[i];
			}
		}
		skynet_free(s->slot);
		s->slot = new_slot;
		s->slot_size *= 2;
	}
}

int
skynet_handle_retire(uint32_t handle) {
	int ret = 0;
	// s是一个全量结构，包含全局数据
	struct handle_storage *s = H;

	rwlock_wlock(&s->lock);

	uint32_t hash = handle & (s->slot_size-1);
	struct skynet_context * ctx = s->slot[hash];
	// 判断
	if (ctx != NULL && skynet_context_handle(ctx) == handle) {
		s->slot[hash] = NULL;
		ret = 1;
		int i;
		int j=0, n=s->name_count;
		for (i=0; i<n; ++i) {
			// 判断是否是当前handle的名字
			if (s->name[i].handle == handle) {
				// 如果是，那么就释放
				skynet_free(s->name[i].name);
				continue;
			} else if (i!=j) {
				// 如果不相等，说明，中途移除了。所以后面往前移动
				s->name[j] = s->name[i];
			}
			++j;
		}
		// 赋值最新的j
		s->name_count = j;
	} else {
		// handle不相等。说明什么
		ctx = NULL;
	}

	rwlock_wunlock(&s->lock);

	if (ctx) {
		// release ctx may call skynet_handle_* , so wunlock first.
		skynet_context_release(ctx);
	}

	return ret;
}

void 
skynet_handle_retireall() {
	struct handle_storage *s = H;
	for (;;) {
		int n=0;
		int i;
		for (i=0;i<s->slot_size;i++) {
			rwlock_rlock(&s->lock);
			struct skynet_context * ctx = s->slot[i];
			uint32_t handle = 0;
			if (ctx) {
				handle = skynet_context_handle(ctx);
				++n;
			}
			rwlock_runlock(&s->lock);
			if (handle != 0) {
				skynet_handle_retire(handle);
			}
		}
		if (n==0)
			return;
	}
}


// 此处的handle 是dst
struct skynet_context * 
skynet_handle_grab(uint32_t handle) {
	struct handle_storage *s = H;
	struct skynet_context * result = NULL;

	rwlock_rlock(&s->lock);

	uint32_t hash = handle & (s->slot_size-1);
	struct skynet_context * ctx = s->slot[hash];
	if (ctx && skynet_context_handle(ctx) == handle) {
		result = ctx;
		skynet_context_grab(result);
	}

	rwlock_runlock(&s->lock);

	return result;
}

uint32_t 
skynet_handle_findname(const char * name) {
	struct handle_storage *s = H;

	rwlock_rlock(&s->lock);

	uint32_t handle = 0;

	int begin = 0;
	int end = s->name_count - 1;
	while (begin<=end) {
		// 使用二分法，说明name是一个有序的结构
		int mid = (begin+end)/2;
		struct handle_name *n = &s->name[mid];
		int c = strcmp(n->name, name);
		if (c==0) {
			handle = n->handle;
			break;
		}
		if (c<0) {
			begin = mid + 1;
		} else {
			end = mid - 1;
		}
	}

	rwlock_runlock(&s->lock);

	return handle;
}

// name 中间插入
static void
_insert_name_before(struct handle_storage *s, char *name, uint32_t handle, int before) {
	// 判断容量是否足够
	if (s->name_count >= s->name_cap) {
		// 扩容？
		s->name_cap *= 2;
		assert(s->name_cap <= MAX_SLOT_SIZE);
		// 分配内存
		struct handle_name * n = skynet_malloc(s->name_cap * sizeof(struct handle_name));
		int i;
		// 迁移
		for (i=0;i<before;i++) {
			n[i] = s->name[i];
		}
		// 预留一位，并迁移后续的
		for (i=before;i<s->name_count;i++) {
			n[i+1] = s->name[i];
		}
		// 释放之前的
		skynet_free(s->name);
		s->name = n;
	} else {
		int i;
		后移
		for (i=s->name_count;i>before;i--) {
			s->name[i] = s->name[i-1];
		}
	}

	s->name[before].name = name;
	s->name[before].handle = handle;
	s->name_count ++;
}

// 尾部插入
static const char *
_insert_name(struct handle_storage *s, const char * name, uint32_t handle) {
	int begin = 0;
	int end = s->name_count - 1;
	// 判断是否已有
	while (begin<=end) {
		int mid = (begin+end)/2;
		struct handle_name *n = &s->name[mid];
		int c = strcmp(n->name, name);
		if (c==0) {
			return NULL;
		}
		if (c<0) {
			begin = mid + 1;
		} else {
			end = mid - 1;
		}
	}
	// 
	char * result = skynet_strdup(name);

	_insert_name_before(s, result, handle, begin);

	return result;
}

const char * 
skynet_handle_namehandle(uint32_t handle, const char *name) {
	rwlock_wlock(&H->lock);

	const char * ret = _insert_name(H, name, handle);

	rwlock_wunlock(&H->lock);

	return ret;
}

void 
skynet_handle_init(int harbor) {
	assert(H==NULL);
	// 分配内存
	struct handle_storage * s = skynet_malloc(sizeof(*H));
	// 初始大小
	s->slot_size = DEFAULT_SLOT_SIZE;
	// 分配内存
	s->slot = skynet_malloc(s->slot_size * sizeof(struct skynet_context *));
	// 初始化
	memset(s->slot, 0, s->slot_size * sizeof(struct skynet_context *));
	// 初始化锁
	rwlock_init(&s->lock);
	// reserve 0 for system
	s->harbor = (uint32_t) (harbor & 0xff) << HANDLE_REMOTE_SHIFT;
	s->handle_index = 1;
	s->name_cap = 2;
	s->name_count = 0;
	s->name = skynet_malloc(s->name_cap * sizeof(struct handle_name));

	H = s;

	// Don't need to free H
}

