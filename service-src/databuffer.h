#ifndef skynet_databuffer_h
#define skynet_databuffer_h

#include <stdlib.h>
#include <string.h>
#include <assert.h>

// 定义pool的长度
#define MESSAGEPOOL 1023

// 这个结构体是做什么用的呢
// 为什么还有next
struct message {
	char * buffer;
	int size;
	struct message * next;
};
// 这个结构体是做什么用的呢
// 
struct databuffer {
	// header是什么？？
	int header;
	-- 偏移量
	int offset;
	-- 当前大小
	int size;
	struct message * head;
	struct message * tail;
};

// message pool list，为什么需要一个pool list呢
struct messagepool_list {
	struct messagepool_list *next;
	// 这里定义pool的大小
	struct message pool[MESSAGEPOOL];
};
// message pool, 为什么需要一个pool呢
struct messagepool {
	struct messagepool_list * pool;
	struct message * freelist;
};

// use memset init struct 

static void 
messagepool_free(struct messagepool *pool) {
	// pool list
	struct messagepool_list *p = pool->pool;
	// p 不为null
	while(p) {
		// 把当前messagepool_list 赋值本地变量
		struct messagepool_list *tmp = p;
		// 获取下一个pool
		p=p->next;
		// 释放pool
		skynet_free(tmp);
	}
	pool->pool = NULL;
	pool->freelist = NULL;
}

// 这个方法是做什么用的。
// 为什么读取buff后，buff为空，就调用这个方法
static inline void
_return_message(struct databuffer *db, struct messagepool *mp) {
	// 对于这里的 指针还是不了解。  什么时候用*， 什么不用*
	// 我目前的理解：带*的是实际值，不带* 是 指针
	struct message *m = db->head;

	// m->next 是空，说了什么
	// 为什么必定db->tail == m 呢
	// 说明只有一个值，message。 之前有误解，把这个databuffer当成链表中的一个node。其实不是，这是一个链表结构
	if (m->next == NULL) {
		assert(db->tail == m);
		db->head = db->tail = NULL;
	} else {
		// 丢弃当前head
		db->head = m->next;
	}
	skynet_free(m->buffer);

	// 接入到free list中
	m->buffer = NULL;
	m->size = 0;
	m->next = mp->freelist;
	mp->freelist = m;
}

// 这里用*， 用说明是指针
// 这个是读数据， 读指定大小的数据

// 指定了databuffer和 messagepool, 
static void
databuffer_read(struct databuffer *db, struct messagepool *mp, char * buffer, int sz) {
	assert(db->size >= sz);
	db->size -= sz;
	for (;;) {
		// 头指针
		struct message *current = db->head;
		// 当前缓存数据大小
		int bsz = current->size - db->offset;
		// 当前数据 足够
		if (bsz > sz) {
			// current->buffer + db -> offset 是什么地址？看着像是buffer是message的初始地址 ，db -> offset 是偏移地址
			memcpy(buffer, current->buffer + db->offset, sz);
			// 所以这里加了
			db->offset += sz;
			return;
		}
		// 如果数据大小刚好相等
		if (bsz == sz) {
			// 把数据copy -> buffer？？？
			memcpy(buffer, current->buffer + db->offset, sz);
			db->offset = 0;
			// 回收数据？？
			_return_message(db, mp);
			return;
		} else {
			// 这里复制过了
			memcpy(buffer, current->buffer + db->offset, bsz);

			// db->header变了。之前的head放入freelist里了
			_return_message(db, mp);
			db->offset = 0;

			// 这里加bsz什么意思呢
			buffer+=bsz;
			// 这里是本地变量，做操作没意义呀。 哦哦，这是个循环函数。 那哪里有阻塞呢？？
			sz-=bsz;
		}
	}
}

// 应该是写数据
static void
databuffer_push(struct databuffer *db, struct messagepool *mp, void *data, int sz) {
	struct message * m;
	// 判断mp的freelist 是否有值
	if (mp->freelist) {
		// 获取一个空闲buffer
		m = mp->freelist;
		mp->freelist = m->next;
	} else {
		// 分配一个新的
		struct messagepool_list * mpl = skynet_malloc(sizeof(*mpl));
		// 这应该是数组的指针。 指向开始地址。 当然，也可以代表第一个message
		struct message * temp = mpl->pool;
		int i;
		// 初始化
		for (i=1;i<MESSAGEPOOL;i++) {
			temp[i].buffer = NULL;
			temp[i].size = 0;
			temp[i].next = &temp[i+1];
		}
		temp[MESSAGEPOOL-1].next = NULL;

		// impl 放到mp的pool链表里
		mpl->next = mp->pool;
		mp->pool = mpl;
		// 当前使用的message
		m = &temp[0];
		// mp没有空闲池子，所以创建
		mp->freelist = &temp[1];
	}		
	// m -> buffer 初始值
	m->buffer = data;
	// 大小
	m->size = sz;
	// 下一个为null
	m->next = NULL;
	// db->size 是什么。
	db->size += sz;
	// db没有数据
	if (db->head == NULL) {
		assert(db->tail == NULL);
		// 设置第一个值。 为什么要这么关联呢？？？？？
		// 这里有三个结构，共用一套数据。为什么？？，是为了做什么
		db->head = db->tail = m;
	} else {
		// 加入队列
		db->tail->next = m;
		db->tail = m;
	}
}


// 什么是读header呢？？
// 这个应该是先读取头几个字符。因为要知道整个数据的长度，再进行全部的读取
static int
databuffer_readheader(struct databuffer *db, struct messagepool *mp, int header_size) {
	// header === 0 说明什么？？？
	if (db->header == 0) {
		// parser header (2 or 4)
		if (db->size < header_size) {
			return -1;
		}
		// 创建了一个池子
		uint8_t plen[4];
		// 把数据读到plen 中
		databuffer_read(db,mp,(char *)plen,header_size);
		// big-endian
		if (header_size == 2) {
			db->header = plen[0] << 8 | plen[1];
		} else {
			db->header = plen[0] << 24 | plen[1] << 16 | plen[2] << 8 | plen[3];
		}
	}
	// 如果db->size 小于 db->header，那么返回 -1？？。 说明收到的数据不全？？
	if (db->size < db->header)
		return -1;
	// 返回整个消息长度	
	return db->header;
}

static inline void
databuffer_reset(struct databuffer *db) {
	db->header = 0;
}

static void
databuffer_clear(struct databuffer *db, struct messagepool *mp) {
	while (db->head) {
		_return_message(db,mp);
	}
	memset(db, 0, sizeof(*db));
}

#endif
