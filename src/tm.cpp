/**
 * @file   tm.c
 * @author [...]
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of your own transaction manager.
 * You can completely rewrite this file (and create more files) as you wish.
 * Only the interface (i.e. exported symbols and semantic) must be preserved.
**/

// Requested features
#define _GNU_SOURCE
#define _POSIX_C_SOURCE   200809L
#ifdef __STDC_NO_ATOMICS__
    #error Current C11 compiler does not support atomic operations
#endif

// External headers

// Internal headers
#include <tm.hpp>
//#include <tm.hpp>
#include <iostream>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <atomic>
#include <unordered_map>
using namespace std;
// -------------------------------------------------------------------------- //

/** Define a proposition as likely true.
 * @param prop Proposition
**/
#undef likely
#ifdef __GNUC__
    #define likely(prop) \
        __builtin_expect((prop) ? 1 : 0, 1)
#else
    #define likely(prop) \
        (prop)
#endif

/** Define a proposition as likely false.
 * @param prop Proposition
**/
#undef unlikely
#ifdef __GNUC__
    #define unlikely(prop) \
        __builtin_expect((prop) ? 1 : 0, 0)
#else
    #define unlikely(prop) \
        (prop)
#endif

/** Define one or several attributes.
 * @param type... Attribute names
**/
#undef as
#ifdef __GNUC__
    #define as(type...) \
        __attribute__((type))
#else
    #define as(type...)
    #warning This compiler has no support for GCC attributes
#endif



struct link {
    struct link* prev; // Previous link in the chain
    struct link* next; // Next link in the chain
};

/** Link reset.
    * @param link Link to reset
**/
static void link_reset(struct link* link) {
    link->prev = link;
    link->next = link;
}

/** Link insertion before a "base" link.
    * @param link Link to insert
    * @param base Base link relative to which 'link' will be inserted
**/
static void link_insert(struct link* link, struct link* base) {
    struct link* prev = base->prev;
    link->prev = prev;
    link->next = base;
    base->prev = link;
    prev->next = link;
}

/** Link removal.
    * @param link Link to remove
**/
static void link_remove(struct link* link) {
    struct link* prev = link->prev;
    struct link* next = link->next;
    prev->next = next;
    next->prev = prev;
}

struct record {
    struct record* next; // Next link in the chain
    tx_t id;
    void* value;
};

static void record_insert(struct record* record, struct record* base) {
    record->next = base->next;
    base->next = record;
}

static record* record_remove(struct record* base) {
    record* tmp = base->next;
    base->next = tmp->next;
}


struct region {
    std::atomic_flag lock = ATOMIC_FLAG_INIT;
    std::atomic<tx_t> tx;
    std::atomic<tx_t> write;
    void* start;        // Start of the shared memory region
    struct link allocs; // Allocated shared memory regions
    size_t size;        // Size of the shared memory region (in bytes)
    size_t align;       // Claimed alignment of the shared memory region (in bytes)
    size_t align_alloc; // Actual alignment of the memory allocations (in bytes)
    size_t delta_alloc; // Space to add at the beginning of the segment for the link chain (in bytes)
    std::unordered_map<void*, record*> map;

};

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size as(unused), size_t align as(unused)) noexcept {
    cout << "create a shared memory" << endl;
    struct region* region = (struct region*)malloc(sizeof(struct region));
    (region->tx).store(1);
    (region->write).store(0);
    
    if (unlikely(!region)) {
        return invalid_shared;
    }
    size_t align_alloc = align < sizeof(void*) ? sizeof(void*) : align; // Also satisfy alignment requirement of 'struct link'
    if (unlikely(posix_memalign(&(region->start), align_alloc, size) != 0)) {
        free(region);
        return invalid_shared;
    }
    
    memset(region->start, 0, size);
    link_reset(&(region->allocs));
    region->size = size;
    region->align = align;
    region->align_alloc = align_alloc;
    region->delta_alloc = (sizeof(struct link) + align_alloc - 1) / align_alloc * align_alloc;
    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared as(unused)) noexcept {
    struct region* region = (struct region*)shared;
    struct link* allocs = &(region->allocs);
    while (true) { // Free allocated segments
        struct link* alloc = allocs->next;
        if (alloc == allocs)
            break;
        link_remove(alloc);
        free(alloc);
    }
    free(region->start);
    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared as(unused)) noexcept {
    return ((struct region*)shared)->start;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared as(unused)) noexcept {
    return ((struct region*)shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared as(unused)) noexcept {
    return ((struct region*)shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared as(unused), bool is_ro as(unused)) noexcept {
    region* p_r = ((struct region*)shared);
    if (is_ro) {
        if ((p_r->lock).test_and_set(std::memory_order_acquire)) { //get the lock
            (p_r->lock).clear(std::memory_order_release);
            cout << p_r->tx << endl;
            cout << "ro" << endl;
            return (p_r->tx).fetch_add(1);
        }
        else {
            cout << "invalid tx" << endl;
            return invalid_tx;
        }
        
    }else {
        if ((p_r->lock).test_and_set(std::memory_order_acquire)) {
            (p_r->write).store(p_r->tx);
            cout << p_r->tx << endl;
            cout << "rw" << endl;
            return (p_r->tx).fetch_add(1);
        }
        else {
            cout << "invalid tx" << endl;
            return invalid_tx;
        }
        
    }
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared as(unused), tx_t tx as(unused)) noexcept {
    region* p_r = ((struct region*)shared);
    if (tx == (p_r->write).load()) {
        (p_r->lock).clear(std::memory_order_release);
    }
    return true;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared as(unused), tx_t tx as(unused), void const* source as(unused), size_t size as(unused), void* target as(unused)) noexcept {
    region* p_r = ((struct region*)shared);
    void* dest = malloc(sizeof(void const*));
    memcpy(&dest, &source, sizeof(void const*));
    if ((p_r->map).count(dest) == 0) {//don't have old value
        memcpy(target, source, size);
    }
    else {
        record* pr;
        pr = (p_r->map).at(dest);
        pr = pr->next;
        while (true) {
            if (tx > pr->id) {
                memcpy(target, pr->value, size);
                break;
            }
            else {
                pr = pr->next;
            }
        }
    }
    return true;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared as(unused), tx_t tx as(unused), void const* source as(unused), size_t size as(unused), void* target as(unused)) noexcept {
    region* p_r = ((struct region*)shared);
    if ((p_r->map).count(target) == 0) {//don't have old value, build
        record *pr;//head node pointer
        pr->next = NULL;
        record *p1;
        p1->id = 0;
        p1->value = malloc(size);
        memcpy(p1->value, target, size);
        record* p2;
        p2->id = tx;
        p2->value = malloc(size);
        memcpy(p2->value, source, size);
        (p_r->map).insert({ target, pr });
        record_insert(p1, pr);
        record_insert(p2, pr);
        memcpy(target, source, size);
    }else {
        record* r;
        r->id = tx;
        r->value = malloc(size);
        memcpy(r->value, source, size);
        record* pr;
        pr = (p_r->map).at(target);
        record_insert(r, pr);
        memcpy(target, source, size);
    }
    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
Alloc tm_alloc(shared_t shared as(unused), tx_t tx as(unused), size_t size as(unused), void** target as(unused)) noexcept {
    size_t align_alloc = ((struct region*)shared)->align_alloc;
    size_t delta_alloc = ((struct region*)shared)->delta_alloc;
    void* segment;
    if (unlikely(posix_memalign(&segment, align_alloc, delta_alloc + size) != 0)) // Allocation failed
        return Alloc::nomem;
    link_insert((struct link*)segment, &(((struct region*)shared)->allocs));
    segment = (void*)((uintptr_t)segment + delta_alloc);
    memset(segment, 0, size);
    *target = segment;
    return Alloc::success;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx as(unused), void* segment) noexcept {
    size_t delta_alloc = ((struct region*)shared)->delta_alloc;
    segment = (void*)((uintptr_t)segment - delta_alloc);
    link_remove((struct link*)segment);
    free(segment);
    return true;
}
