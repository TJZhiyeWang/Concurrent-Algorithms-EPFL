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
#include <tm.h>

#include "macros.h"
#include <pthread.h>
#include <unistd.h>
#include "lock.h"




static const tx_t read_only_tx = UINTPTR_MAX - 10;
static const tx_t read_write_tx = UINTPTR_MAX - 11;


/**
 * @brief List of dynamically allocated segments.
 */
struct segment_node {
    struct segment_node* prev;
    struct segment_node* next;
    struct control* start;
    void* readable;
    void* writable;
    bool free;
    size_t size;
};
typedef struct segment_node* segment_list;

struct control
{
    struct lock_t lock;
    size_t epoch;
    bool write;
    long unsigned int access_set;
};

struct remain
{
    struct lock_t lock;
    size_t remaining;
};



/**
 * @brief Simple Shared Memory Region (a.k.a Transactional Memory).
 */
struct region {
    struct lock_t lock;
    struct control* start;        // Start of the control block
    void* readable;
    void* writable;
    segment_list allocs; // Shared memory segments dynamically allocated via tm_alloc within transactions
    size_t size;        // Size of the non-deallocable memory segment (in bytes)
    size_t align;       // Size of a word in the shared memory region (in bytes)
    size_t counter;     //epoch
    size_t blocking; //count blocking number
    struct remain remaining;
};





/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t unused(size), size_t unused(align)) {
    
    struct region* region = (struct region*) malloc(sizeof(struct region));
    if (unlikely(!region)) {
        return invalid_shared;
    }

    int num = size / align;
    
    if (posix_memalign(&(region->start), sizeof(struct control), sizeof(struct control) * num) != 0) {
        free(region);
        return invalid_shared;
    }

    if (posix_memalign(&(region->readable), align, size) != 0) {
        free(region->start);
        free(region);
        return invalid_shared;
    }

    if (posix_memalign(&(region->writable), align, size) != 0) {
        free(region->readable);
        free(region->start);
        free(region);
        return invalid_shared;
    }

    if (!lock_init(&(region->lock))) {
        free(region->readable);
        free(region->writable);
        free(region->start);
        free(region);
        return invalid_shared;
    }

    if (!lock_init(&((region->remaining).lock))) {
        free(region->readable);
        free(region->writable);
        free(region->start);
        free(region);
        lock_cleanup(&(region->lock));
        return invalid_shared;
    }

    (region->remaining).remaining = 0;

    memset(region->readable, 0, size);
    memset(region->writable, 0, size);

    for(int i=0; i<num; i++){
        struct control* cl = (region->start + i);
        cl->write = false;
        cl->access_set = 0;
        cl->epoch = 0;
        lock_init(&(cl->lock));
    }//init control block

    region->allocs      = NULL;
    region->size        = size;
    region->align       = align;
    region->counter     = 1;
    region->blocking    = 0;
    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t unused(shared)) {
    struct region* region = (struct region*) shared;
    while (region->allocs) { // Free allocated segments
        segment_list tail = region->allocs->next;
        free(region->allocs);
        region->allocs = tail;
    }
    free(region->start);
    free(region->readable);
    free(region->writable);
    lock_cleanup(&(region->lock));
    lock_cleanup(&((region->remaining).lock));
    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t unused(shared)) {
    return ((struct region*) shared)->start;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t unused(shared)) {
    return ((struct region*) shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t unused(shared)) {
    return ((struct region*) shared)->align;
}

void enter(shared_t unused(shared)){
    struct region* region = (struct region*) shared;
    lock_acquire(&((region->remaining).lock));

    if ((region->remaining).remaining==0){
        (region->remaining).remaining = 1;
    }else{
        lock_acquire(&(region->lock));
        lock_wait(&(region->lock));//process wait
        lock_release(&(region->lock));
        (region->blocking)++;
    }
    lock_release(&((region->remaining).lock));
    return;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t unused(shared), bool unused(is_ro)) {
    enter(shared);
    if (is_ro) {
        return read_only_tx;
    } else {
        return read_write_tx;
    }
}

size_t get_epoch(shared_t unused(shared)){
    struct region* region = (struct region*) shared;
    return region->counter;
}

void leave(shared_t unused(shared)){
    struct region* region = (struct region*) shared;
    lock_acquire(&((region->remaining).lock));
    (region->remaining).remaining--;
    if ((region->remaining).remaining == 0){
        (region->counter)++;
        (region->remaining).remaining = region->blocking;
        region->blocking = 0;
        commit(shared);
        lock_wake_up(&(region->lock));
    }
    lock_release(&((region->remaining).lock));
}

//todo copy all segments
void commit(shared_t unused(shared)){
    struct region* region = (struct region*) shared;
    memcpy(region->readable, region->writable, region->size);
    int num = region->size/region->align;
    for(int i=0; i<num; i++){
        struct control* cl = (region->start + i);
        cl->write = false;
        cl->access_set = 0;
        cl->epoch = 0;
        lock_init(&(cl->lock));
    }//init control block
    struct segment_node* node = region->allocs;
    while(node){
        if (node->free){
            if (node->prev) node->prev->next = node->next;
            else region->allocs = node->next;
            if (node->next) node->next->prev = node->prev;
            struct segment_node* tmp = node;
            node = node->next;
            free(tmp);
        }else{
            memcpy(node->readable, node->writable, node->size);
            int num = node->size/region->align;
            for(int i=0; i<num; i++){
                struct control* cl = (node->start + i);
                cl->write = false;
                cl->access_set = 0;
                cl->epoch = 0;
                lock_init(&(cl->lock));
            }//init control block
            node = node->next;
        }
    }
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t unused(shared), tx_t unused(tx)) {
    // TODO: tm_end(shared_t, tx_t)
    return false;
}

bool read_word(shared_t unused(shared), tx_t unused(tx), int index, struct segment_node* node, void* unused(target), size_t size){
    struct region* region = (struct region*) shared;
    if (tx == read_only_tx){
        void* source = (void*)(node->readable + (region->align * index));
        memcpy(target, source, size);
        return true;
    }else{
        struct control* ct = (struct control*)((node->start)+index);
        lock_acquire(&(ct->lock));
        if (ct->epoch == get_epoch(shared)){
            if (ct->access_set == pthread_self()){
                void* source = (void*)(node->writable + (region->align * index));
                memcpy(target, source, size);
                lock_release(&(ct->lock));
                return true;
            }else{
                lock_release(&(ct->lock));
                return false;
            }
        }
        else{
            void* source = (void*)(node->readable + (region->align * index));
            memcpy(target, source, size);
            ct->access_set = pthread_self();
            lock_release(&(ct->lock));
            return true;
        }
    }
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t unused(shared), tx_t unused(tx), void const* unused(source), size_t unused(size), void* unused(target)) {
    //find the source block
    struct region* region = (struct region*) shared;
    struct segment_node* node = region->allocs;
    while(node){
        int num = (node->size)/(region->align);
        int index = (((char*)source) - (char*)(node->start))/(region->align);
        if (index >=0 && index < num){
            return read_word(shared, tx, index, node, target, size);
        }else{
            node = node->next;
        }
    }
    return false;
}

bool write_word(shared_t unused(shared), tx_t unused(tx), int index, struct segment_node* node, void const* unused(source), size_t size){
    struct region* region = (struct region*) shared;
    struct control* ct = (struct control*)((node->start)+index);
    lock_acquire(&(ct->lock));
    if (ct->epoch == get_epoch(shared)){
        if (ct->access_set == pthread_self()){
            void* target = (void*)(node->writable + (region->align * index));
            memcpy(target, source, size);
            lock_release(&(ct->lock));
            return true;
        }else{
            lock_release(&(ct->lock));
            return false;
        }
    }else{
        if (ct->access_set != 0){
            lock_release(&(ct->lock));
            return false;
        }else{
            void* target = (void*)(node->writable + (region->align * index));
            memcpy(target, source, size);
            ct->access_set = pthread_self();
            ct->epoch = get_epoch(shared);//set flag has been written
            lock_release(&(ct->lock));
            return true;
        }
    }
}


/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t unused(shared), tx_t unused(tx), void const* unused(source), size_t unused(size), void* unused(target)) {
    struct region* region = (struct region*) shared;
    struct segment_node* node = region->allocs;
    while(node){
        int num = (node->size)/(region->align);
        int index = (((char*)target) - (char*)(node->start))/(region->align);
        if (index >=0 && index < num){
            return write_word(shared, tx, index, node, source, size);
        }else{
            node = node->next;
        }
    }    
    return false;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t unused(shared), tx_t unused(tx), size_t unused(size), void** unused(target)) {
    size_t align = ((struct region*) shared)->align;
    align = align < sizeof(struct segment_node*) ? sizeof(void*) : align;

    struct segment_node* sn = (struct segment_node*) malloc(sizeof(struct segment_node));
    int num = size / align;

    if (unlikely(posix_memalign(sn->start, sizeof(struct control), num*sizeof(struct control)) != 0)){
        free(sn);
        return nomem_alloc;
    }

    if (unlikely(posix_memalign(sn->readable, align, size) != 0)){
        free(sn->start);
        free(sn);
        return nomem_alloc;
    }

    if (unlikely(posix_memalign(sn->writable, align, size) != 0)){
        free(sn->start);
        free(sn->readable);
        free(sn);
        return nomem_alloc;
    }

    for(int i=0; i<num; i++){
        struct control* cl = (sn->start + i);
        cl->write = false;
        cl->access_set = 0;
        cl->epoch = 0;
        lock_init(&(cl->lock));
    }//init control block

    memset(sn->readable, 0, size);
    memset(sn->writable, 0, size);

    sn->free = false;
    sn->size = size;

    // Insert in the linked list
    sn->prev = NULL;
    sn->next = ((struct region*) shared)->allocs;
    if (sn->next) sn->next->prev = sn;
    ((struct region*) shared)->allocs = sn;
    
    *target = sn->start;
    return success_alloc;

}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t unused(shared), tx_t unused(tx), void* unused(target)) {
    // find the segment and set a free flag
    struct region* region = (struct region*) shared;
    struct segment_node* node = region->allocs;
    while(node){
        if (node->start == target){
            node->free = true;
            return true;
        }else{
            node = node->next;
        }
    }
    return false;
}
