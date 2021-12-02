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
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include "macros.h"
#include <pthread.h>
#include <unistd.h>
#include "lock.h"



/**
 * @brief List of dynamically allocated segments.
 */
struct segment_node {
    struct control* start;
    struct segment_node* prev;
    struct segment_node* next;
    void* readable;
    void* writable;
    bool free;
    size_t size;
    size_t num;
};
typedef struct segment_node* segment_list;

struct control
{
    struct lock_t lock;
    size_t epoch;
    long unsigned int access_set;
};

struct op_node {
    bool write;
    struct control* ptr_control;
    void* read_address;
    void* write_address;
    int size;
};
typedef struct op_node* op_list;

struct tx {
    struct tx* next;
    bool read_only;
    op_list op;
    int max_op; //initial with 8 and expand with *2
    int cur_op; //current operation number
    bool success;
    bool free;
};
typedef struct tx* tx_list;


/**
 * @brief Simple Shared Memory Region (a.k.a Transactional Memory).
 */
struct region {
    struct lock_t lock;
    struct control* start;        // Start of the control block
    void* readable;            // read version
    void* writable;         // write version(dual version)
    tx_list txlist;
    segment_list allocs; // Shared memory segments dynamically allocated via tm_alloc within transactions
    size_t size;        // Size of the non-deallocable memory segment (in bytes)
    size_t align;       // Size of a word in the shared memory region (in bytes)
    size_t num;         // account number
    size_t counter;     //epoch number
    size_t blocking; //thread blocking number
    size_t remaining; //threads that are still execute
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

    region->start = malloc(sizeof(struct control)*num);
    if (region->start == NULL){
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



    memset(region->readable, 0, size);
    memset(region->writable, 0, size);

    for(int i=0; i<num; i++){
        struct control* cl = (region->start + i);
        cl->access_set = 0;
        cl->epoch = 0;
        lock_init(&(cl->lock));
    }//init control block

    region->txlist      = NULL;
    region->allocs      = NULL;
    region->size        = size;
    region->align       = align;
    region->counter     = 1;
    region->blocking    = 0;
    region->remaining   = 0;
    region->num         = size/align;
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
    while (region->txlist) { // Free allocated segments
        tx_list tail = region->txlist->next;
        free(region->txlist);
        region->txlist = tail;
    }

    for(size_t i=0; i<region->num; i++){
        struct control* cl = (region->start + i);
        lock_cleanup(&(cl->lock));
    }//init control block

    free(region->start);
    free(region->readable);
    free(region->writable);
    lock_cleanup(&(region->lock));
    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t unused(shared)) {
    return ((struct region*) shared)->readable;
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
    lock_acquire(&(region->lock));
    // printf("remaining: %d\n", (region->remaining).remaining);
    if (region->remaining==0){
        region->remaining = 1;
        lock_release(&(region->lock));
    }else{
        (region->blocking)++;
        // printf("blocking:%d\n",region->blocking);
        lock_wait(&(region->lock));//process wait
        lock_release(&(region->lock));
    }
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
        struct tx* transaction = (struct tx*)malloc(sizeof(struct tx));
        if (transaction == NULL)
            return invalid_tx;
        transaction->read_only = true;
        transaction->op = NULL;
        transaction->next = NULL;
        transaction->max_op = 16;
        transaction->cur_op = 0;
        transaction->success = true;
        transaction->free = false;
        return (tx_t)transaction;
    } else {
        struct tx* transaction = (struct tx*)malloc(sizeof(struct tx));
        if (transaction == NULL)
            return invalid_tx;
        transaction->read_only = false;
        transaction->max_op = 8;
        transaction->cur_op = 0;
        transaction->op = (struct op_node*)malloc(sizeof(struct op_node) * transaction->max_op);
        transaction->next = NULL;
        transaction->success = true;
        transaction->free = false;
        return (tx_t)transaction;
    }
}

void add_op(tx_t tx, bool write, struct control* c_address, void* w_address, void* r_address, int size){
    struct tx* transaction = (struct tx*) tx;
    if (transaction->cur_op == transaction->max_op){
        //expand
        struct op_node* tmp = (struct op_node*)malloc(sizeof(struct op_node) * transaction->max_op * 2);
        memcpy(tmp, transaction->op, transaction->cur_op * sizeof(struct op_node));
        free(transaction->op);
        transaction->op = tmp;
        transaction->max_op = transaction->max_op * 2;
    }
    struct op_node* ptr = (struct op_node*) (transaction->op + transaction->cur_op);
    ptr->ptr_control = c_address;
    ptr->read_address = r_address;
    ptr->write_address = w_address;
    ptr->write = write;
    ptr->size = size;
    transaction->cur_op++;
}

size_t get_epoch(shared_t unused(shared)){
    struct region* region = (struct region*) shared;
    return region->counter;
}


void commit(shared_t unused(shared), tx_t unused(tx)){
    struct region* region = (struct region*) shared;
    struct tx* transaction = (struct tx*) tx;
    struct tx* tx_list = region->txlist;
    while(tx_list){
        struct tx* transaction = tx_list;
        if (transaction->success){
            for (int i=0; i<transaction->cur_op; i++){
                struct op_node* node = transaction->op + i;
                node->ptr_control->access_set = 0;
                if (node->write)
                    memcpy(node->read_address, node->write_address, node->size);
            }
        }else{
            for (int i=0; i<transaction->cur_op; i++){
                struct op_node* node = transaction->op + i;
                node->ptr_control->access_set = 0;
                if (node->write)
                    memcpy(node->write_address, node->read_address, node->size);
            }
        }
        //remove node
        struct tx* tmp = tx_list;
        tx_list = tx_list->next;
        free(tmp->op);
        free(tmp);
    }
    region->txlist = NULL;
    if (transaction->free){
        struct segment_node* node = region->allocs;
        while(node){
            if (node->free){
                if (node->prev) node->prev->next = node->next;
                else region->allocs = node->next;
                if (node->next) node->next->prev = node->prev;
                struct segment_node* tmp = node;
                node = node->next;
                for(size_t i=0; i<tmp->num; i++){
                    struct control* cl = (node->start + i);
                    lock_cleanup(&(cl->lock));
                }
                free(tmp->start);
                free(tmp->readable);
                free(tmp->writable);
                free(tmp);
                continue;
            }
            node = node->next;
        }
    }
}

void leave(shared_t unused(shared), tx_t unused(tx)){
    struct region* region = (struct region*) shared;
    struct tx* transaction = (struct tx*) tx;

    lock_acquire(&(region->lock));
    region->remaining--;
    //add transaction to region's txlist
    if (transaction->cur_op == 0){
        free(transaction->op);
        free(transaction);
    }else{
        transaction->next = region->txlist;
    // if (transaction->next) transaction->next->prev = transaction;
        region->txlist = transaction;
    }
    

    // printf("remaining:%d\n",region->remaining);
    if (region->remaining == 0){
        // printf("blocking:%d\n",region->blocking);
        (region->counter)++;
        region->remaining = region->blocking;
        region->blocking = 0;
        commit(shared, tx);
        lock_wake_up(&(region->lock));
    }
    lock_release(&(region->lock));
}


/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t unused(shared), tx_t unused(tx)) {
    leave(shared, tx);
    return true;
}

bool read_word(shared_t unused(shared), tx_t unused(tx), int index, struct control* start, void* readable, void* writable, void* unused(target), size_t size){
    struct region* region = (struct region*) shared;
    struct tx* transaction = (struct tx*) tx;
    if (transaction->read_only){
        void* source = readable + (region->align * index);
        memcpy(target, source, size);
        return true;
    }else{
        struct control* ct = (struct control*)((start)+index);
        lock_acquire(&(ct->lock));
    
        if (ct->epoch == get_epoch(shared)){
            if (ct->access_set == pthread_self()){
                void* source = (void*)(writable + (region->align * index));
                memcpy(target, source, size);
                lock_release(&(ct->lock));
                add_op(tx, false, ct, NULL, NULL, 0);
                return true;
            }else{
                lock_release(&(ct->lock));
                transaction->success = false;
                return false;
            }
        }
        else{
            void* source = (void*)(readable + (region->align * index));
            memcpy(target, source, size);
            if (ct->access_set == 0)
                ct->access_set = pthread_self();
            
            lock_release(&(ct->lock));
            add_op(tx, false, ct, NULL, NULL, 0);
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
    struct tx* transaction = (struct tx*) tx;

    int num = region->num;
    int index = (((char*)source) - (char*)(region->readable))/(region->align);
    // printf("read_index:%d\n", index);
    if (index >=0 && index < num){
        struct segment_node* node = (struct segment_node*)malloc(sizeof(struct segment_node));
        node->readable = region->readable;
        node->start = region->start;
        node->writable = region->writable;
        node->num = region->num;
        bool res = read_word(shared, tx, index, region->start, region->readable, region->writable, target, size);
        // printf("res:%d\n", *(int*)target);
        free(node);
        if (res){
            return res;
        }else{
            leave(shared, tx);
            return res;
        }
    }
    struct segment_node* node = region->allocs;
    while(node){
        int num = node->num;
        int index = ((char*)source - (char*)(node->readable))/(region->align);
        // printf("%lu read_index:%d\n",pthread_self(), index);
        // printf("num:%d\n", num);
        // printf("read_only: %d\n", transaction->read_only);
        if (index >=0 && index < num){
            bool res = read_word(shared, tx, index, node->start, node->readable, node->writable, target, size);
            if (res){
                return res;
            }else{
                leave(shared, tx);
                return res;
            }
        }else{
            node = node->next;
        }
    }
    transaction->success = false;
    leave(shared, tx);
    return false;
}

bool write_word(shared_t unused(shared), tx_t unused(tx), int index, struct control* start, void* readable, void* writable, void const* unused(source), size_t size){
    struct region* region = (struct region*) shared;
    struct control* ct = (struct control*)((start)+index);
    struct tx* transaction = (struct tx*) tx;

    lock_acquire(&(ct->lock));
    
    if (ct->epoch == get_epoch(shared)){
        if (ct->access_set == pthread_self()){
            void* target = (void*)(writable + (region->align * index));
            memcpy(target, source, size);
            lock_release(&(ct->lock));
            add_op(tx, true, ct, target, (void*)(readable + (region->align * index)), size);
            return true;
        }else{
            transaction->success = false;
            lock_release(&(ct->lock));
            return false;
        }
    }else{
        if (ct->access_set != 0 && ct->access_set != pthread_self()){
            transaction->success = false;
            lock_release(&(ct->lock));
            return false;
        }else{
            void* target = (void*)(writable + (region->align * index));
            memcpy(target, source, size);
            ct->access_set = pthread_self();
            ct->epoch = get_epoch(shared);//set flag has been written
            lock_release(&(ct->lock));
            add_op(tx, true, ct, target, (void*)(readable + (region->align * index)), size);
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
    struct tx* transaction = (struct tx*) tx;
    int num = region->num;
    int index = (((char*)target) - (char*)(region->readable))/(region->align);
    // printf("write_index%lu:%d\n", pthread_self(),index);
    // printf("epoch_write:%d\n", get_epoch(shared));
    // printf("write_index:%d\n", index);

    if (index >=0 && index < num){
        bool res = write_word(shared, tx, index, region->start, region->readable, region->writable, source, size);
        // printf("index: %d, res: %d\n",index, res);
        if (res){
            return res;
        }else{
            leave(shared, tx);
            return res;
        }
    }

    struct segment_node* node = region->allocs;
    while(node){
        int num = node->num;
        int index = ((char*)target - (char*)(node->readable))/(region->align);
        // printf("write_index:%d\n", index);
        if (index >=0 && index < num){
            bool res = write_word(shared, tx, index, node->start, node->readable, node->writable, source, size);
            if (res){
                return res;
            }else{
                leave(shared, tx);
                return res;
            }
        }else{
            node = node->next;
        }
    }
    transaction->success = false;
    leave(shared, tx);
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
    sn->num = num;
    sn->start = malloc(sizeof(struct control)*num);
    if (sn->start == NULL){
        free(sn);
        leave(shared, tx);
        return nomem_alloc;
    }

    if (unlikely(posix_memalign(&(sn->readable), align, size) != 0)){
        free(sn->start);
        free(sn);
        leave(shared, tx);
        return nomem_alloc;
    }


    if (unlikely(posix_memalign(&(sn->writable), align, size) != 0)){
        free(sn->start);
        free(sn->readable);
        free(sn);
        leave(shared, tx);
        return nomem_alloc;
    }


    for(int i=0; i<num; i++){
        struct control* cl = (sn->start + i);
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

    *target = sn->readable;
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
    struct tx* transaction = (struct tx*) tx;
    while(node){
        if (node->readable == target){
            node->free = true;
            transaction->free = true;
            return true;
        }else{
            node = node->next;
        }
    }
    leave(shared, tx);
    return false;
}
