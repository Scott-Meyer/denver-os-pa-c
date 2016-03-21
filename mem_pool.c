/*
 * Created by Ivo Georgiev on 2/9/16.
 */

#include <stdlib.h>
#include <assert.h>
#include <stdio.h> // for perror()

#include "mem_pool.h"

/*************/
/*           */
/* Constants */
/*           */
/*************/
static const float      MEM_FILL_FACTOR                 = 0.75;
static const unsigned   MEM_EXPAND_FACTOR               = 2;

static const unsigned   MEM_POOL_STORE_INIT_CAPACITY    = 20;
static const float      MEM_POOL_STORE_FILL_FACTOR      = 0.75;
static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR    = 2;

static const unsigned   MEM_NODE_HEAP_INIT_CAPACITY     = 40;
static const float      MEM_NODE_HEAP_FILL_FACTOR       = 0.75;
static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR     = 2;

static const unsigned   MEM_GAP_IX_INIT_CAPACITY        = 40;
static const float      MEM_GAP_IX_FILL_FACTOR          = 0.75;
static const unsigned   MEM_GAP_IX_EXPAND_FACTOR        = 2;



/*********************/
/*                   */
/* Type declarations */
/*                   */
/*********************/
typedef struct _node {
    alloc_t alloc_record;
    unsigned used;
    unsigned allocated;
    struct _node *next, *prev; // doubly-linked list for gap deletion
} node_t, *node_pt;

typedef struct _gap {
    size_t size;
    node_pt node;
} gap_t, *gap_pt;

typedef struct _pool_mgr {
    pool_t pool;
    node_pt node_heap;
    unsigned total_nodes;
    unsigned used_nodes;
    gap_pt gap_ix;
    unsigned gap_ix_capacity;
} pool_mgr_t, *pool_mgr_pt;



/***************************/
/*                         */
/* Static global variables */
/*                         */
/***************************/
static pool_mgr_pt *pool_store = NULL; // an array of pointers, only expand
static unsigned pool_store_size = 0;
static unsigned pool_store_capacity = 0;



/********************************************/
/*                                          */
/* Forward declarations of static functions */
/*                                          */
/********************************************/
static alloc_status _mem_resize_pool_store();
static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr);
static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status
        _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                           size_t size,
                           node_pt node);
static alloc_status
        _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                size_t size,
                                node_pt node);
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr);



/****************************************/
/*                                      */
/* Definitions of user-facing functions */
/*                                      */
/****************************************/
alloc_status mem_init() {
    //If there is no pool, initialize it and set globals to initial values.
    if (pool_store == NULL) {
        pool_store = malloc(MEM_POOL_STORE_INIT_CAPACITY * sizeof(pool_mgr_pt));
        //make sure the malloc worked.
        if (pool_store == NULL)
            return ALLOC_FAIL;

        //set global to initial
        pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;
        pool_store_size = 0;
        return ALLOC_OK;
    }

    //If there is already a pool, this function should not be called
    //without first calling mem_free();
    else {
        return ALLOC_CALLED_AGAIN;
    }
}

alloc_status mem_free() {
    // ensure that it's called only once for each mem_init
    // make sure all pool managers have been deallocated
    // can free the pool store array
    // update static variables

    //if there is a pool free it.
    if (pool_store != NULL) {
        free(pool_store);
        pool_store = NULL;
        return ALLOC_OK;
    }

    //if there is no pool, return called again as a fail type.
    else {
        return ALLOC_CALLED_AGAIN;
    }
}

pool_pt mem_pool_open(size_t size, alloc_policy policy) {
    // make sure there the pool store is allocated
    if (pool_store == NULL)
        return NULL;

    // expand the pool store, if necessary
    _mem_resize_pool_store();

    // allocate a new mem pool mgr
    // check success, on error return null
    pool_mgr_pt myPoolManager = malloc(sizeof(pool_mgr_t));
    if (myPoolManager == NULL)
        return NULL;

    // allocate a new memory pool
    // check success, on error deallocate mgr and return null
    myPoolManager->pool.mem = malloc(size);
    if (myPoolManager->pool.mem == NULL) {
        free(myPoolManager);
        return NULL;
    }

    // allocate a new node heap
    // check success, on error deallocate mgr/pool and return null
    myPoolManager->node_heap = malloc(MEM_NODE_HEAP_INIT_CAPACITY * sizeof(node_t));
    if (myPoolManager->node_heap == NULL) {
        free(myPoolManager->pool.mem);
        free(myPoolManager);
        return NULL;
    }

    // allocate a new gap index
    // check success, on error deallocate mgr/pool/heap and return null
    myPoolManager->gap_ix = malloc(MEM_GAP_IX_INIT_CAPACITY * sizeof(gap_t));
    if (myPoolManager->gap_ix == NULL) {
        free(myPoolManager->node_heap);
        free(myPoolManager->pool.mem);
        free(myPoolManager);
        return NULL;
    }

    // assign all the pointers and update meta data:
    //   initialize top node of node heap
    myPoolManager->node_heap[0].alloc_record.mem = myPoolManager->pool.mem;
    myPoolManager->node_heap[0].alloc_record.size = size;
    myPoolManager->node_heap[0].used = 1;
    myPoolManager->node_heap[0].allocated = 0;
    myPoolManager->node_heap[0].next = NULL;
    myPoolManager->node_heap[0].prev = NULL;

    //  (was missing) initialize pool mgr pool
    myPoolManager->pool.policy = policy;
    myPoolManager->pool.total_size = size;
    myPoolManager->pool.alloc_size = 0;
    myPoolManager->pool.num_allocs = 0;
    myPoolManager->pool.num_gaps = 0;

    //   initialize top node of gap index
    _mem_add_to_gap_ix(myPoolManager, size, &myPoolManager->node_heap[0]);

    //   initialize pool mgr
    myPoolManager->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;
    myPoolManager->used_nodes = 1;
    myPoolManager->gap_ix_capacity = MEM_GAP_IX_INIT_CAPACITY;

    //   link pool mgr to pool store
    pool_store[pool_store_size] = myPoolManager;
    pool_store_size = pool_store_size + 1;

    // return the address of the mgr, cast to (pool_pt)
    return &(myPoolManager->pool);
}

alloc_status mem_pool_close(pool_pt pool) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt myPoolManager = (pool_mgr_pt)pool;
    // check if this pool is allocated
    if (myPoolManager->pool.mem == NULL) {
        return ALLOC_CALLED_AGAIN;
    }

    // check if pool has only one gap
    else if (myPoolManager->pool.num_gaps != 1) {
        return ALLOC_NOT_FREED;
    }

    // check if it has zero allocations
    else if (myPoolManager->pool.num_allocs != 0) {
        return ALLOC_NOT_FREED;
    }

    else {
        // free memory pool
        free(myPoolManager->pool.mem);
        // free node heap
        free(myPoolManager->node_heap);
        // free gap index
        free(myPoolManager->gap_ix);
        // find mgr in pool store and set to null
        for (int i = 0; i < pool_store_size; i++) {
            if (pool_store[i] == myPoolManager) {
                pool_store[i] = NULL;
            }
        }

        // note: don't decrement pool_store_size, because it only grows
        // free mgr
        free(myPoolManager);

        return ALLOC_OK;
    }
}

alloc_pt mem_new_alloc(pool_pt pool, size_t size) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt myPoolManager = (pool_mgr_pt) pool;
    int gapNumber = 0;
    size_t remainingGap = 0;
    node_pt myNode = NULL;
    node_pt unusedNode = NULL;
    // check if any gaps, return null if none
    if (myPoolManager->pool.num_gaps == 0) {
        return NULL;
    }

    // expand heap node, if necessary, quit on error
    if (_mem_resize_node_heap(myPoolManager) != ALLOC_OK) {
        return NULL;
    };

    // check used nodes fewer than total nodes, quit on error
    if (myPoolManager->used_nodes > myPoolManager->total_nodes) {
        return NULL;
    }
    // get a node for allocation:
    // if FIRST_FIT, then find the first sufficient node in the node heap
    if (myPoolManager->pool.policy == FIRST_FIT) {
        for (int i = 0; i < myPoolManager->total_nodes; i++) {
            if (myPoolManager->node_heap[i].used == 1) {
                if (myPoolManager->node_heap[i].allocated == 0) {
                    if (myPoolManager->node_heap[i].alloc_record.size >= size) {
                        myNode = &myPoolManager->node_heap[i];
                        break;
                    }
                }
            }
        }
    }

    // if BEST_FIT, then find the first sufficient node in the gap index
    else if (myPoolManager->pool.policy == BEST_FIT) {
        for(int i = 0; i < myPoolManager->pool.num_gaps; ++i) {
            if (myPoolManager->gap_ix[i].size >= size) {
                myNode = myPoolManager->gap_ix[i].node;
                break;
            }
        }
    }

    else {
        return NULL;
    }

    // check if node found
    if (myNode == NULL)
        return NULL;

    // update metadata (num_allocs, alloc_size)
    myPoolManager->pool.num_allocs += 1;
    myPoolManager->pool.alloc_size += size;

    // calculate the size of the remaining gap, if any
    remainingGap = myNode->alloc_record.size - size;

    // remove node from gap index
    _mem_remove_from_gap_ix(myPoolManager, size, myNode);

    // convert gap_node to an allocation node of given size
    myNode->allocated = 1;
    myNode->alloc_record.size = size;
    // adjust node heap:
    //   if remaining gap, need a new node
    if (remainingGap > 0) {
        //   find an unused one in the node heap
        for (int i = 0; i < myPoolManager->total_nodes; ++i) {
            if (myPoolManager->node_heap[i].used == 0) {
                unusedNode = &(myPoolManager->node_heap[i]);
                break;
            }
        }

        //   make sure one was found
        if (unusedNode == NULL)
            return NULL;

        //   initialize it to a gap node
        unusedNode->used = 1;
        unusedNode->allocated = 0;
        unusedNode->alloc_record.mem = myNode->alloc_record.mem + myNode->alloc_record.size;
        unusedNode->alloc_record.size = remainingGap;

        //   update metadata (used_nodes)
        myPoolManager->used_nodes += 1;

        //   update linked list (new node right after the node for allocation)
        if (myNode->next) {
            myNode->next->prev = unusedNode;
        }
        unusedNode->next = myNode->next;
        myNode->next = unusedNode;
        unusedNode->prev = myNode;

        //   add to gap index
        //   check if successful
        if (_mem_add_to_gap_ix(myPoolManager, remainingGap, unusedNode) != ALLOC_OK)
            return NULL;
    }
    // return allocation record by casting the node to (alloc_pt)
    return (alloc_pt)myNode;
}

alloc_status mem_del_alloc(pool_pt pool, alloc_pt alloc) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt myPoolManager = (pool_mgr_pt) pool;

    // get node from alloc by casting the pointer to (node_pt)
    node_pt myNode = (node_pt) alloc;

    // find the node in the node heap
    // this is node-to-delete
    node_pt node = NULL;
    for (int i = 0; i < myPoolManager->total_nodes; ++i) {
        if (myPoolManager->node_heap[i].used == 1) {
            if (&myPoolManager->node_heap[i] == myNode) {
                node = &myPoolManager->node_heap[i];
                break;
            }
        }
    }

    // make sure it's found
    if (node == NULL)
        return ALLOC_FAIL;

    // convert to gap node
    node->allocated = 0;

    // update metadata (num_allocs, alloc_size)
    myPoolManager->pool.num_allocs -= 1;
    myPoolManager->pool.alloc_size -= node->alloc_record.size;

    // if the next node in the list is also a gap, merge into node-to-delete
    if (node->next != NULL && node->next->allocated == 0) {
        node_pt nextNode = node->next;
        //   remove the next node from gap index
        //   check success
        if(_mem_remove_from_gap_ix(myPoolManager, nextNode->alloc_record.size, nextNode) != ALLOC_OK)
            return ALLOC_FAIL;

        //   add the size to the node-to-delete
        node->alloc_record.size += nextNode->alloc_record.size;

        //   update node as unused
        nextNode->used = 0;

        //   update metadata (used nodes)
        myPoolManager->used_nodes -= 1;

        //   update linked list:
        /*
                        if (next->next) {
                            next->next->prev = node_to_del;
                            node_to_del->next = next->next;
                        } else {
                            node_to_del->next = NULL;
                        }
                        next->next = NULL;
                        next->prev = NULL;
         */
        if (nextNode->next) {
            nextNode->next->prev = nextNode->prev;
            node->next = nextNode->next;
        }
        else {
            node->next = NULL;
        }
        nextNode->next = NULL;
        nextNode->prev = NULL;
    }

    // this merged node-to-delete might need to be added to the gap index
    // but one more thing to check...
    // if the previous node in the list is also a gap, merge into previous!
    if (node->prev != NULL && node->prev->allocated == 0) {
        node_pt prevNode = node->prev;

        //   remove the previous node from gap index
        //   check success
        if (_mem_remove_from_gap_ix(myPoolManager, node->prev->alloc_record.size, node->prev) != ALLOC_OK)
            return ALLOC_FAIL;

        //   add the size of node-to-delete to the previous
        node->prev->alloc_record.size += node->alloc_record.size;

        //   update node-to-delete as unused
        node->used = 0;

        //   update metadata (used_nodes)
        myPoolManager->used_nodes -= 1;

        //   update linked list
        /*
                        if (node_to_del->next) {
                            prev->next = node_to_del->next;
                            node_to_del->next->prev = prev;
                        } else {
                            prev->next = NULL;
                        }
                        node_to_del->next = NULL;
                        node_to_del->prev = NULL;
         */
        if (node->next) {
            prevNode->next = node->next;
            prevNode->next->prev = prevNode;
        }
        else {
            prevNode->next = NULL;
        }
        node->next = NULL;
        node->prev = NULL;

        node = prevNode;
    }

    //   change the node to add to the previous node!
    // add the resulting node to the gap index
    // check success
    if (_mem_add_to_gap_ix(myPoolManager, node->alloc_record.size, node) != ALLOC_OK)
        return ALLOC_FAIL;
    return ALLOC_OK;
}

void mem_inspect_pool(pool_pt pool,
                      pool_segment_pt *segments,
                      unsigned *num_segments) {
    // get the mgr from the pool
    pool_mgr_pt myPoolManager = (pool_mgr_pt) pool;

    // allocate the segments array with size == used_nodes
    pool_segment_pt segments_array = malloc(myPoolManager->used_nodes * sizeof(pool_segment_t));

    // check successful
    if (segments_array == NULL)
        return;

    // loop through the node heap and the segments array
    //    for each node, write the size and allocated in the segment
    // "return" the values:
    /*
                    *segments = segs;
                    *num_segments = pool_mgr->used_nodes;
     */
    node_pt firstNode = myPoolManager->node_heap;
    for (int i = 0; i < myPoolManager->used_nodes; ++i) {
        segments_array[i].size = firstNode->alloc_record.size;
        segments_array[i].allocated = firstNode->allocated;
        firstNode = firstNode->next;
    }
    *segments = segments_array;
    *num_segments = myPoolManager->used_nodes;
}



/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/
static alloc_status _mem_resize_pool_store() {
    //If the mem has to be expanded
    if (((float) pool_store_size / pool_store_capacity)
        > MEM_POOL_STORE_FILL_FACTOR) {
        //First set the capacity to what it needs to be, then realloc for the new size.
        pool_store_capacity = pool_store_capacity * MEM_EXPAND_FACTOR;
        pool_store = realloc(pool_store, (pool_store_capacity * sizeof(pool_mgr_pt)));

        //make sure the realloc worked.
        if (pool_store = NULL)
            return ALLOC_FAIL;
        else
            return ALLOC_OK;
    }
    else {
        return ALLOC_OK;
    }
}

static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr) {
    //If node_heap has to be expanded
    if (((float) pool_mgr->used_nodes / pool_mgr->total_nodes)
            > MEM_NODE_HEAP_FILL_FACTOR) {
        //First set the total to what it needs to be, then realloc for new size.
        pool_mgr->total_nodes = pool_mgr->total_nodes * MEM_NODE_HEAP_EXPAND_FACTOR;
        pool_mgr->node_heap = realloc(pool_mgr->node_heap, (pool_mgr->total_nodes * sizeof(node_t)));

        //make sure the realloc worked.
        if (pool_mgr->node_heap == NULL)
            return ALLOC_FAIL;
        else
            return ALLOC_OK;
    }
    else {
        return ALLOC_OK;
    }
}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr) {
    //If gap_ix has to be expanded
    if (((float) pool_mgr->pool.num_gaps / pool_mgr->gap_ix_capacity)
        > MEM_GAP_IX_FILL_FACTOR) {
        //First set the capacity to what it needs to be, then realloc for new size.
        pool_mgr->gap_ix_capacity = pool_mgr->gap_ix_capacity * MEM_GAP_IX_EXPAND_FACTOR;
        pool_mgr->gap_ix = realloc(pool_mgr->gap_ix, (pool_mgr->gap_ix_capacity * sizeof(gap_t)));

        //make sure the realloc worked.
        if (pool_mgr->gap_ix == NULL)
            return ALLOC_FAIL;
        else
            return ALLOC_OK;
    }
    else {
        return ALLOC_OK;
    }
}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                                       size_t size,
                                       node_pt node) {

    // expand the gap index, if necessary (call the function)
    //assert(_mem_resize_gap_ix(pool_mgr) == ALLOC_OK);
    assert(_mem_resize_gap_ix(pool_mgr) == ALLOC_OK);

    // add the entry at the end
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = size;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = node;

    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps += 1;

    // sort the gap index (call the function)
    // check success
    return _mem_sort_gap_ix(pool_mgr);
}

static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                            size_t size,
                                            node_pt node) {
    // find the position of the node in the gap index
    int position = -10;
    for (int i = 0; i < pool_mgr->pool.num_gaps; ++i) {
        if (pool_mgr->gap_ix[i].node == node) {
            position = i;
            break;
        }
    }

    if (position == -1)
        return ALLOC_FAIL;

    // loop from there to the end of the array:
    //    pull the entries (i.e. copy over) one position up
    //    this effectively deletes the chosen node
    while ( position < (pool_mgr->pool.num_gaps) ) {
        pool_mgr->gap_ix[position].size = pool_mgr->gap_ix[position+1].size;
        pool_mgr->gap_ix[position].node = pool_mgr->gap_ix[position+1].node;
        ++position;
    }

    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps = pool_mgr->pool.num_gaps - 1;

    // zero out the element at position num_gaps!
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = 0;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = NULL;

    return ALLOC_OK;
}

// note: only called by _mem_add_to_gap_ix, which appends a single entry
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr) {
    // the new entry is at the end, so "bubble it up"
    // loop from num_gaps - 1 until but not including 0:

    //zoot zoot bubble sort
    for(int i = pool_mgr->pool.num_gaps -1; i > 0; --i) {
        //    if the size of the current entry is less than the previous (u - 1)
        if (pool_mgr->gap_ix[i].size <= pool_mgr->gap_ix[i-1].size) {
            //    or if the sizes are the same but the current entry points to a
            //    node with a lower address of pool allocation address (mem)
            if (pool_mgr->gap_ix[i].size == pool_mgr->gap_ix[i-1].size) {
                if (pool_mgr->gap_ix[i].node > pool_mgr->gap_ix[i-1].node)
                    break;
            }
            //       swap them (by copying) (remember to use a temporary variable)
            node_pt tmpNode = pool_mgr->gap_ix[i-1].node;
            size_t tmpSize = pool_mgr->gap_ix[i-1].size;

            pool_mgr->gap_ix[i-1].node = pool_mgr->gap_ix[i].node;
            pool_mgr->gap_ix[i-1].size = pool_mgr->gap_ix[i].size;

            pool_mgr->gap_ix[i].node = tmpNode;
            pool_mgr->gap_ix[i].size = tmpSize;
        }
        else
            break;
    }

    return ALLOC_OK;
}


