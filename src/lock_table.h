// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef _LOCK_TABLE_H_
#define _LOCK_TABLE_H_

#include "splinterdb/data.h"
#include "interval_tree/interval_tree_generic.h"
#include "util.h"
#include "experimental_mode.h"

/*
 * Lock Table Entry call tictoc_rw_entry
 */

typedef struct interval_tree_key {
   slice              data;
   const data_config *app_data_cfg;
} interval_tree_key;

static inline interval_tree_key
interval_tree_key_create(slice data, const data_config *app_data_cfg)
{
   return (interval_tree_key){.data = data, .app_data_cfg = app_data_cfg};
}

/*
 * Definition of tictoc_rw_entry and ones related to it
 */

/* typedef uint32 tictoc_timestamp; */

/* typedef struct PACKED tictoc_timestamp_set { */
/*    tictoc_timestamp rts; */
/*    tictoc_timestamp wts; */
/* } tictoc_timestamp_set; */

typedef uint64 tictoc_timestamp;

typedef struct PACKED tictoc_timestamp_set {
   uint64 dummy : 5;
   uint64 delta : 15; // rts = wts + delta
   uint64 wts : 44;
} tictoc_timestamp_set;

extern tictoc_timestamp_set ZERO_TICTOC_TIMESTAMP_SET;

static inline tictoc_timestamp
tictoc_timestamp_set_get_rts(tictoc_timestamp_set *ts)
{
#if EXPERIMENTAL_MODE_SILO == 1
   return ts->wts;
#else
   return ts->wts + ts->delta;
#endif
}

static inline tictoc_timestamp
tictoc_timestamp_set_get_delta(tictoc_timestamp wts, tictoc_timestamp rts)
{
#if EXPERIMENTAL_MODE_SILO == 1
   return 0;
#else
   return rts - wts;
#endif
}

// read_set and write_set entry stored locally
typedef struct tictoc_rw_entry {
   slice            key;
   message          msg; // value + op
   tictoc_timestamp wts;
   tictoc_timestamp rts;

   bool need_to_keep_key;
   bool need_to_decrease_refcount;

   struct rb_node    rb;
   interval_tree_key start;
   interval_tree_key last;
   interval_tree_key __subtree_last;
} tictoc_rw_entry;

/*
 * Lock Table Functions
 */

typedef struct lock_table lock_table;

lock_table *
lock_table_create();

void
lock_table_destroy(lock_table *lock_tbl);

void
lock_table_acquire_entry_lock(lock_table *lock_tbl, tictoc_rw_entry *entry);

bool
lock_table_try_acquire_entry_lock(lock_table *lock_tbl, tictoc_rw_entry *entry);

void
lock_table_release_entry_lock(lock_table *lock_tbl, tictoc_rw_entry *entry);

bool
lock_table_is_entry_locked(lock_table *lock_tbl, tictoc_rw_entry *entry);

#endif // _LOCK_TABLE_H_
