#include "splinterdb/transaction.h"
#include "transaction_internal.h"
#include "splinterdb_internal.h"
#include "platform_linux/platform.h"
#include "data_internal.h"
#include "util.h"
#include "poison.h"

tictoc_timestamp_set ZERO_TICTOC_TIMESTAMP_SET = {.dummy = 0,
                                                  .wts   = 0,
                                                  .delta = 0};

static inline bool
is_serializable(tictoc_transaction *tt_txn)
{
   return (tt_txn->isol_level == TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE);
}

/* Uncomment if necessary
static inline bool
is_snapshot_isolation(tictoc_transaction *tt_txn)
{
   return (tt_txn->isol_level == TRANSACTION_ISOLATION_LEVEL_SNAPSHOT);
}
*/

static inline bool
is_repeatable_read(tictoc_transaction *tt_txn)
{
   return (tt_txn->isol_level == TRANSACTION_ISOLATION_LEVEL_REPEATABLE_READ);
}


static inline void
tictoc_rw_entry_set_key(tictoc_rw_entry *e, slice key, const data_config *cfg)
{
   char *key_buf;
   key_buf = TYPED_ARRAY_ZALLOC(0, key_buf, KEY_SIZE);
   memmove(key_buf, slice_data(key), slice_length(key));
   e->key   = slice_create(KEY_SIZE, key_buf);
   e->start = e->last = interval_tree_key_create(e->key, cfg);
}


static inline void
tictoc_rw_entry_set_msg(tictoc_rw_entry *e, message msg)
{
   char *msg_buf;
   msg_buf = TYPED_ARRAY_ZALLOC(0, msg_buf, message_length(msg));
   memmove(msg_buf, message_data(msg), message_length(msg));
   e->msg = message_create(message_class(msg),
                           slice_create(message_length(msg), msg_buf));
}


/*
 * Algorithm 1: Read Phase
 */

static inline int
tictoc_read(transactional_splinterdb *txn_kvsb,
            tictoc_transaction       *tt_txn,
            slice                     user_key,
            splinterdb_lookup_result *result)
{
   int rc = splinterdb_lookup(txn_kvsb->kvsb, user_key, result);

   if (splinterdb_lookup_found(result)) {
      tictoc_rw_entry *r = tictoc_get_new_read_set_entry(tt_txn);
      platform_assert(!tictoc_rw_entry_is_invalid(r));

      tictoc_rw_entry_set_key(r, key, txn_kvsb->tcfg->kvsb_cfg.data_cfg);

      KeyType    key_ht   = (KeyType)slice_data(r->key);
      ValueType *value_ht = (ValueType *)&ZERO_TICTOC_TIMESTAMP_SET;

      if (iceberg_insert(
             &txn_kvsb->tscache, key_ht, *value_ht, platform_thread_id_self()))
      {
         r->need_to_keep_key = TRUE;
      }

      value_ht = NULL;
      iceberg_get_value(
         &txn_kvsb->tscache, key_ht, &value_ht, platform_thread_id_self());
      platform_assert(value_ht);

      tictoc_timestamp_set *ts_set = (tictoc_timestamp_set *)value_ht;
      r->wts                       = ts_set->wts;
      r->rts                       = tictoc_timestamp_set_get_rts(ts_set);
   }

   return rc;
}

/*
 * Algorithm 2: Validation Phase
 */
static bool
tictoc_validation(transactional_splinterdb *txn_kvsb,
                  tictoc_transaction       *tt_txn)
{
   // Step 2: Compute the Commit Timestamp
   tt_txn->commit_wts = 0;
   tt_txn->commit_rts = 0;

   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      tictoc_rw_entry *w = tictoc_get_write_set_entry(tt_txn, i);

      KeyType    key_ht   = (KeyType)slice_data(w->key);
      ValueType *value_ht = NULL;
      iceberg_get_value(
         &txn_kvsb->tscache, key_ht, &value_ht, platform_thread_id_self());

      bool key_exists_in_cache = (value_ht != NULL);
      if (key_exists_in_cache) {
         tictoc_timestamp_set *ts_set = (tictoc_timestamp_set *)value_ht;
         tt_txn->commit_wts =
            MAX(tt_txn->commit_wts, tictoc_timestamp_set_get_rts(ts_set) + 1);
      } else {
         tt_txn->commit_wts = MAX(tt_txn->commit_wts, 1);
      }
   }

   for (uint64 i = 0; i < tt_txn->read_cnt; ++i) {
      tictoc_rw_entry *r = tictoc_get_read_set_entry(tt_txn, i);
      tt_txn->commit_rts = MAX(tt_txn->commit_rts, r->wts);
   }

   if (is_serializable(tt_txn) || is_repeatable_read(tt_txn)) {
      tt_txn->commit_rts = tt_txn->commit_wts =
         MAX(tt_txn->commit_rts, tt_txn->commit_wts);
   }

   // Step 3: Validate the Read Set
   for (uint64 i = 0; i < tt_txn->read_cnt; ++i) {
      tictoc_rw_entry *r = tictoc_get_read_set_entry(tt_txn, i);

      bool is_read_entry_invalid = r->rts < tt_txn->commit_rts;
      if (is_read_entry_invalid) {
         hash_lock_acquire(&txn_kvsb->hash_lock, r->key);

         KeyType    key_ht   = (char *)slice_data(r->key);
         ValueType *value_ht = NULL;
         iceberg_get_value(
            &txn_kvsb->tscache, key_ht, &value_ht, platform_thread_id_self());
         platform_assert(value_ht);

         tictoc_timestamp_set *ts_set = (tictoc_timestamp_set *)value_ht;

         bool is_read_entry_written_by_another = r->wts != ts_set->wts;
         bool is_read_entry_locked_by_another =
            tictoc_timestamp_set_get_rts(ts_set) <= tt_txn->commit_rts
            && lock_table_is_entry_locked(txn_kvsb->lock_tbl, r)
            && tictoc_rw_entry_is_not_in_write_set(
               tt_txn, r, txn_kvsb->tcfg->kvsb_cfg.data_cfg);
         bool need_to_abort =
            is_read_entry_written_by_another || is_read_entry_locked_by_another;
         if (need_to_abort) {
            hash_lock_release(&txn_kvsb->hash_lock, r->key);
            return FALSE;
         }

         uint32 new_rts =
            MAX(tt_txn->commit_rts, tictoc_timestamp_set_get_rts(ts_set));
         bool need_to_update_rts =
            (new_rts != tictoc_timestamp_set_get_rts(ts_set))
            && !is_repeatable_read(tt_txn);
         if (need_to_update_rts) {
            tictoc_timestamp_set *ts_set_ptr = (tictoc_timestamp_set *)value_ht;
            ts_set_ptr->wts                  = ts_set->wts;
            ts_set_ptr->delta =
               tictoc_timestamp_set_get_delta(ts_set->wts, new_rts);
         }
         hash_lock_release(&txn_kvsb->hash_lock, r->key);
      }
   }

   return TRUE;
}

/*
 * Algorithm 3: Write Phase
 */
static void
tictoc_write(transactional_splinterdb *txn_kvsb, tictoc_transaction *tt_txn)
{
   const splinterdb *kvsb = txn_kvsb->kvsb;

   for (uint64 i = 0; i < tt_txn->write_cnt; ++i) {
      tictoc_rw_entry *w = tictoc_get_write_set_entry(tt_txn, i);

      int rc = 0;

      switch (message_class(w->msg)) {
         case MESSAGE_TYPE_INSERT:
            rc = splinterdb_insert(kvsb, w->key, message_slice(w->msg));
            break;
         case MESSAGE_TYPE_UPDATE:
            rc = splinterdb_update(kvsb, w->key, message_slice(w->msg));
            break;
         case MESSAGE_TYPE_DELETE:
            rc = splinterdb_delete(kvsb, w->key);
            break;
         default:
            break;
      }

      platform_assert(rc == 0, "Error from SplinterDB: %d\n", rc);

      tictoc_timestamp_set ts_set = {
         .wts   = tt_txn->commit_wts,
         .delta = tictoc_timestamp_set_get_delta(tt_txn->commit_wts,
                                                 tt_txn->commit_rts)};

      KeyType    key_ht   = (KeyType)slice_data(w->key);
      ValueType *value_ht = (ValueType *)&ts_set;

      iceberg_update(
         &txn_kvsb->tscache, key_ht, *value_ht, platform_thread_id_self());
   }
}

static int
tictoc_local_write(transactional_splinterdb *txn_kvsb,
                   tictoc_transaction       *txn,
                   tictoc_timestamp_set      ts_set,
                   slice                     user_key,
                   message                   msg)
{
   const data_config *cfg = txn_kvsb->tcfg->kvsb_cfg.data_cfg;

   // TODO: this part can be done by binary search if the write_set is sorted
   for (uint64 i = 0; i < txn->write_cnt; ++i) {
      tictoc_rw_entry *w = tictoc_get_write_set_entry(txn, i);

      if (data_key_compare(cfg, w->key, key) == 0) {
         if (message_is_definitive(msg)) {
            platform_free_from_heap(0, (void *)message_data(w->msg));
            tictoc_rw_entry_set_msg(w, msg);
         } else {
            platform_assert(message_class(w->msg) != MESSAGE_TYPE_DELETE);

            merge_accumulator new_message;
            merge_accumulator_init_from_message(&new_message, 0, msg);
            data_merge_tuples(cfg, key, w->msg, &new_message);

            platform_free_from_heap(0, (void *)message_data(w->msg));

            char *msg_buf;
            msg_buf = TYPED_ARRAY_ZALLOC(0, msg_buf,  merge_accumulator_length(&new_message));
            memmove(msg_buf,
                    merge_accumulator_data(&new_message),
                    merge_accumulator_length(&new_message));
            w->msg = message_create(
               merge_accumulator_message_class(&new_message),
               slice_create(merge_accumulator_length(&new_message), msg_buf));

            merge_accumulator_deinit(&new_message);
         }

         w->wts = ts_set.wts;
         w->rts = tictoc_timestamp_set_get_rts(&ts_set);

         return 0;
      }
   }

   tictoc_rw_entry *w = tictoc_get_new_write_set_entry(txn);
   platform_assert(!tictoc_rw_entry_is_invalid(w));

   tictoc_rw_entry_set_key(w, key, txn_kvsb->tcfg->kvsb_cfg.data_cfg);
   tictoc_rw_entry_set_msg(w, msg);
   w->wts = ts_set.wts;
   w->rts = tictoc_timestamp_set_get_rts(&ts_set);

   return 0;
}

static void
transactional_splinterdb_config_init(
   transactional_splinterdb_config *txn_splinterdb_cfg,
   const splinterdb_config         *kvsb_cfg)
{
   memmove(txn_splinterdb_cfg, kvsb_cfg, sizeof(txn_splinterdb_cfg->kvsb_cfg));
   txn_splinterdb_cfg->isol_level = TRANSACTION_ISOLATION_LEVEL_SERIALIZABLE;
   txn_splinterdb_cfg->tscache_log_slots = 28;
}

static int
transactional_splinterdb_create_or_open(const splinterdb_config   *kvsb_cfg,
                                        transactional_splinterdb **txn_kvsb,
                                        bool open_existing)
{
   transactional_splinterdb_config *txn_splinterdb_cfg;
   txn_splinterdb_cfg = TYPED_ZALLOC(0, txn_splinterdb_cfg);
   transactional_splinterdb_config_init(txn_splinterdb_cfg, kvsb_cfg);

   transactional_splinterdb *_txn_kvsb;
   _txn_kvsb       = TYPED_ZALLOC(0, _txn_kvsb);
   _txn_kvsb->tcfg = txn_splinterdb_cfg;

   int rc = splinterdb_create_or_open(
      &txn_splinterdb_cfg->kvsb_cfg, &_txn_kvsb->kvsb, open_existing);
   bool fail_to_create_splinterdb = (rc != 0);
   if (fail_to_create_splinterdb) {
      platform_free(0, _txn_kvsb);
      platform_free(0, txn_splinterdb_cfg);
      return rc;
   }

   _txn_kvsb->lock_tbl = lock_table_create();

   hash_lock_init(&_txn_kvsb->hash_lock, &default_hash_lock_config);

   iceberg_init(&_txn_kvsb->tscache, txn_splinterdb_cfg->tscache_log_slots);

   *txn_kvsb = _txn_kvsb;

   return 0;
}

int
transactional_splinterdb_create(const splinterdb_config   *kvsb_cfg,
                                transactional_splinterdb **txn_kvsb)
{
   return transactional_splinterdb_create_or_open(kvsb_cfg, txn_kvsb, FALSE);
}


int
transactional_splinterdb_open(const splinterdb_config   *kvsb_cfg,
                              transactional_splinterdb **txn_kvsb)
{
   return transactional_splinterdb_create_or_open(kvsb_cfg, txn_kvsb, TRUE);
}

void
transactional_splinterdb_close(transactional_splinterdb **txn_kvsb)
{
   transactional_splinterdb *_txn_kvsb = *txn_kvsb;
   splinterdb_close(&_txn_kvsb->kvsb);

   hash_lock_deinit(&_txn_kvsb->hash_lock);

   lock_table_destroy(_txn_kvsb->lock_tbl);

   platform_free(0, _txn_kvsb->tcfg);
   platform_free(0, _txn_kvsb);

   *txn_kvsb = NULL;
}

void
transactional_splinterdb_register_thread(transactional_splinterdb *kvs)
{
   splinterdb_register_thread(kvs->kvsb);
}

void
transactional_splinterdb_deregister_thread(transactional_splinterdb *kvs)
{
   splinterdb_deregister_thread(kvs->kvsb);
}

int
transactional_splinterdb_begin(transactional_splinterdb *txn_kvsb,
                               transaction              *txn)
{
   tictoc_transaction_init(&txn->tictoc, txn_kvsb->tcfg->isol_level);
   return 0;
}

int
transactional_splinterdb_commit(transactional_splinterdb *txn_kvsb,
                                transaction              *txn)
{
   tictoc_transaction *tt_txn = &txn->tictoc;

   bool write_successfully = FALSE;

   // Step 1: Lock Write Set
   tictoc_transaction_sort_write_set(tt_txn, txn_kvsb->tcfg->kvsb_cfg.data_cfg);

   while (tictoc_transaction_lock_all_write_set(tt_txn, txn_kvsb->lock_tbl)
          == FALSE)
   {
      platform_sleep_ns(1000); // 1us is the value that is mentioned in the paper
   }

   if (tictoc_validation(txn_kvsb, &txn->tictoc)) {
      tictoc_write(txn_kvsb, &txn->tictoc);
      write_successfully = TRUE;
   }

   tictoc_transaction_unlock_all_write_set(tt_txn, txn_kvsb->lock_tbl);

   for (int i = 0; i < tt_txn->read_cnt; ++i) {
      tictoc_rw_entry *r      = tictoc_get_read_set_entry(tt_txn, i);
      KeyType          key_ht = (KeyType)slice_data(r->key);
      // Decrease the refcount by one
      if (iceberg_remove_and_get_key(
             &txn_kvsb->tscache, &key_ht, platform_thread_id_self()))
      {
         if (slice_data(r->key) != key_ht) {
            platform_free_from_heap(0, key_ht);
         }
      }
   }
   tictoc_transaction_deinit(tt_txn, txn_kvsb->lock_tbl);

   return write_successfully ? 0 : -1;
}

int
transactional_splinterdb_abort(transactional_splinterdb *txn_kvsb,
                               transaction              *txn)
{
   tictoc_transaction_deinit(&txn->tictoc, txn_kvsb->lock_tbl);

   return 0;
}

int
transactional_splinterdb_insert(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                slice                     value)
{
   return tictoc_local_write(txn_kvsb,
                             &txn->tictoc,
                             ZERO_TICTOC_TIMESTAMP_SET,
                             user_key,
                             message_create(MESSAGE_TYPE_INSERT, value));
}

int
transactional_splinterdb_delete(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key)
{
   return tictoc_local_write(
      txn_kvsb, &txn->tictoc, ZERO_TICTOC_TIMESTAMP_SET, user_key, DELETE_MESSAGE);
}

int
transactional_splinterdb_update(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                slice                     delta)
{
   return tictoc_local_write(txn_kvsb,
                             &txn->tictoc,
                             ZERO_TICTOC_TIMESTAMP_SET,
                             user_key,
                             message_create(MESSAGE_TYPE_UPDATE, delta));
}

int
transactional_splinterdb_lookup(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     user_key,
                                splinterdb_lookup_result *result)
{
   return tictoc_read(txn_kvsb, &txn->tictoc, user_key, result);
}

void
transactional_splinterdb_lookup_result_init(
   transactional_splinterdb *txn_kvsb,   // IN
   splinterdb_lookup_result *result,     // IN/OUT
   uint64                    buffer_len, // IN
   char                     *buffer      // IN
)
{
   return splinterdb_lookup_result_init(
      txn_kvsb->kvsb, result, buffer_len, buffer);
}

void
transactional_splinterdb_set_isolation_level(
   transactional_splinterdb   *txn_kvsb,
   transaction_isolation_level isol_level)
{
   platform_assert(isol_level > TRANSACTION_ISOLATION_LEVEL_INVALID);
   platform_assert(isol_level < TRANSACTION_ISOLATION_LEVEL_MAX_VALID);

   txn_kvsb->tcfg->isol_level = isol_level;
}
