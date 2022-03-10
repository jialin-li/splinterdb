// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * splinterdb_quick_test.c --
 *
 *     Quick test of the public API of SplinterDB
 *
 * NOTE: This test case file also serves as an example for how-to build
 *  CTests, and the syntax for different commands etc. Note the
 *  annotations to learn how to write new unit-tests using Ctests.
 *
 * Naming Conventions:
 *
 *  o The file containing unit-test cases for a module / functionality is
 *    expected to be named <something>_test.c
 *
 *  o Individual test cases [ see below ] in a file are prefaced with a
 *    term naming the test suite, for the module / functionality being tested.
 *    Usually it will just be <something>; .e.g., in splinterdb_test.c
 *    the suite-name is 'splinterdb_quick'.
 *
 *  o Each test case should be named test_<operation>
 * -----------------------------------------------------------------------------
 */
#include <stdlib.h> // Needed for system calls; e.g. free
#include <string.h>
#include <errno.h>

#include "splinterdb/data.h"
#include "splinterdb/platform_public.h"
#include "splinterdb/default_data_config.h"
#include "splinterdb/splinterdb.h"
#include "unit_tests.h"
#include "util.h"
#include "test_data.h"
#include "ctest.h" // This is required for all test-case files.
#include "btree.h" // for MAX_INLINE_MESSAGE_SIZE

#define TEST_INSERT_KEY_LENGTH 7
#define TEST_INSERT_VAL_LENGTH 7

#define TEST_MAX_KEY_SIZE   13
#define TEST_MAX_VALUE_SIZE 32

// Hard-coded format strings to generate key and values
static const char key_fmt[] = "key-%02x";
static const char val_fmt[] = "val-%02x";

// Function Prototypes
static void
create_default_cfg(splinterdb_config *out_cfg, data_config *default_data_cfg);


static int
insert_some_keys(const int num_inserts, splinterdb *kvsb);

static int
insert_keys(splinterdb *kvsb, const int minkey, int numkeys, const int incr);

static int
check_current_tuple(splinterdb_iterator *it, const int expected_i);

static int
custom_key_comparator(const data_config *cfg,
                      uint64             key1_len,
                      const void        *key1,
                      uint64             key2_len,
                      const void        *key2);

typedef struct {
   data_config super;
   uint64      num_comparisons;
} comparison_counting_data_config;

/*
 * Global data declaration macro:
 *
 * This is converted into a struct, with a generated name prefixed by the
 * suite name. This structure is then automatically passed to all tests in
 * the test suite. In this function, declare all structures and
 * variables that you need globally to setup Splinter. This macro essentially
 * resolves to a bunch of structure declarations, so no code fragments can
 * be added here.
 *
 * NOTE: All data structures will hang off of data->, where 'data' is a
 * global static variable manufactured by CTEST_SETUP() macro.
 */
CTEST_DATA(splinterdb_quick)
{
   splinterdb       *kvsb;
   splinterdb_config cfg;

   comparison_counting_data_config default_data_cfg;
};

// Optional setup function for suite, called before every test in suite
CTEST_SETUP(splinterdb_quick)
{
   Platform_stdout_fh = fopen("/tmp/unit_test.stdout", "a+");
   Platform_stderr_fh = fopen("/tmp/unit_test.stderr", "a+");

   default_data_config_init(TEST_MAX_KEY_SIZE, &data->default_data_cfg.super);
   create_default_cfg(&data->cfg, &data->default_data_cfg.super);

   int rc = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);
}

// Optional teardown function for suite, called after every test in suite
CTEST_TEARDOWN(splinterdb_quick)
{
   splinterdb_close(data->kvsb);
}

/*
 * ***********************************************************************
 * All tests in each file are named with one term, which represents the
 * module / functionality you are testing. Here, it is: splinterdb_quick
 *
 * This is an individual test case, testing [usually] just one thing.
 * The 2nd term is the test-case name, e.g., 'test_basic_flow'.
 * ***********************************************************************
 */
/*
 *
 * Basic test case that exercises and validates the basic flow of the
 * Splinter APIs.  We exercise:
 *  - splinterdb_insert()
 *  - splinterdb_lookup() and
 *  - splinterdb_delete()
 *
 * Validate that they behave as expected, including some basic error
 * condition checking.
 */
CTEST2(splinterdb_quick, test_basic_flow)
{
   char  *key     = "some-key";
   size_t key_len = sizeof("some-key");

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(data->kvsb, &result, 0, NULL);

   int rc = splinterdb_lookup(data->kvsb, key_len, key, &result);
   ASSERT_EQUAL(0, rc);

   size_t      val_len;
   const char *value;

   // Lookup of a non-existent key should return not-found.
   ASSERT_FALSE(splinterdb_lookup_found(&result));

   static char *to_insert = "some-value";

   // Basic insert of new key should succeed.
   rc =
      splinterdb_insert(data->kvsb, key_len, key, strlen(to_insert), to_insert);
   ASSERT_EQUAL(0, rc);

   // Lookup of inserted key should succeed.
   rc = splinterdb_lookup(data->kvsb, key_len, key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));

   rc = splinterdb_lookup_result_value(data->kvsb, &result, &val_len, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(strlen(to_insert), val_len);
   ASSERT_STREQN(to_insert, value, val_len);

   // Delete key
   rc = splinterdb_delete(data->kvsb, key_len, key);
   ASSERT_EQUAL(0, rc);

   // Deleted key should not be found
   rc = splinterdb_lookup(data->kvsb, key_len, key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(splinterdb_lookup_found(&result));

   splinterdb_lookup_result_deinit(&result);
}

/*
 * Basic test case that exercises and validates the basic flow of the
 * Splinter APIs for key of max-key-length.
 */
CTEST2(splinterdb_quick, test_apis_for_max_key_length)
{
   char large_key[TEST_MAX_KEY_SIZE];
   memset(large_key, 'a', TEST_MAX_KEY_SIZE);

   static char *large_key_value = "a-value";

   // **** Insert of a max-size key should succeed.
   int rc = splinterdb_insert(data->kvsb,
                              TEST_MAX_KEY_SIZE,
                              large_key,
                              strlen(large_key_value),
                              large_key_value);
   ASSERT_EQUAL(0, rc);

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(data->kvsb, &result, 0, NULL);

   // **** Lookup of max-size key should return correct value
   rc = splinterdb_lookup(data->kvsb, TEST_MAX_KEY_SIZE, large_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));

   size_t      val_len;
   const char *value;
   rc = splinterdb_lookup_result_value(data->kvsb, &result, &val_len, &value);
   ASSERT_EQUAL(0, rc);

   ASSERT_EQUAL(strlen(large_key_value), val_len);
   ASSERT_STREQN(large_key_value,
                 value,
                 val_len,
                 "Large key-value did not match as expected.");

   rc = splinterdb_delete(data->kvsb, TEST_MAX_KEY_SIZE, large_key);
   ASSERT_EQUAL(0, rc);

   // **** Should not find this large-key once it's deleted
   rc = splinterdb_lookup(data->kvsb, TEST_MAX_KEY_SIZE, large_key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(splinterdb_lookup_found(&result));
}

/*
 * Test case to verify core interfaces when key-size is > max key-size.
 */
CTEST2(splinterdb_quick, test_key_size_gt_max_key_size)
{
   size_t too_large_key_len = TEST_MAX_KEY_SIZE + 1;
   char  *too_large_key     = calloc(1, too_large_key_len);
   memset(too_large_key, 'a', too_large_key_len);
   char *value = calloc(1, TEST_MAX_VALUE_SIZE);

   int rc = splinterdb_insert(data->kvsb,
                              too_large_key_len,
                              too_large_key,
                              sizeof("a-value"),
                              "a-value");
   ASSERT_EQUAL(EINVAL, rc);

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(data->kvsb, &result, 0, NULL);

   rc =
      splinterdb_lookup(data->kvsb, too_large_key_len, too_large_key, &result);
   ASSERT_EQUAL(EINVAL, rc);

   rc = splinterdb_delete(data->kvsb, too_large_key_len, too_large_key);
   ASSERT_EQUAL(EINVAL, rc);

   splinterdb_lookup_result_deinit(&result);

   if (too_large_key) {
      free(too_large_key);
   }
   if (value) {
      free(value);
   }
}

/*
 * Test case to verify core interfaces when value-size is > max value-size.
 * Here, we basically exercise the insert interface, which will trip up
 * if very large values are supplied. (Once insert fails, there is
 * no further need to verify the other interfaces for very-large-values.)
 */
CTEST2(splinterdb_quick, test_value_size_gt_max_value_size)
{
   size_t            too_large_value_len = MAX_INLINE_MESSAGE_SIZE + 1;
   char             *too_large_value     = calloc(1, too_large_value_len);
   static const char short_key[]         = "a_short_key";

   memset(too_large_value, 'z', too_large_value_len);
   int rc = splinterdb_insert(data->kvsb,
                              sizeof(short_key),
                              short_key,
                              too_large_value_len,
                              too_large_value);

   ASSERT_EQUAL(EINVAL, rc);
   if (too_large_value) {
      free(too_large_value);
   }
}

/*
 * Test case to exercise APIs for variable-length values; empty value,
 * short and somewhat longish value. After inserting this data, the lookup
 * sub-cases exercises different combinations to cover internal re-allocation
 * when supplied output buffer is smaller than the datum value.
 */
CTEST2(splinterdb_quick, test_variable_length_values)
{
   const char empty_string[0];
   const char short_string[1] = "v";

   char almost_max_length_string[TEST_MAX_VALUE_SIZE - 1];
   memset(almost_max_length_string, 'a', TEST_MAX_VALUE_SIZE - 1);

   char max_length_string[TEST_MAX_VALUE_SIZE];
   memset(max_length_string, 'b', TEST_MAX_VALUE_SIZE);

   // Insert keys with different value (lengths)
   int rc = splinterdb_insert(
      data->kvsb, sizeof("empty"), "empty", sizeof(empty_string), empty_string);
   ASSERT_EQUAL(0, rc);

   rc = splinterdb_insert(
      data->kvsb, sizeof("short"), "short", sizeof(short_string), short_string);
   ASSERT_EQUAL(0, rc);

   rc = splinterdb_insert(data->kvsb,
                          sizeof("long"),
                          "long",
                          sizeof(almost_max_length_string),
                          almost_max_length_string);
   ASSERT_EQUAL(0, rc);

   rc = splinterdb_insert(data->kvsb,
                          sizeof("max"),
                          "max",
                          sizeof(max_length_string),
                          max_length_string);
   ASSERT_EQUAL(0, rc);


   // Allocate and mark a buffer with ample space to hold the results
   char big_buffer[2 * TEST_MAX_VALUE_SIZE];
   memset(big_buffer, 'x', sizeof(big_buffer));

   size_t                   val_len;
   const char              *value;
   splinterdb_lookup_result result;

   // allocate a result that has full access to the buffer
   splinterdb_lookup_result_init(
      data->kvsb, &result, sizeof(big_buffer), big_buffer);

   // look up a 0-length value
   rc = splinterdb_lookup(data->kvsb, sizeof("empty"), "empty", &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));
   rc = splinterdb_lookup_result_value(data->kvsb, &result, &val_len, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(0, val_len);

   // lookup tuple with value of length 1, providing sufficient buffer
   rc = splinterdb_lookup(data->kvsb, sizeof("short"), "short", &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));
   rc = splinterdb_lookup_result_value(data->kvsb, &result, &val_len, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(1, val_len);

   // lookup tuple with almost max-sized-value
   rc = splinterdb_lookup(data->kvsb, sizeof("long"), "long", &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));
   rc = splinterdb_lookup_result_value(data->kvsb, &result, &val_len, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(TEST_MAX_VALUE_SIZE - 1, val_len);
   ASSERT_STREQN(almost_max_length_string, value, val_len);

   // lookup tuple with max-sized-value
   rc = splinterdb_lookup(data->kvsb, sizeof("max"), "max", &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));
   rc = splinterdb_lookup_result_value(data->kvsb, &result, &val_len, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(TEST_MAX_VALUE_SIZE, val_len);
   ASSERT_STREQN(max_length_string, value, val_len);

   // done with the big buffer
   splinterdb_lookup_result_deinit(&result);

   // freshen up the buffer
   memset(big_buffer, 'x', sizeof(big_buffer));

   // init the result again, but pretend the buffer is small
   splinterdb_lookup_result_init(
      data->kvsb, &result, TEST_MAX_VALUE_SIZE / 2, big_buffer);

   // lookup tuple with max-sized-value, passing it the short buffer
   rc = splinterdb_lookup(data->kvsb, sizeof("max"), "max", &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));

   rc = splinterdb_lookup_result_value(data->kvsb, &result, &val_len, &value);
   ASSERT_EQUAL(0, rc);
   // we get the full result back, because internally splinterdb did an
   // allocation
   ASSERT_EQUAL(TEST_MAX_VALUE_SIZE, val_len);
   ASSERT_STREQN(max_length_string, value, TEST_MAX_VALUE_SIZE);

   // our buffer is untouched
   ASSERT_STREQN(
      "xxxxxxxxxxxxxxxxxxxxxxxxx", big_buffer, TEST_MAX_VALUE_SIZE / 2);

   // we can deinit the result, and it doesn't try to free the stack space we
   // originally gave it
   splinterdb_lookup_result_deinit(&result);


   // init another result, but don't give it a buffer
   splinterdb_lookup_result_init(data->kvsb, &result, 0, NULL);
   // lookup, see we get the full result back
   rc = splinterdb_lookup(data->kvsb, sizeof("max"), "max", &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));
   rc = splinterdb_lookup_result_value(data->kvsb, &result, &val_len, &value);
   ASSERT_EQUAL(0, rc);
   // we get the full result back, because internally splinterdb did an
   // allocation
   ASSERT_EQUAL(TEST_MAX_VALUE_SIZE, val_len);
   ASSERT_STREQN(max_length_string, value, TEST_MAX_VALUE_SIZE);

   // we can de-init the result, and it doesn't crash
   splinterdb_lookup_result_deinit(&result);
}

/*
 * iterator test case.
 */
CTEST2(splinterdb_quick, test_basic_iterator)
{
   const int num_inserts = 50;
   int       rc          = insert_some_keys(num_inserts, data->kvsb);
   ASSERT_EQUAL(0, rc);

   int i = 0;

   splinterdb_iterator *it = NULL;

   rc = splinterdb_iterator_init(data->kvsb, &it, 0, NULL);
   ASSERT_EQUAL(0, rc);

   for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      rc = check_current_tuple(it, i);
      ASSERT_EQUAL(0, rc);
      i++;
   }
   rc = splinterdb_iterator_status(it);
   ASSERT_EQUAL(0, rc);

   splinterdb_iterator_deinit(it);
}

/*
 * Test case to exercise and verify that splinterdb iterator interfaces with a
 * non-NULL start key correctly sets up the start scan at the requested
 * initial key value.
 */
CTEST2(splinterdb_quick, test_splinterdb_iterator_with_startkey)
{
   const int            num_inserts = 50;
   splinterdb_iterator *it          = NULL;
   int                  rc          = insert_some_keys(num_inserts, data->kvsb);
   ASSERT_EQUAL(0, rc);

   char key[TEST_INSERT_KEY_LENGTH] = {0};

   for (int ictr = 0; ictr < num_inserts; ictr++) {

      // Initialize the i'th key
      snprintf(key, sizeof(key), key_fmt, ictr);
      rc = splinterdb_iterator_init(data->kvsb, &it, strlen(key), key);
      ASSERT_EQUAL(0, rc);

      bool is_valid = splinterdb_iterator_valid(it);
      ASSERT_TRUE(is_valid);

      // Scan should have been positioned at the i'th key
      rc = check_current_tuple(it, ictr);
      ASSERT_EQUAL(0, rc);

      splinterdb_iterator_deinit(it);
   }
}

/*
 * Test case to exercise splinterdb iterator with a non-NULL but non-existent
 * start-key. The iterator just starts at the first key, if any, after the
 * specified start-key.
 *  . If start-key > max-key, we will find no more keys to scan.
 *  . If start-key < min-key, we will start scan from 1st key in set.
 */
CTEST2(splinterdb_quick, test_splinterdb_iterator_with_non_existent_startkey)
{
   int                  rc = 0;
   splinterdb_iterator *it = NULL;

   const int num_inserts = 50;
   rc                    = insert_some_keys(num_inserts, data->kvsb);
   ASSERT_EQUAL(0, rc);

   // start-key > max-key ('key-50')
   char *key = "unknownKey";

   rc = splinterdb_iterator_init(data->kvsb, &it, strlen(key), key);

   // Iterator should be invalid, as lookup key is non-existent.
   bool is_valid = splinterdb_iterator_valid(it);
   ASSERT_FALSE(is_valid);

   splinterdb_iterator_deinit(it);

   // If you start with a key before min-key-value, scan will start from
   // 1st key inserted. (We do lexicographic comparison, so 'U' sorts
   // before 'key...', which is what key's format is.)
   key = "UnknownKey";
   rc  = splinterdb_iterator_init(data->kvsb, &it, strlen(key), key);
   ASSERT_EQUAL(0, rc);

   int ictr = 0;
   // Iterator should be initialized to 1st key inserted, if the supplied
   // start_key is not found, but below the min-key inserted.
   rc = check_current_tuple(it, ictr);
   ASSERT_EQUAL(0, rc);

   // Just to be sure, run through the set of keys, to cross-check that
   // we are getting all of them back in the right order.
   for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      rc = check_current_tuple(it, ictr);
      ASSERT_EQUAL(0, rc);
      ictr++;
   }
   // We should have iterated thru all the keys that were inserted
   ASSERT_EQUAL(num_inserts, ictr);

   if (it) {
      splinterdb_iterator_deinit(it);
   }
}

/*
 * Test case to exercise splinterdb iterator with a non-NULL but non-existent
 * start-key.  The data in this test case is loaded such that we have a
 * sequence of key values with gaps of 2 (i.e. 1, 4, 7, 10, ...).
 *
 * Then, there are basically 4 sub-cases we exercise here:
 *
 *  a) start-key exactly == min-key
 *  b) start-key < min-key
 *  c) start-key between some existing key values; (Choose 5, which should
 *      end up starting the scan at 7.)
 *  d) start-key beyond max-key (Scan should come out as invalid.)
 */
CTEST2(splinterdb_quick,
       test_splinterdb_iterator_with_missing_startkey_in_sequence)
{
   const int num_inserts = 50;
   // Should insert keys: 1, 4, 7, 10 13, 16, 19, ...
   int minkey = 1;
   int rc     = insert_keys(data->kvsb, minkey, num_inserts, 3);
   ASSERT_EQUAL(0, rc);

   char key[TEST_INSERT_KEY_LENGTH];

   // (a) Test iter_init with a key == the min-key
   snprintf(key, sizeof(key), key_fmt, minkey);

   splinterdb_iterator *it = NULL;
   rc = splinterdb_iterator_init(data->kvsb, &it, strlen(key), key);
   ASSERT_EQUAL(0, rc);

   bool is_valid = splinterdb_iterator_valid(it);
   ASSERT_TRUE(is_valid);

   // Iterator should be initialized to 1st key inserted, if the supplied
   // start_key is below min-key inserted thus far.
   int ictr = minkey;
   rc       = check_current_tuple(it, ictr);
   ASSERT_EQUAL(0, rc);

   splinterdb_iterator_deinit(it);

   // (b) Test iter_init with a value below the min-key-value.
   int kctr = (minkey - 1);

   snprintf(key, sizeof(key), key_fmt, kctr);

   rc = splinterdb_iterator_init(data->kvsb, &it, strlen(key), key);
   ASSERT_EQUAL(0, rc);

   is_valid = splinterdb_iterator_valid(it);
   ASSERT_TRUE(is_valid);

   // Iterator should be initialized to 1st key inserted, if the supplied
   // start_key is below min-key inserted thus far.
   ictr = minkey;
   rc   = check_current_tuple(it, ictr);
   ASSERT_EQUAL(0, rc);

   splinterdb_iterator_deinit(it);

   // (c) Test with a non-existent value between 2 valid key values.
   kctr = 5;
   snprintf(key, sizeof(key), key_fmt, kctr);

   rc = splinterdb_iterator_init(data->kvsb, &it, strlen(key), key);
   ASSERT_EQUAL(0, rc);

   is_valid = splinterdb_iterator_valid(it);
   ASSERT_TRUE(is_valid);

   // Iterator should be initialized to next key following kctr.
   ictr = 7;
   rc   = check_current_tuple(it, ictr);
   ASSERT_EQUAL(0, rc);

   splinterdb_iterator_deinit(it);

   // (d) Test with a non-existent value beyond max key value.
   //     iter_init should end up as being invalid.
   kctr = -1;
   snprintf(key, sizeof(key), key_fmt, kctr);

   rc = splinterdb_iterator_init(data->kvsb, &it, strlen(key), key);
   ASSERT_EQUAL(0, rc);

   is_valid = splinterdb_iterator_valid(it);
   ASSERT_FALSE(is_valid);

   if (it) {
      splinterdb_iterator_deinit(it);
   }
}

/*
 * Test case to verify the interfaces to close() and reopen() a KVS work
 * as expected. After reopening the KVS, we should be able to retrieve data
 * that was inserted in the previous open.
 */
CTEST2(splinterdb_quick, test_close_and_reopen)
{
   const char  *key     = "some-key";
   const size_t key_len = strlen(key);
   const char  *val     = "some-value";
   const size_t val_len = strlen(val);


   int rc = splinterdb_insert(data->kvsb, key_len, key, val_len, val);
   ASSERT_EQUAL(0, rc);

   // Close and re-open the database
   splinterdb_close(data->kvsb);
   rc = splinterdb_open(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   const char *value;
   size_t      result_val_len;

   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(data->kvsb, &result, 0, NULL);

   rc = splinterdb_lookup(data->kvsb, key_len, key, &result);
   ASSERT_EQUAL(0, rc);

   ASSERT_TRUE(splinterdb_lookup_found(&result));
   rc = splinterdb_lookup_result_value(
      data->kvsb, &result, &result_val_len, &value);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(val_len, result_val_len);
   ASSERT_STREQN(val,
                 value,
                 val_len,
                 "value found did not match expected 'val' up to %d bytes\n",
                 val_len);

   splinterdb_lookup_result_deinit(&result);
}

/*
 * Regression test for bug where repeating a cycle of insert-close-reopen
 * causes a space leak and eventually hits an assertion
 * (fixed in PR #214 / commit 8b33fd149d33054173790a8a30b99e97f08ffa81)
 */
CTEST2(splinterdb_quick, test_repeated_insert_close_reopen)
{
   char  *key     = "some-key";
   size_t key_len = strlen(key);
   char  *val     = "f";
   size_t val_len = strlen(val);

   for (int i = 0; i < 20; i++) {
      int rc = splinterdb_insert(data->kvsb, key_len, key, val_len, val);
      ASSERT_EQUAL(0, rc, "Insert is expected to pass, iter=%d.", i);

      splinterdb_close(data->kvsb);

      rc = splinterdb_open(&data->cfg, &data->kvsb);
      ASSERT_EQUAL(0, rc);
   }
}

// Check that the value-oriented functions work sensibly with a custom
// data_config
CTEST2(splinterdb_quick, test_custom_data_config)
{
   // We need to reconfigure Splinter with user-specified data_config
   // Tear down default instance, and create a new one.
   splinterdb_close(data->kvsb);
   data->cfg.data_cfg                 = test_data_config;
   data->cfg.data_cfg->key_size       = 20;
   data->cfg.data_cfg->max_key_length = 20;
   int rc = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   const size_t key_len = 3;
   const char  *key     = "foo";
   rc                   = splinterdb_insert(data->kvsb, key_len, key, 3, "bar");
   ASSERT_EQUAL(0, rc);

   // confirm its there
   splinterdb_lookup_result result;
   splinterdb_lookup_result_init(data->kvsb, &result, 0, NULL);
   rc = splinterdb_lookup(data->kvsb, key_len, key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));

   size_t      val_len;
   const char *val;
   rc = splinterdb_lookup_result_value(data->kvsb, &result, &val_len, &val);
   ASSERT_EQUAL(0, rc);
   ASSERT_EQUAL(3, val_len);
   ASSERT_EQUAL(0, memcmp(val, "bar", 3));

   // insert a message that adds to the refcount
   char         msg_buffer[sizeof(data_handle)] = {0};
   data_handle *msg                             = (data_handle *)msg_buffer;
   msg->message_type                            = MESSAGE_TYPE_UPDATE;
   msg->ref_count                               = 5;
   rc                                           = splinterdb_insert_raw_message(
      data->kvsb, key_len, key, sizeof(msg_buffer), msg_buffer);
   ASSERT_EQUAL(0, rc);

   // check still found
   rc = splinterdb_lookup(data->kvsb, key_len, key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_TRUE(splinterdb_lookup_found(&result));
   rc = splinterdb_lookup_result_value(data->kvsb, &result, &val_len, &val);
   ASSERT_EQUAL(0, rc);

   // insert a message that drops the refcount to zero
   msg->message_type = MESSAGE_TYPE_UPDATE;
   msg->ref_count    = -6;
   rc                = splinterdb_insert_raw_message(
      data->kvsb, key_len, key, sizeof(msg_buffer), msg_buffer);
   ASSERT_EQUAL(0, rc);

   // on lookup, merge will decide the tuple is deleted
   rc = splinterdb_lookup(data->kvsb, key_len, key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(splinterdb_lookup_found(&result));

   // add it back as a value
   rc = splinterdb_insert(data->kvsb, key_len, key, 3, "bar");
   ASSERT_EQUAL(0, rc);

   // delete it using a raw message
   msg->message_type = MESSAGE_TYPE_DELETE;
   rc                = splinterdb_insert_raw_message(
      data->kvsb, key_len, key, sizeof(msg_buffer), msg_buffer);
   ASSERT_EQUAL(0, rc);

   // on lookup, it should not be found
   rc = splinterdb_lookup(data->kvsb, key_len, key, &result);
   ASSERT_EQUAL(0, rc);
   ASSERT_FALSE(splinterdb_lookup_found(&result));
}

/*
 * Test case to exercise APIs using custom user-defined comparator function.
 *
 * NOTE: This test case is expected to be the last one in this suite as it
 *  reconfigures SplinterDB. All other cases that exercise the default
 *  configuration should precede this one.
 */
// A user-specified spy comparator
static int
custom_key_comparator(const data_config *cfg,
                      uint64             key1_len,
                      const void        *key1,
                      uint64             key2_len,
                      const void        *key2)
{
   platform_assert(key1 != NULL);
   platform_assert(key2 != NULL);

   int r =
      slice_lex_cmp(slice_create(key1_len, key1), slice_create(key2_len, key2));

   // record that this spy was called
   comparison_counting_data_config *ccdcfg =
      (comparison_counting_data_config *)cfg;
   ccdcfg->num_comparisons++;
   return r;
}

CTEST2(splinterdb_quick, test_iterator_custom_comparator)
{
   // We need to reconfigure Splinter with user-specified key comparator fn.
   // Tear down default instance, and create a new one.
   splinterdb_close(data->kvsb);

   data->default_data_cfg.super.key_compare = custom_key_comparator;
   data->default_data_cfg.num_comparisons   = 0;

   int rc = splinterdb_create(&data->cfg, &data->kvsb);
   ASSERT_EQUAL(0, rc);

   const int num_inserts = 50;

   rc = insert_some_keys(num_inserts, data->kvsb);
   ASSERT_EQUAL(0, rc);

   splinterdb_iterator *it = NULL;
   rc                      = splinterdb_iterator_init(data->kvsb, &it, 0, NULL);
   ASSERT_EQUAL(0, rc);

   int i = 0;
   for (; splinterdb_iterator_valid(it); splinterdb_iterator_next(it)) {
      rc = check_current_tuple(it, i);
      ASSERT_EQUAL(0, rc);
      i++;
   }

   rc = splinterdb_iterator_status(it);
   ASSERT_EQUAL(0, rc);

   // Expect that iterator has stopped at num_inserts
   ASSERT_EQUAL(num_inserts, i);
   ASSERT_TRUE(data->default_data_cfg.num_comparisons > (2 * num_inserts));

   bool is_valid = splinterdb_iterator_valid(it);
   ASSERT_FALSE(is_valid);

   if (it) {
      splinterdb_iterator_deinit(it);
   }
}

/*
 * ********************************************************************************
 * Define minions and helper functions here, after all test cases are
 * enumerated.
 * ********************************************************************************
 */

static void
create_default_cfg(splinterdb_config *out_cfg, data_config *default_data_cfg)
{
   *out_cfg = (splinterdb_config){.filename   = TEST_DB_NAME,
                                  .cache_size = 64 * Mega,
                                  .disk_size  = 127 * Mega,
                                  .data_cfg   = default_data_cfg};
}

/*
 * Helper function to insert n-keys (num_inserts), using pre-formatted
 * key and value strings.
 *
 * Returns: Return code: rc == 0 => success; anything else => failure
 */
static int
insert_some_keys(const int num_inserts, splinterdb *kvsb)
{
   int rc = 0;
   // insert keys backwards, just for kicks
   for (int i = num_inserts - 1; i >= 0; i--) {
      char key[TEST_INSERT_KEY_LENGTH] = {0};
      char val[TEST_INSERT_VAL_LENGTH] = {0};

      ASSERT_EQUAL(6, snprintf(key, sizeof(key), key_fmt, i));
      ASSERT_EQUAL(6, snprintf(val, sizeof(val), val_fmt, i));

      rc = splinterdb_insert(kvsb, sizeof(key), key, sizeof(val), val);
      ASSERT_EQUAL(0, rc);
   }

   return rc;
}

/*
 * Helper function to insert n-keys (num_inserts), using pre-formatted
 * key and value strings. Allows user to specify start value and increment
 * between keys. This can be used to load either fully sequential keys
 * or some with defined gaps.
 *
 * Parameters:
 *  kvsb    - SplinterDB handle
 *  minkey  - Start key to insert
 *  numkeys - # of keys to insert
 *  incr    - Increment between keys (default is 1)
 *
 * Returns: Return code: rc == 0 => success; anything else => failure
 */
static int
insert_keys(splinterdb *kvsb, const int minkey, int numkeys, const int incr)
{
   int rc = -1;

   // Minimally, error check input arguments
   if (!kvsb || (numkeys <= 0) || (incr < 0))
      return rc;

   // insert keys forwards, starting from minkey value
   for (int kctr = minkey; numkeys; kctr += incr, numkeys--) {
      char key[TEST_INSERT_KEY_LENGTH] = {0};
      char val[TEST_INSERT_VAL_LENGTH] = {0};

      snprintf(key, sizeof(key), key_fmt, kctr);
      snprintf(val, sizeof(val), val_fmt, kctr);

      rc = splinterdb_insert(kvsb, sizeof(key), key, sizeof(val), val);
      ASSERT_EQUAL(0, rc);
   }
   return rc;
}

/*
 * Work horse routine to check if the current tuple pointed to by the
 * iterator is the expected one, as indicated by its index,
 * expected_i. We use pre-constructed key / value formats to verify
 * if the current tuple is of the expected format.
 *
 * Returns: Return code: rc == 0 => success; anything else => failure
 */
static int
check_current_tuple(splinterdb_iterator *it, const int expected_i)
{
   int rc = 0;

   char expected_key[MAX_KEY_SIZE]        = {0};
   char expected_val[TEST_MAX_VALUE_SIZE] = {0};
   ASSERT_EQUAL(
      6, snprintf(expected_key, sizeof(expected_key), key_fmt, expected_i));
   ASSERT_EQUAL(
      6, snprintf(expected_val, sizeof(expected_val), val_fmt, expected_i));

   const char *key;
   const char *val;
   size_t      key_len, val_len;

   splinterdb_iterator_get_current(it, &key_len, &key, &val_len, &val);

   ASSERT_EQUAL(TEST_INSERT_KEY_LENGTH, key_len);
   ASSERT_EQUAL(TEST_INSERT_VAL_LENGTH, val_len);

   int key_cmp = memcmp(expected_key, key, key_len);
   int val_cmp = memcmp(expected_val, val, val_len);
   ASSERT_EQUAL(0, key_cmp);
   ASSERT_EQUAL(0, val_cmp);

   return rc;
}
