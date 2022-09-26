// Copyright 2021 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * -----------------------------------------------------------------------------
 * splinter_shmem_test.c --
 *
 *  Exercises the interfaces in SplinterDB shared memory mgmt module.
 *  This is just a template file. Fill it out for your specific test suite.
 * -----------------------------------------------------------------------------
 */
#include "splinterdb/public_platform.h"
#include "platform.h"
#include "unit_tests.h"
#include "ctest.h" // This is required for all test-case files.
#include "shmem.h"

/*
 * Global data declaration macro:
 */
CTEST_DATA(splinter_shmem)
{
   // Declare heap handles to shake out shared memory based allocation.
   size_t               shmem_capacity; // In bytes
   platform_heap_handle hh;
   platform_heap_id     hid;
};

// By default, all test cases will deal with small shared memory segment.
CTEST_SETUP(splinter_shmem)
{
   data->shmem_capacity = (256 * MiB); // bytes
   platform_status rc   = platform_heap_create(platform_get_module_id(),
                                             data->shmem_capacity,
                                             TRUE,
                                             &data->hh,
                                             &data->hid);
   ASSERT_TRUE(SUCCESS(rc));
}

// Tear down the test shared segment.
CTEST_TEARDOWN(splinter_shmem)
{
   platform_heap_destroy(&data->hh);
}

/*
 * Basic test case. This goes through the basic create / destroy
 * interfaces to setup a shared memory segment. While at it, run through
 * few lookup interfaces to validate sizes.
 */
CTEST2(splinter_shmem, test_create_destroy_shmem)
{
   platform_heap_handle hh            = NULL;
   platform_heap_id     hid           = NULL;
   size_t               requested     = (512 * MiB); // bytes
   size_t               heap_capacity = requested;
   platform_status      rc            = platform_heap_create(
      platform_get_module_id(), heap_capacity, TRUE, &hh, &hid);
   ASSERT_TRUE(SUCCESS(rc));

   // Total size of shared segment must be what requested for.
   ASSERT_EQUAL(platform_shmsize(hid), requested);

   // A small chunk at the head is used for shmem_info{} tracking struct
   ASSERT_EQUAL(platform_shmfree(hid),
                (requested - platform_shm_ctrlblock_size()));

   // Destroy shared memory and release memory.
   platform_shmdestroy(&hh);
   ASSERT_TRUE(hh == NULL);
}

/*
 * Test that used space and pad-bytes tracking is happening correctly
 * when all allocation requests are fully aligned. No pad bytes should
 * have been generated for alignment.
 */
CTEST2(splinter_shmem, test_aligned_allocations)
{
   int    keybuf_size = 64;
   int    msgbuf_size = 128;
   uint8 *keybuf      = TYPED_MALLOC_MANUAL(data->hid, keybuf, keybuf_size);
   uint8 *msgbuf      = TYPED_MALLOC_MANUAL(data->hid, msgbuf, msgbuf_size);

   // Sum of requested alloc-sizes == total # of used-bytes
   ASSERT_EQUAL((keybuf_size + msgbuf_size), platform_shmused(data->hid));

   // Free bytes left in shared segment == (sum of requested alloc sizes, less
   // a small bit of the control block.)
   ASSERT_EQUAL((data->shmem_capacity
                 - (keybuf_size + msgbuf_size + platform_shm_ctrlblock_size())),
                platform_shmfree(data->hid));
}
