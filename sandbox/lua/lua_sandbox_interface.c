/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/// @brief Sandboxed Lua execution @file
#include <ctype.h>
#include <luasandbox.h>
#include <luasandbox/util/running_stats.h>
#include <luasandbox/util/heka_message.h>
#include <luasandbox/util/output_buffer.h>
#include <luasandbox/heka/sandbox.h>
#include <luasandbox/lua.h>
#include <luasandbox/lualib.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "_cgo_export.h"

struct lsb_lua_sandbox {
  lua_State         *lua;
  void              *parent;
  char              *lua_file;
  char              *state_file;
  lsb_logger        logger;
  lsb_state         state;
  lsb_output_buffer output;
  size_t            usage[LSB_UT_MAX][LSB_US_MAX];
  char              error_message[LSB_ERROR_SIZE];
};

struct heka_stats {
  unsigned long long im_cnt;
  unsigned long long im_bytes;

  unsigned long long pm_cnt;
  unsigned long long pm_failures;

  lsb_running_stats pm;
  lsb_running_stats te;
};


struct lsb_heka_sandbox {
  void                              *parent;
  lsb_lua_sandbox                   *lsb;
  lsb_heka_message                  *msg;
  char                              *name;
  char                              *hostname;
  union {
    lsb_heka_im_input     iim;
    lsb_heka_im_analysis  aim;
    lsb_heka_update_checkpoint        ucp;
  } cb;
  char                              type;
  struct heka_stats                 stats;
};
////////////////////////////////////////////////////////////////////////////////
/// Heka bridged calls into hsb and lsb sandbox.
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
/// Initialization calls from Go
////////////////////////////////////////////////////////////////////////////////
static int inject_message(void *parent, const char *pb, size_t pb_len)
{
    char *pb_copy = pb;  // Probably need to actually do a memcopy here.
    return go_lua_inject_message(parent, pb_copy, pb_len);
}

lsb_heka_sandbox* heka_create_sandbox(void *parent,
                                      const char *lua_file,
                                      const char *state_file,
                                      const char *lsb_cfg,
                                      lsb_logger *logger)
{
    lsb_heka_create_analysis(parent, lua_file, state_file, lsb_cfg, logger,
                             inject_message);
}


lsb_state heka_lsb_get_state(lsb_heka_sandbox* hsb)
{
    if (!hsb) return LSB_UNKNOWN;
    return lsb_get_state(hsb->lsb);
}

const char* heka_lsb_get_error(lsb_heka_sandbox* hsb)
{
    return hsb ? hsb->lsb->error_message : "";
}

size_t heka_lsb_usage(lsb_heka_sandbox* hsb, lsb_usage_type utype, lsb_usage_stat ustat)
{
    return hsb ? lsb_usage(hsb->lsb, utype, ustat) : 0;
}

int heka_process_message(lsb_heka_sandbox* hsb, const char* pb)
{
    if (!hsb) return 1;

    lsb_heka_message m;
    lsb_init_heka_message(&m, 2);
    bool rv = lsb_decode_heka_message(&m, pb, sizeof(pb)-1, NULL);
    if (!rv) return 1;
    return lsb_heka_pm_analysis(hsb, &m, 0);
}
