/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/// Heka Go interfaces for the Lua sandbox @file
#ifndef lua_sandbox_interface_
#define lua_sandbox_interface_

#include <luasandbox.h>
#include <luasandbox/util/running_stats.h>
#include <luasandbox/util/heka_message.h>
#include <luasandbox/util/output_buffer.h>
#include <luasandbox/heka/sandbox.h>
#include <luasandbox/lua.h>
#include <luasandbox/lualib.h>

// LMW_ERR_*: Lua Message Write errors
extern const int LMW_ERR_NO_SANDBOX_PACK;
extern const int LMW_ERR_WRONG_TYPE;
extern const int LMW_ERR_NEWFIELD_FAILED;
extern const int LMW_ERR_BAD_FIELD_INDEX;
extern const int LMW_ERR_BAD_ARRAY_INDEX;
extern const int LMW_ERR_INVALID_FIELD_NAME;

lsb_state heka_lsb_get_state(lsb_heka_sandbox* hsb);

const char* heka_lsb_get_error(lsb_heka_sandbox* hsb);

size_t heka_lsb_usage(lsb_heka_sandbox* hsb, lsb_usage_type utype, lsb_usage_stat ustat);

lsb_heka_sandbox* heka_create_sandbox(void *parent,
                                      const char *lua_file,
                                      const char *state_file,
                                      const char *lsb_cfg);

/**
* Passes a Heka message down to the sandbox for processing. The instruction
* count limits are active during this call.
*
* @param hsb Pointer to the Heka sandbox
* @param pb Pointer to protobuf encoding of Heka message
*
* @return int Zero on success, non-zero on failure.
*/
int heka_process_message(lsb_heka_sandbox* hsb, const char* pb, int pblen);
#endif

