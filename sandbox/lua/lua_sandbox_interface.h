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

// Enumerate sandbox types for switching.
#define SBX_TYPE_INPUT 0
#define SBX_TYPE_ANALYSIS 1
#define SBX_TYPE_OUTPUT 2

// LMW_ERR_*: Lua Message Write errors.
extern const int LMW_ERR_NO_SANDBOX_PACK;
extern const int LMW_ERR_WRONG_TYPE;
extern const int LMW_ERR_NEWFIELD_FAILED;
extern const int LMW_ERR_BAD_FIELD_INDEX;
extern const int LMW_ERR_BAD_ARRAY_INDEX;
extern const int LMW_ERR_INVALID_FIELD_NAME;

/**
* Creates a Heka sandbox for the execution of a provided Lua file.
*
* @param parent Opaque pointer to host object which owns this sandbox.
* @param lua_file String containing filesystem path to Lua file to be loaded.
* @param state_file String containing filesystem path to Lua sandbox state
*                   to be loaded, if any.
* @param lsb_cfg Lua string containing sandbox plugin configuration.
*
* @return Pointer to newly created lsb_heka_sandbox instance.
*/
lsb_heka_sandbox* heka_create_sandbox(void *parent,
                                      const int sbx_type,
                                      const char *lua_file,
                                      const char *state_file,
                                      const char *lsb_cfg);


/**
* Passes a Heka message down to an analysis sandbox for processing. The
* instruction count limits are active during this call.
*
* @param hsb Pointer to the Heka sandbox.
* @param pb Pointer to protobuf encoding of Heka message.
* @param pblen Length in bytes of protobuf encoding.
*
* @return int Zero on success, non-zero on failure.
*/
int heka_analysis_process_message(lsb_heka_sandbox* hsb, const char* pb, int pblen);

/**
* Passes a Heka message down to an output sandbox for processing. The
* instruction count limits are active during this call.
*
* @param hsb Pointer to the Heka sandbox.
* @param pb Pointer to protobuf encoding of Heka message.
* @param pblen Length in bytes of protobuf encoding.
* @param sequence_id Opaque pointer to a checkpoint identifier for async / batch processing.
*
* @return int Zero on success, non-zero on failure.
*/
int heka_output_process_message(lsb_heka_sandbox* hsb, const char* pb, int pblen,
                                  void* sequence_id);
#endif
