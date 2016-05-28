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

////////////////////////////////////////////////////////////////////////////////
/// Bridge functions called from C back to Go.
////////////////////////////////////////////////////////////////////////////////
static int input_inject_message(void *parent, const char *pb, size_t pb_len,
                                double cp_numeric, const char *cp_string)
{
    return go_lua_inject_message(parent, (char*)pb, pb_len);
}

static int inject_message(void *parent, const char *pb, size_t pb_len)
{
    return go_lua_inject_message(parent, (char*)pb, pb_len);
}

static int output_update_checkpoint(void *parent, void *sequence_id)
{
    return go_lua_output_update_checkpoint(parent, sequence_id);
}

void heka_log(void *context,
              const char *component,
              int level,
              const char *fmt,
              ...)
{
    const char *severity;
    switch (level) {
    case 7:
        severity = "debug";
        break;
    case 6:
        severity = "info";
        break;
    case 5:
        severity = "notice";
        break;
    case 4:
        severity = "warning";
        break;
    case 3:
        severity = "error";
        break;
    case 2:
        severity = "crit";
        break;
    case 1:
        severity = "alert";
        break;
    case 0:
        severity = "panic";
        break;
    default:
        severity = "debug";
        break;
    }

    char var_str[500];
    va_list args;
    va_start(args, fmt);
    vsnprintf(var_str, 500, fmt, args);
    va_end(args);

    int len = strlen(var_str) + strlen(severity) + 5;
    char str[len];
    snprintf(str, len, "[%s] %s\n", severity, var_str);
    go_lua_log(str);
}

lsb_logger logger = {.context = NULL, .cb = heka_log};

////////////////////////////////////////////////////////////////////////////////
/// Bridge functions called from Go into C.
////////////////////////////////////////////////////////////////////////////////
lsb_heka_sandbox* heka_create_sandbox(void *parent,
                                      const int sbx_type,
                                      const char *lua_file,
                                      const char *state_file,
                                      const char *lsb_cfg)
{
    lsb_heka_sandbox *sbx;
    switch (sbx_type) {
    case SBX_TYPE_INPUT:
        sbx = lsb_heka_create_input(parent, lua_file, state_file, lsb_cfg, &logger,
                                    input_inject_message);
        break;
    case SBX_TYPE_ANALYSIS:
        sbx = lsb_heka_create_analysis(parent, lua_file, state_file, lsb_cfg, &logger,
                                       inject_message);
        break;
    case SBX_TYPE_OUTPUT:
        sbx = lsb_heka_create_output(parent, lua_file, state_file, lsb_cfg, &logger,
                                     output_update_checkpoint);
        break;
    }
    return sbx;
}

int heka_analysis_process_message(lsb_heka_sandbox* hsb, const char* pb, int pblen)
{
    if (!hsb) return 1;

    lsb_heka_message m;
    lsb_init_heka_message(&m, 2);
    bool rv = lsb_decode_heka_message(&m, pb, pblen, &logger);
    if (!rv) return 1;
    return lsb_heka_pm_analysis(hsb, &m, 0);
}

int heka_output_process_message(lsb_heka_sandbox* hsb, const char* pb, int pblen,
                                void* sequence_id)
{
    if (!hsb) return 1;

    lsb_heka_message m;
    lsb_init_heka_message(&m, 2);
    bool rv = lsb_decode_heka_message(&m, pb, pblen, &logger);
    if (!rv) return 1;
    return lsb_heka_pm_output(hsb, &m, sequence_id, 0);
}
