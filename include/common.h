/* Copyright (c) 2012. MRSG Team. All rights reserved. */

/* This file is part of MRSG.

MRSG is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

MRSG is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with MRSG.  If not, see <http://www.gnu.org/licenses/>. */

#ifndef MRSG_COMMON_H
#define MRSG_COMMON_H

#include <msg/msg.h>
#include <xbt/sysdep.h>
#include <xbt/log.h>
#include <xbt/asserts.h>
#include "mrsg.h"

/* Hearbeat parameters. */
#define HEARTBEAT_MIN_INTERVAL 3
#define HEARTBEAT_TIMEOUT 600

/* Short message names. */
#define SMS_GET_CHUNK "SMS-GC"
#define SMS_GET_INTER_PAIRS "SMS-GIP"
#define SMS_HEARTBEAT "SMS-HB"
#define SMS_TASK "SMS-T"
#define SMS_TASK_DONE "SMS-TD"
#define SMS_FINISH "SMS-F"

#define NONE (-1)
#define MAX_SPECULATIVE_COPIES 3

/* Mailbox related. */
#define MAILBOX_ALIAS_SIZE 256
#define MASTER_MAILBOX "MASTER"
#define DATANODE_MAILBOX "%zu:DN"
#define TASKTRACKER_MAILBOX "%zu:TT"
#define TASK_MAILBOX "%zu:%d"

/** @brief  Possible task status. */
enum task_status_e {
    /* The initial status must be the first enum. */
    T_STATUS_PENDING,
    T_STATUS_TIP,
    T_STATUS_TIP_SLOW,
    T_STATUS_DONE
};

/** @brief  Information sent by the workers with every heartbeat. */
struct heartbeat_s {
    int  slots_av[2];
};

typedef struct heartbeat_s* heartbeat_t;

struct config_s {
    double         chunk_size;
    double         grid_average_speed;
    double         grid_cpu_power;
    int            chunk_count;
    int            chunk_replicas;
    int            heartbeat_interval;
    int            amount_of_tasks[2];
    int            number_of_workers;
    int            slots[2];
    int            initialized;
    msg_host_t*    workers;
} config;

struct job_s {
    int           finished;
    int           tasks_pending[2];
    int*          task_instances[2];
    int*          task_status[2];
    msg_task_t**  task_list[2];
    size_t**      map_output;
    heartbeat_t   heartbeats;
} job;

/** @brief  Information sent as the task data. */
struct task_info_s {
    enum phase_e  phase;
    size_t        id;
    size_t        src;
    size_t        wid;
    int           pid;
    msg_task_t    task;
    size_t*       map_output_copied;
    double        shuffle_end;
};

typedef struct task_info_s* task_info_t;

struct stats_s {
    int   map_local;
    int   map_remote;
    int   map_spec_l;
    int   map_spec_r;
    int   reduce_normal;
    int   reduce_spec;
    int*  maps_processed;
    int*  reduces_processed;
} stats;

struct user_s {
    double (*task_cost_f)(enum phase_e phase, size_t tid, size_t wid);
    void (*dfs_f)(char** dfs_matrix, size_t chunks, size_t workers, int replicas);
    int (*map_output_f)(size_t mid, size_t rid);
} user;


/** 
 * @brief  Send a message/task.
 * @param  str      The message.
 * @param  cpu      The amount of cpu required by the task.
 * @param  net      The message size in bytes.
 * @param  data     Any data to attatch to the message.
 * @param  mailbox  The destination mailbox alias.
 * @return The MSG status of the operation.
 */
msg_error_t send (const char* str, double cpu, double net, void* data, const char* mailbox);

/** 
 * @brief  Send a short message, of size zero.
 * @param  str      The message.
 * @param  mailbox  The destination mailbox alias.
 * @return The MSG status of the operation.
 */
msg_error_t send_sms (const char* str, const char* mailbox);

/** 
 * @brief  Receive a message/task from a mailbox.
 * @param  msg      Where to store the received message.
 * @param  mailbox  The mailbox alias.
 * @return The status of the transfer.
 */
msg_error_t receive (msg_task_t* msg, const char* mailbox);

/** 
 * @brief  Compare the message from a task with a string.
 * @param  msg  The message/task.
 * @param  str  The string to compare with.
 * @return A positive value if matches, zero if doesn't.
 */
int message_is (msg_task_t msg, const char* str);

/**
 * @brief  Return the maximum of two values.
 */
int maxval (int a, int b);

size_t map_output_size (size_t mid);

size_t reduce_input_size (size_t rid);

#endif /* !MRSG_COMMON_H */
