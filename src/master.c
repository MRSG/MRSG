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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "common.h"
#include "worker.h"
#include "dfs.h"

XBT_LOG_EXTERNAL_DEFAULT_CATEGORY (msg_test);

static FILE*       tasks_log;

static void print_config (void);
static void print_stats (void);
static int is_straggler (msg_host_t worker);
static int task_time_elapsed (msg_task_t task);
static void set_speculative_tasks (msg_host_t worker);
static void send_scheduler_task (enum phase_e phase, size_t wid);
static void update_stats (enum task_type_e task_type);
static void send_task (enum phase_e phase, size_t tid, size_t data_src, size_t wid);
char* task_type_string (enum task_type_e task_type);
static void finish_all_task_copies (task_info_t ti);


/** @brief  Main master function. */
int master (int argc, char* argv[])
{
    heartbeat_t  heartbeat;
    msg_error_t  status;
    msg_host_t   worker;
    msg_task_t   msg = NULL;
    size_t       wid;
    task_info_t  ti;

    print_config ();
    XBT_INFO ("JOB BEGIN"); XBT_INFO (" ");

    tasks_log = fopen ("tasks.csv", "w");
    fprintf (tasks_log, "task_id,phase,worker_id,time,action,shuffle_end\n");

    while (job.tasks_pending[MAP] + job.tasks_pending[REDUCE] > 0)
    {
	msg = NULL;
	status = receive (&msg, MASTER_MAILBOX);
	if (status == MSG_OK)
	{
	    worker = MSG_task_get_source (msg);
	    wid = get_worker_id (worker);

	    if (message_is (msg, SMS_HEARTBEAT))
	    {
		heartbeat = &job.heartbeats[wid];

		if (is_straggler (worker))
		{
		    set_speculative_tasks (worker);
		}
		else
		{
		    if (heartbeat->slots_av[MAP] > 0)
			send_scheduler_task(MAP, wid);

		    if (heartbeat->slots_av[REDUCE] > 0)
			send_scheduler_task(REDUCE, wid);
		}
	    }
	    else if (message_is (msg, SMS_TASK_DONE))
	    {
		ti = (task_info_t) MSG_task_get_data (msg);

		if (job.task_status[ti->phase][ti->id] != T_STATUS_DONE)
		{
		    job.task_status[ti->phase][ti->id] = T_STATUS_DONE;
		    finish_all_task_copies (ti);
		    job.tasks_pending[ti->phase]--;
		    if (job.tasks_pending[ti->phase] <= 0)
		    {
			XBT_INFO (" ");
			XBT_INFO ("%s PHASE DONE", (ti->phase==MAP?"MAP":"REDUCE"));
			XBT_INFO (" ");
		    }
		}
		xbt_free_ref (&ti);
	    }
	    MSG_task_destroy (msg);
	}
    }

    fclose (tasks_log);

    job.finished = 1;

    print_config ();
    print_stats ();
    XBT_INFO ("JOB END");

    return 0;
}

/** @brief  Print the job configuration. */
static void print_config (void)
{
    XBT_INFO ("JOB CONFIGURATION:");
    XBT_INFO ("slots: %d map, %d reduce", config.slots[MAP], config.slots[REDUCE]);
    XBT_INFO ("chunk replicas: %d", config.chunk_replicas);
    XBT_INFO ("chunk size: %.0f MB", config.chunk_size/1024/1024);
    XBT_INFO ("input chunks: %d", config.chunk_count);
    XBT_INFO ("input size: %d MB", config.chunk_count * (int)(config.chunk_size/1024/1024));
    XBT_INFO ("maps: %d", config.amount_of_tasks[MAP]);
    XBT_INFO ("reduces: %d", config.amount_of_tasks[REDUCE]);
    XBT_INFO ("workers: %d", config.number_of_workers);
    XBT_INFO ("grid power: %g flops", config.grid_cpu_power);
    XBT_INFO ("average power: %g flops/s", config.grid_average_speed);
    XBT_INFO ("heartbeat interval: %ds", config.heartbeat_interval);
    XBT_INFO (" ");
}

/** @brief  Print job statistics. */
static void print_stats (void)
{
    XBT_INFO ("JOB STATISTICS:");
    XBT_INFO ("local maps: %d", stats.map_local);
    XBT_INFO ("non-local maps: %d", stats.map_remote);
    XBT_INFO ("speculative maps (local): %d", stats.map_spec_l);
    XBT_INFO ("speculative maps (remote): %d", stats.map_spec_r);
    XBT_INFO ("total non-local maps: %d", stats.map_remote + stats.map_spec_r);
    XBT_INFO ("total speculative maps: %d", stats.map_spec_l + stats.map_spec_r);
    XBT_INFO ("normal reduces: %d", stats.reduce_normal);
    XBT_INFO ("speculative reduces: %d", stats.reduce_spec);
    XBT_INFO (" ");
}

/**
 * @brief  Checks if a worker is a straggler.
 * @param  worker  The worker to be probed.
 * @return 1 if true, 0 if false.
 */
static int is_straggler (msg_host_t worker)
{
    int     task_count;
    size_t  wid;

    wid = get_worker_id (worker);

    task_count = (config.slots[MAP] + config.slots[REDUCE]) - (job.heartbeats[wid].slots_av[MAP] + job.heartbeats[wid].slots_av[REDUCE]);

    if (MSG_get_host_speed (worker) < config.grid_average_speed && task_count > 0)
	return 1;

    return 0;
}

/**
 * @brief  Returns for how long a task is running.
 * @param  task  The task to be probed.
 * @return The amount of seconds since the beginning of the computation.
 */
static int task_time_elapsed (msg_task_t task)
{
    task_info_t  ti;

    ti = (task_info_t) MSG_task_get_data (task);

    return (MSG_task_get_compute_duration (task) - MSG_task_get_remaining_computation (task))
	/ MSG_get_host_speed (config.workers[ti->wid]);
}

/**
 * @brief  Mark the tasks of a straggler as possible speculative tasks.
 * @param  worker  The straggler worker.
 */
static void set_speculative_tasks (msg_host_t worker)
{
    size_t       tid;
    size_t       wid;
    task_info_t  ti;

    wid = get_worker_id (worker);

    if (job.heartbeats[wid].slots_av[MAP] < config.slots[MAP])
    {
	for (tid = 0; tid < config.amount_of_tasks[MAP]; tid++)
	{
	    if (job.task_list[MAP][tid][0] != NULL)
	    {
		ti = (task_info_t) MSG_task_get_data (job.task_list[MAP][tid][0]);
		if (ti->wid == wid && task_time_elapsed (job.task_list[MAP][tid][0]) > 60)
		{
		    job.task_status[MAP][tid] = T_STATUS_TIP_SLOW;
		}
	    }
	}
    }

    if (job.heartbeats[wid].slots_av[REDUCE] < config.slots[REDUCE])
    {
	for (tid = 0; tid < config.amount_of_tasks[REDUCE]; tid++)
	{
	    if (job.task_list[REDUCE][tid][0] != NULL)
	    {
		ti = (task_info_t) MSG_task_get_data (job.task_list[REDUCE][tid][0]);
		if (ti->wid == wid && task_time_elapsed (job.task_list[REDUCE][tid][0]) > 60)
		{
		    job.task_status[REDUCE][tid] = T_STATUS_TIP_SLOW;
		}
	    }
	}
    }
}

static void send_scheduler_task (enum phase_e phase, size_t wid)
{
    size_t tid = user.scheduler_f (phase, wid);

    if (tid == NONE)
    {
	return;
    }

    enum task_type_e task_type = get_task_type (phase, tid, wid);
    size_t	     sid = NONE;

    if (task_type == LOCAL || task_type == LOCAL_SPEC)
    {
	sid = wid;
    }
    else if (task_type == REMOTE || task_type == REMOTE_SPEC)
    {
	sid = find_random_chunk_owner (tid);
    }

    XBT_INFO ("%s %zu assigned to %s %s", (phase==MAP?"map":"reduce"), tid,
	    MSG_host_get_name (config.workers[wid]),
	    task_type_string (task_type));

    send_task (phase, tid, sid, wid);

    update_stats (task_type);
}

enum task_type_e get_task_type (enum phase_e phase, size_t tid, size_t wid)
{
    enum task_status_e task_status = job.task_status[phase][tid];

    switch (phase)
    {
	case MAP:
	    switch (task_status)
	    {
		case T_STATUS_PENDING:
		    return chunk_owner[tid][wid]? LOCAL : REMOTE;

		case T_STATUS_TIP_SLOW:
		    return chunk_owner[tid][wid]? LOCAL_SPEC : REMOTE_SPEC;

		default:
		    return NO_TASK;
	    }

	case REDUCE:
	    switch (task_status)
	    {
		case T_STATUS_PENDING:
		    return NORMAL;

		case T_STATUS_TIP_SLOW:
		    return SPECULATIVE;

		default:
		    return NO_TASK;
	    }

	default:
	    return NO_TASK;
    }
}

/**
 * @brief  Send a task to a worker.
 * @param  phase     The current job phase.
 * @param  tid       The task ID.
 * @param  data_src  The ID of the DataNode that owns the task data.
 * @param  wid       The destination worker id.
 */
static void send_task (enum phase_e phase, size_t tid, size_t data_src, size_t wid)
{
    char         mailbox[MAILBOX_ALIAS_SIZE];
    int          i;
    double       cpu_required = 0.0;
    msg_task_t   task = NULL;
    task_info_t  task_info;

    cpu_required = user.task_cost_f (phase, tid, wid);

    task_info = xbt_new (struct task_info_s, 1);
    task = MSG_task_create (SMS_TASK, cpu_required, 0.0, (void*) task_info);

    task_info->phase = phase;
    task_info->id = tid;
    task_info->src = data_src;
    task_info->wid = wid;
    task_info->task = task;
    task_info->shuffle_end = 0.0;

    // for tracing purposes...
    MSG_task_set_category (task, (phase==MAP?"MAP":"REDUCE"));

    if (job.task_status[phase][tid] != T_STATUS_TIP_SLOW)
	job.task_status[phase][tid] = T_STATUS_TIP;

    job.heartbeats[wid].slots_av[phase]--;

    for (i = 0; i < MAX_SPECULATIVE_COPIES; i++)
    {
	if (job.task_list[phase][tid][i] == NULL)
	{
	    job.task_list[phase][tid][i] = task;
	    break;
	}
    }

    fprintf (tasks_log, "%d_%zu_%d,%s,%zu,%.3f,START,\n", phase, tid, i, (phase==MAP?"MAP":"REDUCE"), wid, MSG_get_clock ());

#ifdef VERBOSE
    XBT_INFO ("TX: %s > %s", SMS_TASK, MSG_host_get_name (config.workers[wid]));
#endif

    sprintf (mailbox, TASKTRACKER_MAILBOX, wid);
    xbt_assert (MSG_task_send (task, mailbox) == MSG_OK, "ERROR SENDING MESSAGE");

    job.task_instances[phase][tid]++;
}

static void update_stats (enum task_type_e task_type)
{
    switch (task_type)
    {
	case LOCAL:
	    stats.map_local++;
	    break;

	case REMOTE:
	    stats.map_remote++;
	    break;

	case LOCAL_SPEC:
	    stats.map_spec_l++;
	    break;

	case REMOTE_SPEC:
	    stats.map_spec_r++;
	    break;

	case NORMAL:
	    stats.reduce_normal++;
	    break;

	case SPECULATIVE:
	    stats.reduce_spec++;
	    break;

	default:
	    return;
    }
}

char* task_type_string (enum task_type_e task_type)
{
    switch (task_type)
    {
	case REMOTE:
	    return "(non-local)";

	case LOCAL_SPEC:
	case SPECULATIVE:
	    return "(speculative)";

	case REMOTE_SPEC:
	    return "(non-local, speculative)";

	default:
	    return "";
    }
}

/**
 * @brief  Kill all copies of a task.
 * @param  ti  The task information of any task instance.
 */
static void finish_all_task_copies (task_info_t ti)
{
    int     i;
    int     phase = ti->phase;
    size_t  tid = ti->id;

    for (i = 0; i < MAX_SPECULATIVE_COPIES; i++)
    {
	if (job.task_list[phase][tid][i] != NULL)
	{
	    MSG_task_cancel (job.task_list[phase][tid][i]);
	    //FIXME: MSG_task_destroy (job.task_list[phase][tid][i]);
	    job.task_list[phase][tid][i] = NULL;
	    fprintf (tasks_log, "%d_%zu_%d,%s,%zu,%.3f,END,%.3f\n", ti->phase, tid, i, (ti->phase==MAP?"MAP":"REDUCE"), ti->wid, MSG_get_clock (), ti->shuffle_end);
	}
    }
}

// vim: set ts=8 sw=4:
