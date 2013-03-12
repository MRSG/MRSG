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
#include "dfs.h"

XBT_LOG_EXTERNAL_DEFAULT_CATEGORY (msg_test);

static FILE*       tasks_log;

static void print_config (void);
static void print_stats (void);
static int is_straggler (msg_host_t worker);
static int task_time_elapsed (msg_task_t task);
static void set_speculative_tasks (msg_host_t worker);
static void send_map_to_worker (msg_host_t dest);
static void send_reduce_to_worker (msg_host_t dest);
static void send_task (enum phase_e phase, size_t tid, size_t data_src, msg_host_t dest);
static void finish_all_task_copies (task_info_t ti);

/** @brief  Main master function. */
int master (int argc, char* argv[])
{
    heartbeat_t  heartbeat;
    msg_host_t     worker;
    msg_task_t     msg = NULL;
    size_t       wid;
    task_info_t  ti;

    print_config ();
    XBT_INFO ("JOB BEGIN"); XBT_INFO (" ");

    tasks_log = fopen ("tasks.log", "w");

    while (job.tasks_pending[MAP] + job.tasks_pending[REDUCE] > 0)
    {
	msg = NULL;
	receive (&msg, MASTER_MAILBOX);
	worker = MSG_task_get_source (msg);
	wid = get_worker_id (worker);

	if (message_is (msg, SMS_HEARTBEAT))
	{
	    heartbeat = &w_heartbeat[wid];

	    if (is_straggler (worker))
	    {
		set_speculative_tasks (worker);
	    }
	    else
	    {
		if (heartbeat->slots_av[MAP] > 0)
		    send_map_to_worker (worker);

		if (heartbeat->slots_av[REDUCE] > 0)
		    send_reduce_to_worker (worker);
	    }
	}
	else if (message_is (msg, SMS_TASK_DONE))
	{
	    ti = (task_info_t) MSG_task_get_data (msg);

	    switch (ti->phase)
	    {
		case MAP:
		    if (job.task_status[MAP][ti->id] != T_STATUS_DONE)
		    {
			job.task_status[MAP][ti->id] = T_STATUS_DONE;
			finish_all_task_copies (ti);
			stats.maps_processed[wid]++;
			job.tasks_pending[MAP]--;
			if (job.tasks_pending[MAP] <= 0)
			{
			    XBT_INFO (" ");
			    XBT_INFO ("MAP PHASE DONE");
			    XBT_INFO (" ");
			}
		    }
		    break;

		case REDUCE:
		    if (job.task_status[REDUCE][ti->id] != T_STATUS_DONE)
		    {
			job.task_status[REDUCE][ti->id] = T_STATUS_DONE;
			finish_all_task_copies (ti);
			stats.reduces_processed[wid]++;
			job.tasks_pending[REDUCE]--;
			if (job.tasks_pending[REDUCE] <= 0)
			{
			    XBT_INFO (" ");
			    XBT_INFO ("REDUCE PHASE DONE");
			    XBT_INFO (" ");
			}
		    }
		    break;
	    }
	    xbt_free_ref (&ti);
	}
	MSG_task_destroy (msg);
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
    XBT_INFO ("slots: %d map, %d reduce", config.map_slots, config.reduce_slots);
    XBT_INFO ("chunk replicas: %d", config.chunk_replicas);
    XBT_INFO ("chunk size: %.0f MB", config.chunk_size/1024/1024);
    XBT_INFO ("input chunks: %d", config.chunk_count);
    XBT_INFO ("input size: %d MB", config.chunk_count * (int)(config.chunk_size/1024/1024));
    XBT_INFO ("maps: %d", config.number_of_maps);
    XBT_INFO ("reduces: %d", config.number_of_reduces);
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

    task_count = (config.map_slots + config.reduce_slots) - (w_heartbeat[wid].slots_av[MAP] + w_heartbeat[wid].slots_av[REDUCE]);

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
	/ MSG_get_host_speed (worker_hosts[ti->wid]);
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

    if (w_heartbeat[wid].slots_av[MAP] < config.map_slots)
    {
	for (tid = 0; tid < config.number_of_maps; tid++)
	{
	    if (job.task_list[MAP][0][tid] != NULL)
	    {
		ti = (task_info_t) MSG_task_get_data (job.task_list[MAP][0][tid]);
		if (ti->wid == wid && task_time_elapsed (job.task_list[MAP][0][tid]) > 60)
		{
		    job.task_status[MAP][tid] = T_STATUS_TIP_SLOW;
		}
	    }
	}
    }

    if (w_heartbeat[wid].slots_av[REDUCE] < config.reduce_slots)
    {
	for (tid = 0; tid < config.number_of_reduces; tid++)
	{
	    if (job.task_list[REDUCE][0][tid] != NULL)
	    {
		ti = (task_info_t) MSG_task_get_data (job.task_list[REDUCE][0][tid]);
		if (ti->wid == wid && task_time_elapsed (job.task_list[REDUCE][0][tid]) > 60)
		{
		    job.task_status[REDUCE][tid] = T_STATUS_TIP_SLOW;
		}
	    }
	}
    }
}

/**
 * @brief  Choose a map task, and send it to a worker.
 * @param  dest  The destination worker.
 */
static void send_map_to_worker (msg_host_t dest)
{
    char*   flags;
    int     task_type;
    size_t  chunk;
    size_t  sid = NONE;
    size_t  tid = NONE;
    size_t  wid;

    if (job.tasks_pending[MAP] <= 0)
	return;

    enum { LOCAL, REMOTE, LOCAL_SPEC, REMOTE_SPEC, NO_TASK };
    task_type = NO_TASK;

    wid = get_worker_id (dest);

    /* Look for a task for the worker. */
    for (chunk = 0; chunk < config.chunk_count; chunk++)
    {
	if (job.task_status[MAP][chunk] == T_STATUS_PENDING)
	{
	    if (chunk_owner[chunk][wid])
	    {
		task_type = LOCAL;
		tid = chunk;
		break;
	    }
	    else
	    {
		task_type = REMOTE;
		tid = chunk;
	    }
	}
	else if (job.task_status[MAP][chunk] == T_STATUS_TIP_SLOW
		&& task_type > REMOTE
		&& !job.task_has_spec_copy[MAP][chunk])
	{
	    if (chunk_owner[chunk][wid])
	    {
		task_type = LOCAL_SPEC;
		tid = chunk;
	    }
	    else if (task_type > LOCAL_SPEC)
	    {
		task_type = REMOTE_SPEC;
		tid = chunk;
	    }
	}
    }

    switch (task_type)
    {
	case LOCAL:
	    flags = "";
	    sid = wid;
	    stats.map_local++;
	    break;

	case REMOTE:
	    flags = "(non-local)";
	    sid = find_random_chunk_owner (tid);
	    stats.map_remote++;
	    break;

	case LOCAL_SPEC:
	    flags = "(speculative)";
	    sid = wid;
	    job.task_has_spec_copy[MAP][tid] = 1;
	    stats.map_spec_l++;
	    break;

	case REMOTE_SPEC:
	    flags = "(non-local, speculative)";
	    sid = find_random_chunk_owner (tid);
	    job.task_has_spec_copy[MAP][tid] = 1;
	    stats.map_spec_r++;
	    break;

	default: return;
    }

    XBT_INFO ("map %zu assigned to %s %s", tid, MSG_host_get_name (dest), flags);

    send_task (MAP, tid, sid, dest);
}

/**
 * @brief  Choose a reduce task, and send it to a worker.
 * @param  dest  The destination worker.
 */
static void send_reduce_to_worker (msg_host_t dest)
{
    char*   flags;
    int     task_type;
    size_t  t;
    size_t  tid = NONE;

    if (job.tasks_pending[REDUCE] <= 0 || (float)job.tasks_pending[MAP]/config.number_of_maps > 0.9)
	return;

    enum { NORMAL, SPECULATIVE, NO_TASK };
    task_type = NO_TASK;

    for (t = 0; t < config.number_of_reduces; t++)
    {
	if (job.task_status[REDUCE][t] == T_STATUS_PENDING)
	{
	    task_type = NORMAL;
	    tid = t;
	    break;
	}
	else if (job.task_status[REDUCE][t] == T_STATUS_TIP_SLOW
		&& !job.task_has_spec_copy[REDUCE][t])
	{
	    task_type = SPECULATIVE;
	    tid = t;
	}
    }

    switch (task_type)
    {
	case NORMAL:
	    flags = "";
	    stats.reduce_normal++;
	    break;

	case SPECULATIVE:
	    flags = "(speculative)";
	    job.task_has_spec_copy[REDUCE][tid] = 1;
	    stats.reduce_spec++;
	    break;

	default: return;
    }

    XBT_INFO ("reduce %zu assigned to %s %s", tid, MSG_host_get_name (dest), flags);

    send_task (REDUCE, tid, NONE, dest);
}

/**
 * @brief  Send a task to a worker.
 * @param  phase     The current job phase.
 * @param  tid       The task ID.
 * @param  data_src  The ID of the DataNode that owns the task data.
 * @param  dest      The destination worker.
 */
static void send_task (enum phase_e phase, size_t tid, size_t data_src, msg_host_t dest)
{
    char         mailbox[MAILBOX_ALIAS_SIZE];
    int          i;
    double       cpu_required = 0.0;
    msg_task_t     task = NULL;
    task_info_t  task_info;
    size_t       wid;

    wid = get_worker_id (dest);

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

    w_heartbeat[wid].slots_av[phase]--;

    for (i = 0; i < MAX_SPECULATIVE_COPIES; i++)
    {
	if (job.task_list[phase][i][tid] == NULL)
	{
	    job.task_list[phase][i][tid] = task;
	    break;
	}
    }

    fprintf (tasks_log, "%d_%zu_%d\t%s\t%zu\t%.3f\tSTART\n", phase, tid, i, (phase==MAP?"MAP":"REDUCE"), wid, MSG_get_clock ());

#ifdef VERBOSE
    XBT_INFO ("TX: %s > %s", SMS_TASK, MSG_host_get_name (dest));
#endif

    sprintf (mailbox, TASKTRACKER_MAILBOX, wid);
    xbt_assert (MSG_task_send (task, mailbox) == MSG_OK, "ERROR SENDING MESSAGE");
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
	if (job.task_list[phase][i][tid] != NULL)
	{
	    MSG_task_cancel (job.task_list[phase][i][tid]);
	    //FIXME: MSG_task_destroy (job.task_list[phase][i][tid]);
	    job.task_list[phase][i][tid] = NULL;
	    fprintf (tasks_log, "%d_%zu_%d\t-\t-\t%.3f\tEND\t%.3f\n", ti->phase, tid, i, MSG_get_clock (), ti->shuffle_end);
	}
    }
}

