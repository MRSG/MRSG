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

#include "common.h"
#include "dfs.h"

XBT_LOG_EXTERNAL_DEFAULT_CATEGORY (msg_test);

static void heartbeat (void);
static int listen (int argc, char* argv[]);
static int compute (int argc, char* argv[]);
static void update_map_output (msg_host_t worker, size_t mid);
static void get_chunk (task_info_t ti);
static void get_map_output (task_info_t ti);

/**
 * @brief  Main worker function.
 *
 * This is the initial function of a worker node.
 * It creates other processes and runs a heartbeat loop.
 */
int worker (int argc, char* argv[])
{
    char           mailbox[MAILBOX_ALIAS_SIZE];
    msg_host_t     me;

    me = MSG_host_self ();

    /* Spawn a process that listens for tasks. */
    MSG_process_create ("listen", listen, NULL, me);
    /* Spawn a process to exchange data with other workers. */
    MSG_process_create ("data-node", data_node, NULL, me);
    /* Start sending heartbeat signals to the master node. */
    heartbeat ();

    sprintf (mailbox, DATANODE_MAILBOX, get_worker_id (me));
    send_sms (SMS_FINISH, mailbox);
    sprintf (mailbox, TASKTRACKER_MAILBOX, get_worker_id (me));
    send_sms (SMS_FINISH, mailbox);

    return 0;
}

/**
 * @brief  The heartbeat loop.
 */
static void heartbeat (void)
{
    while (!job.finished)
    {
	send_sms (SMS_HEARTBEAT, MASTER_MAILBOX);
	MSG_process_sleep (config.heartbeat_interval);
    }
}

/**
 * @brief  Process that listens for tasks.
 */
static int listen (int argc, char* argv[])
{
    char         mailbox[MAILBOX_ALIAS_SIZE];
    msg_host_t     me;
    msg_task_t     msg = NULL;

    me = MSG_host_self ();
    sprintf (mailbox, TASKTRACKER_MAILBOX, get_worker_id (me));

    while (!job.finished)
    {
	msg = NULL;
	receive (&msg, mailbox);

	if (message_is (msg, SMS_TASK))
	{
	    MSG_process_create ("compute", compute, msg, me);
	}
	else if (message_is (msg, SMS_FINISH))
	{
	    MSG_task_destroy (msg);
	    break;
	}
    }

    return 0;
}

/**
 * @brief  Process that computes a task.
 */
static int compute (int argc, char* argv[])
{
    msg_error_t  error;
    msg_task_t     task;
    task_info_t  ti;
    xbt_ex_t     e;

    task = (msg_task_t) MSG_process_get_data (MSG_process_self ());
    ti = (task_info_t) MSG_task_get_data (task);
    ti->pid = MSG_process_self_PID ();

    switch (ti->phase)
    {
	case MAP:
	    get_chunk (ti);
	    break;

	case REDUCE:
	    get_map_output (ti);
	    break;
    }

    if (job.task_status[ti->phase][ti->id] != T_STATUS_DONE)
    {
	TRY
	{
	    error = MSG_task_execute (task);

	    if (ti->phase == MAP && error == MSG_OK)
		update_map_output (MSG_host_self (), ti->id);
	}
	CATCH (e)
	{
	    xbt_assert (e.category == cancel_error, "%s", e.msg);
	    xbt_ex_free (e);
	}
    }

    w_heartbeat[ti->wid].slots_av[ti->phase]++;
    
    if (!job.finished)
	send (SMS_TASK_DONE, 0.0, 0.0, ti, MASTER_MAILBOX);

    return 0;
}

/**
 * @brief  Update the amount of data produced by a mapper.
 * @param  worker  The worker that finished a map task.
 * @param  mid     The ID of map task.
 */
static void update_map_output (msg_host_t worker, size_t mid)
{
    size_t  rid;
    size_t  wid;

    wid = get_worker_id (worker);

    for (rid = 0; rid < config.number_of_reduces; rid++)
	job.map_output[wid][rid] += user.map_output_f (mid, rid);
}

/**
 * @brief  Get the chunk associated to a map task.
 * @param  ti  The task information.
 */
static void get_chunk (task_info_t ti)
{
    char      mailbox[MAILBOX_ALIAS_SIZE];
    msg_task_t  data = NULL;
    size_t    my_id;

    my_id = get_worker_id (MSG_host_self ());

    /* Request the chunk to the source node. */
    if (ti->src != my_id)
    {
	sprintf (mailbox, DATANODE_MAILBOX, ti->src);
	send_sms (SMS_GET_CHUNK, mailbox); 

	sprintf (mailbox, TASK_MAILBOX, my_id, MSG_process_self_PID ());
	receive (&data, mailbox);
	
	MSG_task_destroy (data);
    }
}

/**
 * @brief  Copy the itermediary pairs for a reduce task.
 * @param  ti  The task information.
 */
static void get_map_output (task_info_t ti)
{
    char      mailbox[MAILBOX_ALIAS_SIZE];
    msg_task_t  data = NULL;
    size_t    total_copied, must_copy;
    size_t    mid;
    size_t    my_id;
    size_t    wid;
    size_t*   data_copied;

    my_id = get_worker_id (MSG_host_self ());
    data_copied = xbt_new0 (size_t, config.number_of_workers);
    ti->map_output_copied = data_copied;
    total_copied = 0;
    must_copy = 0;
    for (mid = 0; mid < config.number_of_maps; mid++)
	must_copy += user.map_output_f (mid, ti->id);

#ifdef VERBOSE
    XBT_INFO ("INFO: start copy");
#endif

    while (total_copied < must_copy)
    {
	for (wid = 0; wid < config.number_of_workers; wid++)
	{
	    if (job.task_status[REDUCE][ti->id] == T_STATUS_DONE)
	    {
		xbt_free_ref (&data_copied);
		return;
	    }

	    if (job.map_output[wid][ti->id] > data_copied[wid])
	    {
		sprintf (mailbox, DATANODE_MAILBOX, wid);
		send (SMS_GET_INTER_PAIRS, 0.0, 0.0, ti, mailbox);

		sprintf (mailbox, TASK_MAILBOX, my_id, MSG_process_self_PID ());
		data = NULL;
		receive (&data, mailbox);
		data_copied[wid] += MSG_task_get_data_size (data);
		total_copied += MSG_task_get_data_size (data);
		MSG_task_destroy (data);
	    }
	}
	/* (Hadoop 0.20.2) mapred/ReduceTask.java:1979 */
	MSG_process_sleep (5);
    }

#ifdef VERBOSE
    XBT_INFO ("INFO: copy finished");
#endif
    ti->shuffle_end = MSG_get_clock ();

    xbt_free_ref (&data_copied);
}

