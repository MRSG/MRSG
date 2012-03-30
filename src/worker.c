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
static void get_chunk (task_info_t ti);
static void get_pairs (task_info_t ti);
static size_t ask_for_pairs (size_t wid, size_t maps_copied);

/**
 * @brief  Main worker function.
 *
 * This is the initial function of a worker node.
 * It creates other processes and runs a heartbeat loop.
 */
int worker (int argc, char* argv[])
{
    char         mailbox[MAILBOX_ALIAS_SIZE];
    m_host_t     me;
    m_process_t  listen_p;
    m_process_t  data_node_p;
    m_task_t     msg = NULL;

    me = MSG_host_self ();

    /* Wait until a start signal is sent by the master. */
    sprintf (mailbox, TASKTRACKER_MAILBOX, get_worker_id (me));
    receive (&msg, mailbox);
    MSG_task_destroy (msg);

    /* Spawn a process that listens for tasks. */
    listen_p = MSG_process_create ("listen", listen, NULL, me);
    /* Spawn a process to exchange data with other workers. */
    data_node_p = MSG_process_create ("data-node", data_node, NULL, me);
    /* Start sending heartbeat signals to the master node. */
    heartbeat ();

    MSG_process_kill (data_node_p);
    MSG_process_kill (listen_p);

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
    m_host_t     me;
    m_task_t     msg = NULL;

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
    }

    return 0;
}

/**
 * @brief  Process that computes a task.
 */
static int compute (int argc, char* argv[])
{
    m_task_t     task;
    task_info_t  ti;
    xbt_ex_t     e;

    task = (m_task_t) MSG_process_get_data (MSG_process_self ());
    ti = (task_info_t) MSG_task_get_data (task);
    ti->pid = MSG_process_self_PID ();

    switch (ti->phase)
    {
	case MAP:
	    get_chunk (ti);
	    break;

	case REDUCE:
	    get_pairs (ti);
	    break;
    }

    if (job.task_state[ti->phase][ti->id] != T_STATE_DONE)
    {
	TRY
	{
	    MSG_task_execute (task);
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
 * @brief  Get the chunk associated to a map task.
 * @param  ti  The task information.
 */
static void get_chunk (task_info_t ti)
{
    char      mailbox[MAILBOX_ALIAS_SIZE];
    m_task_t  data = NULL;
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
static void get_pairs (task_info_t ti)
{
    char      mailbox[MAILBOX_ALIAS_SIZE];
    m_task_t  data = NULL;
    size_t    copy, total_copied;
    size_t    my_id;
    size_t    wid;
    size_t*   maps_copied;

    my_id = get_worker_id (MSG_host_self ());
    sprintf (mailbox, TASK_MAILBOX, my_id, MSG_process_self_PID ());

    maps_copied = xbt_new0 (size_t, config.number_of_workers);

#ifdef VERBOSE
    XBT_INFO ("INFO: start copy");
#endif
    total_copied = 0;
    for (wid = 0; wid < config.number_of_workers; wid++)
	maps_copied[wid] = 0;

    while (job.tasks_pending[MAP] > 0 || (total_copied + stats.maps_processed[my_id]) < config.number_of_maps)
    {
	for (wid = 0; wid < config.number_of_workers; wid++)
	{
	    if (wid != my_id)
	    {
		if (job.task_state[REDUCE][ti->id] == T_STATE_DONE)
		{
		    xbt_free_ref (&maps_copied);
		    return;
		}

		copy = ask_for_pairs (wid, maps_copied[wid]);

		if (copy)
		{
		    data = NULL;
		    receive (&data, mailbox);
		    maps_copied[wid] += copy;
		    total_copied += copy;
		    MSG_task_destroy (data);
		}
	    }
	}
	MSG_process_sleep (config.heartbeat_interval);
    }
#ifdef VERBOSE
    XBT_INFO ("INFO: copy finished (%zu)", total_copied);
#endif

    xbt_free_ref (&maps_copied);
}

/**
 * @brief  Ask for itermediary pairs.
 * @param  wid          The worker (ID) to whom we'll ask for pairs.
 * @param  maps_copied  The amount of Map results (pair sets) already copied.
 * @return The amount of Map results copied on this request.
 */
static size_t ask_for_pairs (size_t wid, size_t maps_copied)
{
    char    mailbox[MAILBOX_ALIAS_SIZE];
    size_t  copy;

    copy = stats.maps_processed[wid] - maps_copied;

    if (copy)
    {
	sprintf (mailbox, DATANODE_MAILBOX, wid);
	send (SMS_GET_INTER_PAIRS, 0.0, 0.0, (void*) copy, mailbox);
	return copy;
    }

    return 0;
}

