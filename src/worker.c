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
static int acquire_slot_port (int* slots);
static void release_slot_port (task_info_t ti);

/**
 * @brief  Main worker function.
 *
 * This is the initial function of a worker node.
 * It creates other processes and runs a heartbeat loop.
 */
int worker (int argc, char* argv[])
{
    m_host_t          me;
    m_process_t       listen_p;
    m_process_t       data_node_p;

    me = MSG_host_self ();

    /* Wait until a start signal is sent by the master. */
    MSG_task_destroy (receive (PORT_MASTER));

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
    size_t  my_id;

    my_id = get_worker_id (MSG_host_self ());

    while (!job.finished)
    {
	//FIXME No need to send data. w_heartbeat is global.
	send (SMS_HEARTBEAT, 0.0, 0.0, &w_heartbeat[my_id], master_host, PORT_MASTER);
	MSG_process_sleep (config.heartbeat_interval);
    }
}

/**
 * @brief  Process that listens for tasks.
 */
static int listen (int argc, char* argv[])
{
    int*         slots;
    m_host_t     me;
    m_task_t     msg;
    task_info_t  ti;

    me = MSG_host_self ();
    slots = xbt_new0 (int, config.map_slots + config.reduce_slots);

    while (!job.finished)
    {
	msg = receive (PORT_MASTER);

	if (message_is (msg, SMS_TASK))
	{
	    ti = (task_info_t) MSG_task_get_data (msg);
	    ti->slots = slots;
	    ti->slot_port = acquire_slot_port (slots);
	    MSG_process_create ("compute", compute, msg, me);
	}
    }

    xbt_free_ref (&slots);

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

    switch (ti->phase)
    {
	case MAP:
	    get_chunk (ti);
	    break;

	case REDUCE:
	    get_pairs (ti);
	    break;
    }

    release_slot_port (ti);

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
	send (SMS_TASK_DONE, 0.0, 0.0, ti, master_host, PORT_MASTER);

    return 0;
}

/**
 * @brief  Get the chunk associated to a map task.
 * @param  ti  The task information.
 */
static void get_chunk (task_info_t ti)
{
    m_task_t  data;
    size_t    my_id;

    my_id = get_worker_id (MSG_host_self ());

    /* Request the chunk to the source node. */
    if (ti->src != my_id)
    {
	send_sms (SMS_GET_CHUNK, worker_hosts[ti->src], PORT_DATA_REQ); 
	data = receive (ti->slot_port);
	MSG_task_destroy (data);
    }
}

/**
 * @brief  Copy the itermediary pairs for a reduce task.
 * @param  ti  The task information.
 */
static void get_pairs (task_info_t ti)
{
    m_task_t  data;
    size_t    copy, total_copied;
    size_t    my_id;
    size_t    wid;
    size_t*   maps_copied;

    my_id = get_worker_id (MSG_host_self ());
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
		    data = receive (ti->slot_port);
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
    size_t  copy;

    copy = stats.maps_processed[wid] - maps_copied;

    if (copy)
    {
	send (SMS_GET_INTER_PAIRS, 0.0, 0.0, (void*) copy, worker_hosts[wid], PORT_DATA_REQ);
	return copy;
    }

    return 0;
}

/**
 * @brief  Acquire a port ID to use for data fetching.
 * @param  slots  The slots' availability list.
 * @return The ID of an available port.
 */
static int acquire_slot_port (int* slots)
{
    int  slot;
    int  total_slots;

    total_slots = config.map_slots + config.reduce_slots;

    for (slot = 0; slot < total_slots; slot++)
    {
	if (slots[slot] == 0)
	{
	    slots[slot] = 1;
	    break;
	}
    }
    
    xbt_assert (slot < total_slots, "Error getting a slot port.");

    return PORT_SLOTS_START + slot;
}

/**
 * @brief  Release the data fetching port of a task.
 * @param  ti  The task information.
 */
static void release_slot_port (task_info_t ti)
{
    int  slot;

    slot = ti->slot_port - PORT_SLOTS_START;
    ti->slots[slot] = 0;
}

