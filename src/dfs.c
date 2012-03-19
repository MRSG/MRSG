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

#include <msg/msg.h>
#include "common.h"
#include "dfs.h"

XBT_LOG_EXTERNAL_DEFAULT_CATEGORY (msg_test);

static int send_data (int argc, char* argv[]);
static int get_slot_port (m_task_t data_request);

void distribute_data (void)
{
    FILE*   log;
    int     counter;
    size_t  chunk;
    size_t  owner;

    /* Allocate memory for the mapping matrix. */
    chunk_owner = xbt_new (char*, config.chunk_count);
    for (chunk = 0; chunk < config.chunk_count; chunk++)
    {
	chunk_owner[chunk] = xbt_new0 (char, config.number_of_workers);
    }

    /* Call the distribution function. */
    user.dfs_f (chunk_owner, config.chunk_count, config.number_of_workers, config.chunk_replicas);

    /* Save the distribution to a log file. */
    log = fopen ("chunks.log", "w");
    xbt_assert (log != NULL, "Error creating log file.");
    for (owner = 0; owner < config.number_of_workers; owner++)
    {
	fprintf (log, "worker %06zu | ", owner);
	counter = 0;
	for (chunk = 0; chunk < config.chunk_count; chunk++)
	{
	    fprintf (log, "%d", chunk_owner[chunk][owner]);
	    if (chunk_owner[chunk][owner])
		counter++;
	}
	fprintf (log, " | chunks owned: %d\n", counter);
    }
    fclose (log);
}

void default_dfs_f (char** dfs_matrix, size_t chunks, size_t workers, int replicas)
{
    int     r;
    size_t  chunk;
    size_t  owner;

    if (config.chunk_replicas >= config.number_of_workers)
    {
	/* All workers own every chunk. */
	for (chunk = 0; chunk < config.chunk_count; chunk++)
	{
	    for (owner = 0; owner < config.number_of_workers; owner++)
	    {
		chunk_owner[chunk][owner] = 1;
	    }
	}
    }
    else
    {
	/* Ok, it's a typical distribution. */
	for (chunk = 0; chunk < config.chunk_count; chunk++)
	{
	    for (r = 0; r < config.chunk_replicas; r++)
	    {
		owner = ((chunk % config.number_of_workers)
			+ ((config.number_of_workers / config.chunk_replicas) * r)
			) % config.number_of_workers;

		chunk_owner[chunk][owner] = 1;
	    }
	}
    }
}

size_t find_random_chunk_owner (int cid)
{
    int     replica;
    size_t  owner = NONE;
    size_t  wid;

    replica = rand () % config.chunk_replicas;

    for (wid = 0; wid < config.number_of_workers; wid++)
    {
	if (chunk_owner[cid][wid])
	{
	    owner = wid;

	    if (replica == 0)
		break;
	    else
		replica--;
	}
    }

    xbt_assert (owner != NONE, "Aborted: chunk %d is missing.", cid);

    return owner;
}

int data_node (int argc, char* argv[])
{
    m_task_t  msg = NULL;

    while (!job.finished)
    {
	msg = receive (PORT_DATA_REQ);
	MSG_process_create ("send-data", send_data, msg, MSG_host_self ());
    }

    return 0;
}

/**
 * @brief  Process that responds to data requests.
 */
static int send_data (int argc, char* argv[])
{
    double    data_size;
    int       answer_port;
    m_host_t  dest = NULL;
    m_task_t  msg = NULL;
    size_t    maps_requested;

    msg = MSG_process_get_data (MSG_process_self ());
    dest = MSG_task_get_source (msg);
    answer_port = get_slot_port (msg);

    if (message_is (msg, SMS_GET_CHUNK))
    {
	send ("DATA-C", 0.0, config.chunk_size, NULL, dest, answer_port);
    }
    else if (message_is (msg, SMS_GET_INTER_PAIRS))
    {
	maps_requested = (size_t) MSG_task_get_data (msg);
	data_size = (config.map_out_size / config.number_of_maps / config.number_of_reduces)
	    * maps_requested;
	send ("DATA-IP", 0.0, data_size, NULL, dest, answer_port);
    }

    MSG_task_destroy (msg);
    return 0;
}

/**
 * @brief  Get the port to respond to a data request.
 * @param  data_request  The data request message/task.
 * @return The number of the destination port.
 */
static int get_slot_port (m_task_t data_request)
{
    m_process_t  sender;
    m_task_t     task;
    task_info_t  ti;

    sender = MSG_task_get_sender (data_request);
    task = (m_task_t) MSG_process_get_data (sender);
    ti = (task_info_t) MSG_task_get_data (task);

    return ti->slot_port;
}

