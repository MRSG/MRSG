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


static void send_data (msg_task_t msg);


void distribute_data (void)
{
    size_t  chunk;

    /* Allocate memory for the mapping matrix. */
    chunk_owner = xbt_new (char*, config.chunk_count);
    for (chunk = 0; chunk < config.chunk_count; chunk++)
    {
	chunk_owner[chunk] = xbt_new0 (char, config.number_of_workers);
    }

    /* Call the distribution function. */
    user.dfs_f (chunk_owner, config.chunk_count, config.number_of_workers, config.chunk_replicas);
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
    char      mailbox[MAILBOX_ALIAS_SIZE];
    msg_task_t  msg = NULL;

    sprintf (mailbox, DATANODE_MAILBOX, get_worker_id (MSG_host_self ()));

    while (!job.finished)
    {
	msg = NULL;
	receive (&msg, mailbox);
	if (message_is (msg, SMS_FINISH))
	{
	    MSG_task_destroy (msg);
	    break;
	}
	else
	{
	    send_data (msg);
	}
    }

    return 0;
}

static void send_data (msg_task_t msg)
{
    char         mailbox[MAILBOX_ALIAS_SIZE];
    double       data_size;
    size_t       my_id;
    task_info_t  ti;

    my_id = get_worker_id (MSG_host_self ());

    sprintf (mailbox, TASK_MAILBOX,
	    get_worker_id (MSG_task_get_source (msg)),
	    MSG_process_get_PID (MSG_task_get_sender (msg)));

    if (message_is (msg, SMS_GET_CHUNK))
    {
	MSG_task_dsend (MSG_task_create ("DATA-C", 0.0, config.chunk_size, NULL), mailbox, NULL);
    }
    else if (message_is (msg, SMS_GET_INTER_PAIRS))
    {
	ti = (task_info_t) MSG_task_get_data (msg);
	data_size = job.map_output[my_id][ti->id] - ti->map_output_copied[my_id];
	MSG_task_dsend (MSG_task_create ("DATA-IP", 0.0, data_size, NULL), mailbox, NULL);
    }

    MSG_task_destroy (msg);
}

