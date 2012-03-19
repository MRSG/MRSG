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
#include <xbt/sysdep.h>
#include <xbt/log.h>
#include <xbt/asserts.h>
#include "common.h"
#include "dfs.h"
#include "mrsg.h"

XBT_LOG_NEW_DEFAULT_CATEGORY (msg_test, "MRSG");

#define MAX_LINE_SIZE 256

static char master_name[MAX_LINE_SIZE];

int master (int argc, char *argv[]);
int worker (int argc, char *argv[]);

static void check_config (void);
static MSG_error_t run_simulation (const char* platform_file, const char* deploy_file, const char* mr_config_file);
static void init_mr_config (const char* mr_config_file);
static void read_mr_config_file (const char* file_name);
static void init_config (void);
static void init_job (void);
static void init_stats (void);
static void free_global_mem (void);

int MRSG_main (const char* plat, const char* depl, const char* conf)
{
    int argc = 0;
    char* argv[] = {"MRSG"};
    MSG_error_t  res = MSG_OK;

    check_config ();

    MSG_global_init (&argc, argv);
    res = run_simulation (plat, depl, conf);
    MSG_clean ();

    if (res == MSG_OK)
	return 0;
    else
	return 1;
}

static void check_config (void)
{
    xbt_assert (user.task_cost_f != NULL, "Task cost function not specified.");
}

/**
 * @param  platform_file   The path/name of the platform file.
 * @param  deploy_file     The path/name of the deploy file.
 * @param  mr_config_file  The path/name of the configuration file.
 */
static MSG_error_t run_simulation (const char* platform_file, const char* deploy_file, const char* mr_config_file)
{
    MSG_error_t  res = MSG_OK;

    read_mr_config_file (mr_config_file);

    MSG_set_channel_number (2 + config.map_slots + config.reduce_slots);
    MSG_create_environment (platform_file);

    //FIXME: Application deployment.
    MSG_function_register ("master", master);
    MSG_function_register ("worker", worker);
    MSG_launch_application (deploy_file);

    init_mr_config (mr_config_file);

    res = MSG_main ();

    free_global_mem ();

    return res;
}

/**
 * @brief  Initialize the MapReduce configuration.
 * @param  mr_config_file  The path/name of the configuration file.
 */
static void init_mr_config (const char* mr_config_file)
{
    srand (12345);
    init_config ();
    init_stats ();
    init_job ();
    distribute_data ();
}

/**
 * @brief  Read the MapReduce configuration file.
 * @param  file_name  The path/name of the configuration file.
 */
static void read_mr_config_file (const char* file_name)
{
    char    property[256];
    char    value[MAX_LINE_SIZE];
    char    *s1, *s2;
    double  map_out_perc;
    FILE*   file;

    file = fopen (file_name, "r");

    xbt_assert (file != NULL, "Error reading cofiguration file: %s", file_name);

    //TODO assert values
    while ( fscanf (file, "%256s", property) != EOF )
    {
	if ( strcmp (property, "reduces") == 0 )
	{
	    fscanf (file, "%d", &config.number_of_reduces);
	}
	else if ( strcmp (property, "chunk_size") == 0 )
	{
	    fscanf (file, "%lg", &config.chunk_size);
	    config.chunk_size *= 1024 * 1024; /* MB -> bytes */
	}
	else if ( strcmp (property, "input_chunks") == 0 )
	{
	    fscanf (file, "%d", &config.chunk_count);
	}
	else if ( strcmp (property, "dfs_replicas") == 0 )
	{
	    fscanf (file, "%d", &config.chunk_replicas);
	}
	else if ( strcmp (property, "map_output") == 0 )
	{
	    fscanf (file, "%lg", &map_out_perc);
	    config.map_out_size = (map_out_perc / 100) * config.chunk_count * config.chunk_size;
	}
	else if ( strcmp (property, "map_cost") == 0 )
	{
	    fscanf (file, "%lg", &config.cpu_required_map);
	}
	else if ( strcmp (property, "reduce_cost") == 0 )
	{
	    fscanf (file, "%lg", &config.cpu_required_reduce);
	}
	else if ( strcmp (property, "map_slots") == 0 )
	{
	    fscanf (file, "%d", &config.map_slots);
	}
	else if ( strcmp (property, "reduce_slots") == 0 )
	{
	    fscanf (file, "%d", &config.reduce_slots);
	}
	else if ( strcmp (property, "master") == 0 )
	{
	    fgets (value, MAX_LINE_SIZE, file);
	    s1 = strchr (value, '"') + 1;
	    s2 = strchr (s1, '"');
	    *s2 = '\0';
	    strcpy (master_name, s1);
	}
	else
	{
	    printf ("Error: Property %s is not valid. (in %s)", property, file_name);
	    exit (1);
	}
    }

    fclose (file);
}

/**
 * @brief  Initialize the config structure.
 */
static void init_config (void)
{
    int        host_count;
    m_host_t*  ht;
    size_t     i, wid;

    /* Initialize workers information. */

    host_count = MSG_get_host_number();
    config.number_of_workers = host_count - 1;

    w_heartbeat = xbt_new (struct heartbeat_s, config.number_of_workers);
    for (wid = 0; wid < config.number_of_workers; wid++)
    {
	w_heartbeat[wid].slots_av[MAP] = config.map_slots;
	w_heartbeat[wid].slots_av[REDUCE] = config.reduce_slots;
    }

    worker_hosts = xbt_new (m_host_t, config.number_of_workers);
    ht = MSG_get_host_table();
    config.grid_cpu_power = 0.0;

    for (i = wid = 0; i < host_count; i++)
    {
	/*FIXME Skip the master node. */
	if ( strcmp(master_name, MSG_host_get_name(ht[i])) != 0 )
	{
	    worker_hosts[wid] = ht[i];
	    /* Set the worker ID as its data. */
	    MSG_host_set_data (worker_hosts[wid], (void*) wid);
	    /* Add the worker's cpu power to the grid total. */
	    config.grid_cpu_power += MSG_get_host_speed (worker_hosts[wid]);

	    wid++;
	}
	else
	{
	    master_host = ht[i];
	}
    }

    xbt_free_ref (&ht);

    config.cpu_required_map *= config.chunk_size;
    config.cpu_required_reduce *= (config.map_out_size / config.number_of_reduces);
    config.grid_average_speed = config.grid_cpu_power / config.number_of_workers;
    config.heartbeat_interval = maxval (3, config.number_of_workers / 100);
    config.number_of_maps = config.chunk_count;
}

/**
 * @brief  Initialize the job structure.
 */
static void init_job (void)
{
    int  i;

    job.finished = 0;

    /* Initialize map information. */
    job.tasks_pending[MAP] = config.number_of_maps;
    job.task_state[MAP] = xbt_new0 (int, config.number_of_maps);
    job.task_has_spec_copy[MAP] = xbt_new0 (int, config.number_of_maps);
    job.task_list[MAP] = xbt_new0 (m_task_t*, MAX_SPECULATIVE_COPIES);
    for (i = 0; i < MAX_SPECULATIVE_COPIES; i++)
	job.task_list[MAP][i] = xbt_new0 (m_task_t, config.number_of_maps);

    /* Initialize reduce information. */
    job.tasks_pending[REDUCE] = config.number_of_reduces;
    job.task_state[REDUCE] = xbt_new0 (int, config.number_of_reduces);
    job.task_has_spec_copy[REDUCE] = xbt_new0 (int, config.number_of_reduces);
    job.task_list[REDUCE] = xbt_new0 (m_task_t*, MAX_SPECULATIVE_COPIES);
    for (i = 0; i < MAX_SPECULATIVE_COPIES; i++)
	job.task_list[REDUCE][i] = xbt_new0 (m_task_t, config.number_of_reduces);
}

/**
 * @brief  Initialize the stats structure.
 */
static void init_stats (void)
{
    stats.map_local = 0;
    stats.map_remote = 0;
    stats.map_spec_l = 0;
    stats.map_spec_r = 0;
    stats.reduce_normal = 0;
    stats.reduce_spec = 0;
    stats.maps_processed = xbt_new0 (int, config.number_of_workers);
    stats.reduces_processed = xbt_new0 (int, config.number_of_workers);
}

/**
 * @brief  Free allocated memory for global variables.
 */
static void free_global_mem (void)
{
    size_t  i;

    for (i = 0; i < config.chunk_count; i++)
	xbt_free_ref (&chunk_owner[i]);
    xbt_free_ref (&chunk_owner);

    xbt_free_ref (&stats.maps_processed);

    xbt_free_ref (&worker_hosts);
    xbt_free_ref (&job.task_state[MAP]);
    xbt_free_ref (&job.task_has_spec_copy[MAP]);
    xbt_free_ref (&job.task_state[REDUCE]);
    xbt_free_ref (&job.task_has_spec_copy[REDUCE]);
    xbt_free_ref (&w_heartbeat);
    for (i = 0; i < MAX_SPECULATIVE_COPIES; i++)
	xbt_free_ref (&job.task_list[MAP][i]);
    xbt_free_ref (&job.task_list[MAP]);
    for (i = 0; i < MAX_SPECULATIVE_COPIES; i++)
	xbt_free_ref (&job.task_list[REDUCE][i]);
    xbt_free_ref (&job.task_list[REDUCE]);
    xbt_free_ref (&stats.reduces_processed);
}

