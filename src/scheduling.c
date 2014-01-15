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

#include "scheduling.h" // get_task_type

/**
 * @brief  Chooses a map or reduce task and send it to a worker.
 * @param  phase  MAP or REDUCE.
 * @param  wid  Worker id.
 * @return Chosen task id.
 */
size_t default_scheduler_f (enum phase_e phase, size_t wid)
{
    switch (phase)
    {
	case MAP:
	    return choose_default_map_task (wid);

	case REDUCE:
	    return choose_default_reduce_task (wid);

	default:
	    return NONE;
    }
}

/**
 * @brief  Choose a map task, and send it to a worker.
 * @param  wid  Worker id.
 */
size_t choose_default_map_task (size_t wid)
{
    size_t           chunk;
    size_t           tid = NONE;
    enum task_type_e task_type, best_task_type = NO_TASK;

    if (job.tasks_pending[MAP] <= 0)
	return tid;

    /* Look for a task for the worker. */
    for (chunk = 0; chunk < config.chunk_count; chunk++)
    {
	task_type = get_task_type (MAP, chunk, wid);

	if (task_type == LOCAL)
	{
	    tid = chunk;
	    break;
	}
	else if (task_type == REMOTE
		|| (job.task_instances[MAP][chunk] < 2 // Speculative
		    && task_type < best_task_type ))   // tasks.
	{
	    best_task_type = task_type;
	    tid = chunk;
	}
    }

    return tid;
}

/**
 * @brief  Choose a reduce task, and send it to a worker.
 * @param  wid  Worker id.
 */
size_t choose_default_reduce_task (size_t wid)
{
    size_t           t;
    size_t           tid = NONE;
    enum task_type_e task_type, best_task_type = NO_TASK;

    if (job.tasks_pending[REDUCE] <= 0 || (float)job.tasks_pending[MAP]/config.amount_of_tasks[MAP] > 0.9)
	return tid;

    for (t = 0; t < config.amount_of_tasks[REDUCE]; t++)
    {
	task_type = get_task_type (REDUCE, t, wid);

	if (task_type == NORMAL)
	{
	    tid = t;
	    break;
	}
	else if (task_type < best_task_type
		&& job.task_instances[REDUCE][t] < 2)
	{
	    best_task_type = task_type; // SPECULATIVE
	    tid = t;
	}
    }

    return tid;
}

// vim: set ts=8 sw=4:
