#include <mrsg.h>
#include <scheduling.h>

size_t choose_remote_map_task (size_t wid);

/**
 * User function that indicates the amount of bytes
 * that a map task will emit to a reduce task.
 *
 * @param  mid  The ID of the map task.
 * @param  rid  The ID of the reduce task.
 * @return The amount of data emitted (in bytes).
 */
int my_map_output_function (size_t mid, size_t rid)
{
    return 4*1024*1024;
}


/**
 * User function that indicates the cost of a task.
 *
 * @param  phase  The execution phase.
 * @param  tid    The ID of the task.
 * @param  wid    The ID of the worker that received the task.
 * @return The task cost in FLOPs.
 */
double my_task_cost_function (enum phase_e phase, size_t tid, size_t wid)
{
    switch (phase)
    {
	case MAP:
	    return 1e+11;

	case REDUCE:
	    return 5e+11;
    }
}

/**
 * @brief  Chooses a map or reduce task and send it to a worker.
 * @param  phase  MAP or REDUCE.
 * @param  wid  Worker id.
 * @return Chosen task id.
 */
size_t remote_scheduler_f (enum phase_e phase, size_t wid)
{
    switch (phase)
    {
	case MAP:
	    return choose_remote_map_task (wid);

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
size_t choose_remote_map_task (size_t wid)
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

	if (task_type == REMOTE)
	{
	    tid = chunk;
	    break;
	}
	else if (task_type == LOCAL
		|| (job.task_instances[MAP][chunk] < 2 // Speculative
		    && task_type < best_task_type ))   // tasks.
	{
	    best_task_type = task_type;
	    tid = chunk;
	}
    }

    return tid;
}

int main (int argc, char* argv[])
{
    /* MRSG_init must be called before setting the user functions. */
    MRSG_init ();
    /* Set the task cost function. */
    MRSG_set_task_cost_f (my_task_cost_function);
    /* Set the map output function. */
    MRSG_set_map_output_f (my_map_output_function);
    /* Set the scheduler function */
    MRSG_set_scheduler_f (remote_scheduler_f);
    /* Run the simulation. */
    MRSG_main ("g5k.xml", "hello.deploy.xml", "hello.conf");

    return 0;
}

// vim: set ts=8 sw=4:
