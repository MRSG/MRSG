#include <mrsg.h>

int my_map_output_function (size_t mid, size_t rid)
{
    return 6710886;
}

double my_task_cost_function (enum phase_e phase, size_t tid, size_t wid)
{
    switch (phase)
    {
	case MAP:
	    return 16777216000;

	case REDUCE:
	    return 67108864000;
    }
}

int main (int argc, char* argv[])
{
    MRSG_init ();
    MRSG_set_task_cost_f (my_task_cost_function);
    MRSG_set_map_output_f (my_map_output_function);
    MRSG_main ("platform.xml", "deploy.xml", "mrsg.conf");

    return 0;
}

