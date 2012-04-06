#include <mrsg.h>

double my_task_cost_function (enum phase_e phase, size_t tid, size_t wid)
{
    switch (phase)
    {
	case MAP:
	    return 250.0;

	case REDUCE:
	    return 500.0;
    }
}

int main (int argc, char* argv[])
{
    MRSG_init ();
    MRSG_set_task_cost_f (my_task_cost_function);
    MRSG_main ("platform.xml", "deploy.xml", "mrsg.conf");

    return 0;
}

