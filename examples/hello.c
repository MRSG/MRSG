#include <mrsg.h>

#define MAP 0
#define REDUCE 1

double my_task_cost_function (int phase, size_t tid, size_t wid)
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

