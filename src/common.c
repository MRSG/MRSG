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

XBT_LOG_EXTERNAL_DEFAULT_CATEGORY (msg_test);

size_t get_worker_id (msg_host_t worker)
{
    return (size_t) MSG_host_get_data (worker);
}

void send (const char* str, double cpu, double net, void* data, const char* mailbox)
{
    msg_task_t  msg = NULL;

    msg = MSG_task_create (str, cpu, net, data);

#ifdef VERBOSE
    if (!message_is (msg, SMS_HEARTBEAT))
	    XBT_INFO ("TX (%s): %s", mailbox, str);
#endif

    xbt_assert (MSG_task_send (msg, mailbox) == MSG_OK, "ERROR SENDING MESSAGE");
}

void send_sms (const char* str, const char* mailbox)
{
    send (str, 0.0, 0.0, NULL, mailbox);
}

msg_error_t receive (msg_task_t* msg, const char* mailbox)
{
    xbt_assert (MSG_task_receive (msg, mailbox) == MSG_OK, "ERROR RECEIVING MESSAGE");

    return MSG_OK;
}

int message_is (msg_task_t msg, const char* str)
{
    if (strcmp (MSG_task_get_name (msg), str) == 0)
	return 1;

    return 0;
}

int maxval (int a, int b)
{
    if (b > a)
	return b;

    return a;
}

