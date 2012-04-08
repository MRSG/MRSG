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
#include "mrsg.h"

void MRSG_init (void)
{
    user.task_cost_f = NULL;
    user.dfs_f = default_dfs_f;
}

void MRSG_set_task_cost_f ( double (*f)(enum phase_e phase, size_t tid, size_t wid) )
{
    user.task_cost_f = f;
}

void MRSG_set_dfs_f ( void (*f)(char** dfs_matrix, size_t chunks, size_t workers, int replicas) )
{
    user.dfs_f = f;
}

void MRSG_set_map_output_f ( int (*f)(size_t mid, size_t rid) )
{
    user.map_output_f = f;
}

