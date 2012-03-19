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

#ifndef MRSG_H
#define MRSG_H

#include <stdlib.h>

struct config_s {
    double  chunk_size;
    double  cpu_required_map;
    double  cpu_required_reduce;
    double  grid_average_speed;
    double  grid_cpu_power;
    double  map_out_size;
    int     chunk_count;
    int     chunk_replicas;
    int     heartbeat_interval;
    int     map_slots;
    int     number_of_maps;
    int     number_of_reduces;
    int     number_of_workers;
    int     reduce_slots;
} config;

void MRSG_init (void);

int MRSG_main (const char* plat, const char* depl, const char* conf);

void MRSG_set_task_cost_f ( double (*f)(int phase, size_t tid, size_t wid) );

void MRSG_set_dfs_f ( void (*f)(char** dfs_matrix, size_t chunks, size_t workers, int replicas) );

#endif /* !MRSG_H */
