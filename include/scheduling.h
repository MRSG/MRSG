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

#ifndef SCHEDULING_H
#define SCHEDULING_H

#include "common.h"

/**
 * @brief  Chooses a map or reduce task and send it to a worker.
 * @param  phase  MAP or REDUCE.
 * @param  wid  Worker id.
 * @return Chosen task id.
 */
size_t default_scheduler_f (enum phase_e phase, size_t wid);
size_t choose_default_map_task (size_t wid);
size_t choose_default_reduce_task (size_t wid);

#endif /* !SCHEDULING_H */

// vim: set ts=8 sw=4:

