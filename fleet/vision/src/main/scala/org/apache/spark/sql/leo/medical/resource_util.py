"""
Resource_util module: which supply util resource monitor functions.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import psutil


def get_cpu_state(interval=1):
    """
    Get the CPU utilization of the system
    """
    return "CPU utilziation is: " + str(psutil.cpu_percent(interval)) + "%"  # pylint:disable=superfluous-parens


def get_memory_state():
    """
    Get the memory utilization and usage of the system
    """
    ps_mem = psutil.virtual_memory()
    return ("Memory utilization is: %5s%% %6s/%s" % (
        ps_mem.percent,
        str(int(ps_mem.percent * ps_mem.total / 102400 / 1024)) + "M",
        str(int(ps_mem.total / 1024 / 1024)) + "M"
    ))
