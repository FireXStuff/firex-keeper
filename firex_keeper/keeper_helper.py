"""
    Utility functions for the firex_keeper package.
"""
import os

from firexapp.submit.uid import Uid


def get_keeper_dir(logs_dir):
    return os.path.join(logs_dir, Uid.debug_dirname, 'keeper')
