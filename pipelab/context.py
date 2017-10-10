"""
Control global computation context
"""
from __future__ import absolute_import, division, print_function

from .utils import *

from collections import defaultdict
import functools

_globals = defaultdict(lambda: None)
_globals['callbacks'] = set()


def init():
    _globals['tasks'] = load_collections('pipelab.tasks')


class set_options(object):
    """ Set global state within controlled context
    This lets you specify various global settings in a tightly controlled
    ``with`` block.
    """
    def __init__(self, idc, **kwargs):        
        self.old = _globals.copy()
        self.idc = idc
        if idc in _globals:
            raise AttributeError("%s already existing" % idc)
        _globals[idc] = kwargs
        

    def __enter__(self):
        return _globals[self.idc]

    def __exit__(self, type, value, traceback):
        _globals.clear()
        _globals.update(self.old)
