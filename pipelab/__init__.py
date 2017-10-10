from __future__ import absolute_import, division, print_function

import pipelab.task_collection
import pipelab.context

# Once everything has been loaded, call init 
# to load all entry points
pipelab.context.init()

from pipelab.task_collection import Function, Pipeline
from pipelab.context import set_options

__all__ = [
	'Function',
	'Pipeline',
	'set_options'
]
