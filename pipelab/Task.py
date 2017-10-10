from __future__ import absolute_import, division, print_function

from .property import TimerProperty, EntryPointProperty, DependProperty
from .utils import *

import uuid
import time
import itertools as it
import cytoolz as ct
from zenlog import log as logger
from collections import namedtuple


class Task(object):
    __mandatory__ = ['id']
    timer = TimerProperty("timer")
    config = Config()    

    def __init__(self, id, options={}):
        super(Task, self).__init__()
        self.options = options
        self.id = id
        self.start_time = None
        self.end_time = None
        self.cache = False        
        self.depends = []

        '''Check mandatory attributes of any task
        '''
        Task.check_attributes(self)
        
        # if no options set, use the default ones defined for the task
        if not self.options:        
            self.options = self.__options__


    def __call__(self, *args, **kwargs):
        logger.info('Starting timing for %s' % self.id)
        self.timer = 0
        self.start_time = self.timer
        try:
            self.result = self.run(*args, **kwargs)
            
            return self.result
        except Exception as e:
            logger.critical('Critical ({})'.format(e))
            raise
        finally:
            self.end_time = self.timer
            logger.info('Stopping timer. Performance = %d' % (
                (self.end_time - self.start_time) * 1.000
            ))

    @staticmethod
    def help():
        return ("Every pipeline component should be declared in a "
                "dictionary whose first level is the same name as "
                "the component. Example for 'Function' component: "
                "\n{'Function': "
                "\n\t{ ... } "
                "\n}"
        )

    @classmethod
    def from_dict(cls, dct):
        comp_name = cls.__name__
        mandatory = cls.__mandatory__ + Task.__mandatory__

        Task.check_attributes(cls)

        if comp_name in dct:
            comp_dct = dct[comp_name]
            
            if all([a in comp_dct for a in mandatory]):
                rendered_dct = cls.config.render(comp_dct)
                return cls(**rendered_dct)
            else:
                raise AttributeError(cls.__doc__)
        else:
            raise NameError(Task.help())

    def to_dask(self):        
        return tuple([self] + self.depends)

    @staticmethod
    def check_attributes(cls):        
        msg = "\n".join([
            "Be sure that class {cls} has correctly defined the following",
            "mandatory attributes:",
            "",
            "* __mandatory__",
            "* __optional__",
            "",
            "* run"    
        ])

        try:
            getattr(cls, '__mandatory__')
            getattr(cls, '__optional__')
            getattr(cls, '__options__')      
            getattr(cls, 'run')
        except AttributeError as e:
            raise AttributeError(msg.format(cls=cls))