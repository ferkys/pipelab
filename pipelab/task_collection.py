"""
Ready available components to be used in pipelines
"""
from __future__ import absolute_import, division, print_function

from .Task import Task
from .property import TimerProperty, EntryPointProperty, DependProperty
from .context import set_options, _globals
from .utils import *

from zenlog import log as logger
import time
import sqlalchemy
import pandas as pd
from joblib import Memory

def trace_vars(vars):
    if isinstance(vars, dict):
        for k, v in vars.iteritems():
            logger.info("Trace argument key: %s" % k)
            trace_vars(v)
    elif isinstance(vars, list) or isinstance(vars, tuple):
        for v in vars:
            logger.info("Trace argument")
            trace_vars(v)
    elif isinstance(vars, basestring):
        logger.info("Value: %s" % vars)

    else:
        logger.info("Unknown type")
        logger.info("Value: {}".format(vars))


class Function(Task):
    """ Function definition is:

    {'Function':
        {
            'id': 'id_for_function',
            'func': 'package.module:function',
            'depends': ['taskid_1', 'taskid_2'] #optional
        }
    }

    Or equivalent in YAML format
    """
    __mandatory__ = ['func']
    __optional__ = ['depends']
    __options__ = dict(
        logging=True,
        trace_input=False,
        trace_output=False,
        save=False,
    )

    func = EntryPointProperty("func")
    depends = DependProperty("depends")

    def __init__(self, id, func, depends=[], options={}):        
        super(Function, self).__init__(id, options)
        self.func = func                
        self.depends = depends
        

    def run(self, *args, **kwargs):
        with set_options(self.id, **self.options) as conf:            
            if conf.get('logging'):
                logger.info('Executing {}'.format(self.id))

            if conf.get('trace_input'):
                logger.info('Tracing %s input' % self.id)
                trace_vars(args)
                trace_vars(kwargs)

            result = self.func(*args, **kwargs)

            if conf.get('trace_output'):
                logger.info('Tracing %s output' % self.id)
                trace_vars(result)

            if conf['logging']:
                logger.info('Finish {}'.format(self.id))            
            return result


class Pipeline(Task):
    """ Pipeline definition:

    {'Pipeline':
        {
            'options': {
                ...
            },
            'tasks': {
                'Component1': {
                    ...atts...
                },
    
                'Component2': {
                    ...atts...
                }
            }
        }
    }
    """
    __mandatory__ = ['pipe', 'output']
    __optional__ = []
    __options__ = dict()
    
    def __init__(self, id, pipe, output, options={}):
        super(Pipeline, self).__init__(id, options)
        self.pipe = pipe
        self.output = output

    def run(self, get):
        dsk = self.to_dask()
        return get(dsk, self.output)

    def _get_component(self, key):
        assert key in _globals['tasks'], (
            "Unknown component %s" % key
        )
        return _globals['tasks'][key]

    def to_dask(self):
        dask_pipe = {}

        for c in self.pipe:                     
            assert type(c) == dict, "Component is not a dictionary"
            assert len(c) == 1, "Component should have only one global key"            

            component_type = c.keys()[0]            
            assert component_type in _globals['tasks'], (
                "Unknown component %s" % component_type
            )
            task = self._get_component(component_type).from_dict(c)            
            dask_pipe[task.id] = task.to_dask()
            
        return dask_pipe


class OracleSQL(Task):
    """ Function definition is:

    {'Oracle':
        {
            'id': 'Get active clients',
            'database': 'dwh_produccion',
            'user': 'dlabusr_admin',
            'SQL': '... sql statement ...',
            'depends': ['taskid_1', 'taskid_2'] #optional
        }
    }

    Or equivalent in YAML format
    """
    __mandatory__ = ['database', 'sql', 'user']
    __optional__ = ['depends']
    __options__ = dict(
        logging=True,
        cached=False,
        verbose_caching=False,
        tmpdir='/tmp/'
    )    
    depends = DependProperty("depends")

    def __init__(self, id, sql, database, user, depends=[], options={}):
        super(OracleSQL, self).__init__(id, options)
        logger.info("Setting up Oracle component <%s>" % id)
        self.sql = sql
        self.depends = depends
        self.database = database
        self.user = user

        if options.get('cached'):
            verbose = 1 if options.get('verbose_caching') else 0
            
            self.mem = Memory(cachedir='/tmp/', verbose=verbose)            
            self.run = self.mem.cache(self.run)

        pwd = "RqDERHMz3H2lxn0K7uqb"
        connstring = 'oracle+cx_oracle://{user}:{pwd}@{database}'.format(
            user=user,
            pwd=pwd,
            database=database
        )
        self.engine = sqlalchemy.create_engine(connstring)        
    
    def run(self, *args, **kwargs):
        with set_options(self.id, **self.options) as conf:            
            conn = self.engine.connect()
            df = pd.read_sql(self.sql, conn)
            conn.close()
        return df

            





        
        

