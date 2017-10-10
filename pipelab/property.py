from __future__ import absolute_import, division, print_function

import uuid
import time
import pkg_resources as pkg
from zenlog import log as logger


def prueba():
    print("ey! this is prueba!")

def prueba2():
    print("This is a second test!")    

class TypeProperty(object):
    def __init__(self, name, type, default=None):
        self.name = "_" + name
        self.type = type
        self.default = default if default else type()

    def __get__(self, instance, cls):
        return getattr(instance, self.name, self.default)

    def __set__(self, instance, value):
        if not isinstance(value, self.type):
            raise TypeError("Must be a %s" % self.type)
        setattr(instance, self.name, value)

    def __delete__(self, instance):
        raise AttributeError("Can't delete attribute: FunctionProperty")


class DependProperty(object):
    def __init__(self, name):
        self.name = "_" + name
        self.default = []
                
    def __get__(self, instance, cls):
        return getattr(instance, self.name, self.default)

    def __set__(self, instance, value):
        # Pending to be checked
        # if not isinstance(instance, Task):
        #    raise AttributeError("Task does not have a depends attribute")
        
        if isinstance(value, list):
            map(instance.depends.append, value)
            #instance.depends += value
        elif isinstance(value, basestring):
            instance.depends.append(value)
        else:
            print('type: %s' % type(value))
            raise AttributeError("DependProperty must be list or string")            
        setattr(instance, self.name, value)

    def __delete__(self, instance):
        raise AttributeError("Can't delete attribute: FunctionProperty")


class EntryPointProperty(object):
    def __init__(self, name):
        logger.info('FunctionType constructor {}'.format(name))
        self.name = "_" + name

    def __get__(self, instance, cls):        
        epoint = getattr(instance, self.name, None)
        if not epoint:
            raise AttributeError("EntryPoint not set")
        return epoint

    def __set__(self, instance, epoint):
        logger.info('Setting {} = {}'.format(instance, epoint))
        entrypoint = pkg.EntryPoint.parse("{}={}".format(
                self.name, epoint
        ))

        try:
            setattr(instance, self.name, entrypoint.resolve())            
        except ImportError as e:
            raise AttributeError(
                "EntryPoint %s(%s) does not exist" % (self.name, epoint)
            )

    def __delete__(self, instance):
        raise AttributeError("Can't delete attribute: FunctionProperty")


class TimerProperty(object):
    def __init__(self, name):
        self.name = "_" + name
        self.type = int
        self.default = None

    def __get__(self, instance, cls):
        start_value = getattr(instance, self.name, self.default)
        if not start_value:
            raise ValueError("Timer has not been started")

        return time.time() - start_value

    def __set__(self, instance, value):
        if not isinstance(value, int):
            raise TypeError("Timer value Must be a %s" % self.type)
        if value != 0:
            raise ValueError("Timer should be started with 0 value")
        setattr(instance, self.name, time.time())

    def __delete__(self, instance):
        raise AttributeError("Can't delete attribute: Timer")
