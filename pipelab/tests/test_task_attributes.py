from __future__ import absolute_import, division, print_function

from ..Task import Task
from ..task_collection import Function
from ..context import set_options, _globals
from nose.tools import raises
from ..utils import Config
from zenlog import log as logger


class SimpleTask(Task):
    mandatory = ['name']

    def __init__(self, name, predecessors=[]):
        self.name = name
        self.predecessors = []
        super(SimpleTask, self).__init__(name, predecessors)

    def run(self):
        pass

    def from_dict(self):
        pass


class NoMandatoryTask(Task):
    def __init__(self, name, predecessors):
        super(NoMandatoryTask, self).__init__(name, predecessors)


class NoRunTask(Task):
    mandatory = ['name', 'from_dict']

    def __init__(self, name, predecessors):
        super(NoRunTask, self).__init__(name, predecessors)

    def from_dict(self):
        pass


@raises(AttributeError)
def test_no_mandatory():
    t = NoMandatoryTask("t1", [])


@raises(AttributeError)
def test_no_run():
    t = NoRunTask("t1", [])


def do_something(arg1, arg2):
    return "Something {} {}".format(arg1, arg2)


def test_function():
    f = Function(
        id="f1",
        func="pipelab.tests.test_task_attributes:do_something"
    )

    assert f("arg1", "arg2") == "Something arg1 arg2"


def test_options():
    opt = dict(
                save=True,
                spark=False
    )
    f = Function(
            id="f1",
            func="pipelab.tests.test_task_attributes:do_something",
            options=opt
    )

    with set_options(f.id, **opt):
        assert _globals[f.id] == opt


@raises(NameError)
def test_fromdict_no_mandatory():
    d = {
        'Functionbla': {
            'more_keys': []
        }
    }
    f = Function.from_dict(d)


@raises(AttributeError)
def test_fromdict_missing_att():
    d = {
        'Function': {
            # missing name att
            'entry_point': 'tests:say_something',
        }
    }
    f = Function.from_dict(d)
