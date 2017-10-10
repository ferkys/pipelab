from ..Task import Task
from ..task_collection import Function
from nose.tools import raises
from ..utils import Config
from zenlog import log as logger


def test_config():
    a = Config()
    b = Config()
    assert id(a) == id(b)
    b.load(
    '''
    some:
      option1:
        - thing1
        - thing2
      option2:
        - thing3
        - thing4
    '''
    )
    assert a == b
    assert a._cfg == b._cfg
