from __future__ import absolute_import, division, print_function

from ..task_collection import Pipeline

import yaml 

def get_initial_values():
    return 4

def calculate_features(val):
    return val * 2

def independent_function():
    return 9 

def compose_output(val1, val2):
    return "received %s %s" % (val1, val2)


def test_pipe():
    yp = yaml.load(
        '''
        Pipeline:
          id: pipetest
          pipe:
            - Function:
                id: get_initial_values
                func: pipelab.tests.test_pipe:get_initial_values

            - Function:
                id: calculate_features
                func: pipelab.tests.test_pipe:calculate_features
                depends: [get_initial_values]

            - Function:
                id: independent_function
                func: pipelab.tests.test_pipe:independent_function

            - Function:
                id: compose_output
                func: pipelab.tests.test_pipe:compose_output
                depends: [calculate_features, independent_function]
          output: compose_output
        '''
    )

    p = Pipeline.from_dict(yp)
    from dask.multiprocessing import get
    output = p.run(get=get)
    assert output == "received 8 9"
