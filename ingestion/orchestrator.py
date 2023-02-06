import sys
from read import ReadData, read_args
from transform import Transform, transform_args

acceptable_class = ('ReadData', 'Transform')
other_params = ('add_later')

def setup_run(params, df = None):
    if 'run_order' not in params.keys():
        raise Exception('Missing run_order')
    if all(True for item in acceptable_class if item in params.keys() or item == 'run_order'):
        orchestrator(params, df)
    else:
        raise Exception("Non acceptable param passed")

def orchestrator(params, df):
    class_mapper = dict()
    run_order = [{'class': item.split('.')[0], 'method': item.split('.')[1]} if type(item) is str else {'class': 'Custom', 'method': item} for item in params['run_order']]
    for exec in run_order:
        if exec['class'] not in class_mapper.keys() and exec['class'] != 'Custom':
            class_mapper[exec['class']] = getattr(sys.modules[__name__], exec['class'])(params[exec['class']]['params'])
        if exec['class'] == 'ReadData':
            df = getattr(class_mapper[exec['class']],exec['method'])()
        elif exec['class'] == 'Custom':
            df = exec['method'](df)
        else:
            df = getattr(class_mapper[exec['class']],exec['method'])(df)