from orchestrator import setup_run

def custom(df):
    print('inside custom')
    return df

params = {
    'ReadData': {
        'params': {
            'arg1': 'test1',
            'arg2': 'test2'
        }
    },
    'Transform': {
        'params': {
            'arg1': 'test3',
            'arg2': 'test4'
        }
    },
    'run_order': ['ReadData.read', 'Transform.flatten', custom]
}


if __name__ == '__main__':
    setup_run(params)