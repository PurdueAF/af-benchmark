import copy
import yaml

from scenarios.common import column_presets
from scenarios.utils import recreate_dir


default_config = {
    'data-access': {
        'mode': 'explicit-files',
        'files': ['/eos/purdue/store/data/Run2017C/SingleMuon/NANOAOD/02Apr2020-v1/30000/1458BAF1-0467-344C-99A8-8B216A403FB8.root']
    },
    'executor': {
        'backend': 'dask-local',
        'n_workers': 1
    },
    'processor': {
        'parallelize_over': 'columns',
        'columns': {},
        'load_columns_into_memory': True,
        'worker_operation_time': 0
    }
}


def generate_configs(save_dir="./", files=[]):
    recreate_dir(save_dir)

    # n_workers_opts = [4,8]
    n_workers_opts = [1,2,4,8,16]

    iconf = 0
        
    for n_workers in n_workers_opts:
        for label, column_setup in column_presets.items():
            config = copy.deepcopy(default_config)
            config["executor"]["n_workers"] = n_workers
            config["processor"]["columns"] = column_setup
            if files:
                config["data-access"]["files"] = files

            # Custom labels to save to output dataframe
            config["custom_labels"] = {
                "column_setup": label
            }

            config_name = f'config3p2_{iconf}_{label}_{n_workers}w.yaml'
            
            with open(f'{save_dir}/{config_name}', 'w') as file:
                yaml.dump(config, file, default_flow_style=False)

            iconf += 1

    print(f'Saved {iconf} config files to {save_dir}')
