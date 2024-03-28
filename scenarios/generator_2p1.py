import copy
import yaml

from scenarios.presets import column_presets
from scenarios.utils import recreate_dir


default_config = {
    'data-access': {
        'mode': 'explicit-files',
        'files': ['/depot/cms/users/dkondra/90322FC2-4027-0E47-92E4-22307EC8EAD2.root']
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


def generate_configs(save_dir="./"):
    recreate_dir(save_dir)

    n_workers_opts = [2,4]
    # n_workers_opts = [1,2,4,8]

    iconf = 0

    for n_workers in n_workers_opts:
        for label, column_setup in column_presets.items():
            config = copy.deepcopy(default_config)
            config["executor"]["n_workers"] = n_workers
            config["processor"]["columns"] = column_setup

            # Custom labels to save to output dataframe
            config["custom_labels"] = {
                "column_setup": label
            }

            config_name = f'config2p1_{iconf}_{label}_{n_workers}w.yaml'
            
            with open(f'{save_dir}/{config_name}', 'w') as file:
                yaml.dump(config, file, default_flow_style=False)

            iconf += 1

    print(f'Saved {iconf} config files to {save_dir}')
            
    