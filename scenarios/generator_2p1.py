import os, sys
import copy
import yaml
import glob

column_presets = {
    # "full_event": {
    #     # the bechmark will limit this to actual total number of columns
    #     "method": "n_columns",
    #     "values": 100000
    # },
    "main_collections": {
        "method": "collections",
        "values": ["Jet", "Photon", "Tau", "Electron", "Muon"]
    },
    "muons_only": {
        "method": "collections",
        "values": ["Muon"]
    },
    "hmm_columns": {
        "method": "column_list",
        "values": [
            "run", "luminosityBlock", "HLT_IsoMu24", "PV_npvsGood", "fixedGridRhoFastjetAll",
            "Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass", "Muon_charge", "Muon_pfRelIso04_all", "Muon_mediumId", "Muon_ptErr",
            "Electron_pt", "Electron_eta", "Electron_mvaFall17V2Iso_WP90",
            "Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass",
        ]
    }
}


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

def recreate_dir(save_dir):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
        print(f"Directory {save_dir} created.")
    else:
        print(f"Directory {save_dir} already exists, will clean all YAML files from it.")
        yaml_files = glob.glob(f"{save_dir}/*yaml")+glob.glob(f"{save_dir}/*yml")
        for file in yaml_files:
            os.remove(file)

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
            config["custom_labels"] = {
                "column_setup": label
            }

            config_name = f'config2p1_{iconf}_{label}_{n_workers}w.yaml'
            
            with open(f'{save_dir}/{config_name}', 'w') as file:
                yaml.dump(config, file, default_flow_style=False)

            iconf += 1

    print(f'Saved {iconf} config files to {save_dir}')
            
    