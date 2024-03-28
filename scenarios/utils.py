import os
import glob

def recreate_dir(save_dir):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
        print(f"Directory {save_dir} created.")
    else:
        print(f"Directory {save_dir} already exists, will clean all YAML files from it.")
        yaml_files = glob.glob(f"{save_dir}/*yaml")+glob.glob(f"{save_dir}/*yml")
        for file in yaml_files:
            os.remove(file)