import os
import glob

def recreate_dir(save_dir):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
        print(f"Directory {save_dir} created.")
    else:
        print(f"Directory {save_dir} already exists, will clean all files from it.")
        files = glob.glob(f"{save_dir}/*")
        for file in files:
            os.remove(file)