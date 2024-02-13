import glob

def get_file_list(cls):
    mode = cls.config.get('data-access.mode', 'local')
    if mode == 'local':
        file_list = cls.config.get('data-access.files', [])
    elif mode == 'local_dir':
        files_dir = cls.config.get('data-access.files_dir', "")
        file_list = glob.glob(files_dir+"/*.root")
    else:
        raise NotImplementedError(
            f"Data access modes other than 'local' and 'local_dir' are not yet implemented"
        )
        
    return file_list