def get_file_list(config):
    mode = config.get('mode', 'local')
    if mode == 'local':
        file_list = config.get('files', [])
    else:
        raise NotImplementedError(f"Data access modes other than 'local' are not yet implemented")
        
    return file_list