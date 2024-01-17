def get_file_list(cls):
    mode = cls.config.get('data-access.mode', 'local')
    if mode == 'local':
        file_list = cls.config.get('data-access.files', [])
    else:
        raise NotImplementedError(
            f"Data access modes other than 'local' are not yet implemented"
        )
        
    return file_list