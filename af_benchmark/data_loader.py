import glob
from dbs.apis.dbsClient import DbsApi
# for full list of DBS APIs: https://twiki.cern.ch/twiki/bin/viewauth/CMS/DBS3APIInstructions

def get_file_list(cls):
    mode = cls.config.get('data-access.mode', 'local')

    if mode == 'explicit-files':
        file_list = cls.config.get('data-access.files', [])

    elif mode == 'explicit-dirs':
        dirs = cls.config.get('data-access.directories', [])
        file_list = []
        for dir in dirs:
            file_list.extend(glob.glob(dir+"/**/*.root", recursive = True))

    elif mode == 'dbs-datasets':
        dbsdatasets = cls.config.get('data-access.datasets', [])
        xrootdserver = cls.config.get('data-access.xrootdserver', 'eos.cms.rcac.purdue.edu:1094')
        dbs = DbsApi('https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
        file_list = [
            "root://"+xrootdserver+"/"+file['logical_file_name']
                for dataset in dbsdatasets
                for file in dbs.listFiles(dataset=dataset)
        ]

    elif mode == 'dbs-blocks':
        dbsblocks = cls.config.get('data-access.blocks', [])
        xrootdserver = cls.config.get('data-access.xrootdserver', 'eos.cms.rcac.purdue.edu:1094')
        dbs = DbsApi('https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
        file_list = [
            "root://"+xrootdserver+"/"+file['logical_file_name']
                for block in dbsblocks
                for file in dbs.listFiles(block_name=block)
        ]

    elif mode == 'dbs-files':
        dbsfiles = cls.config.get('data-access.files', [])
        xrootdserver = cls.config.get('data-access.xrootdserver', 'cms-xcache.rcac.purdue.edu:1094')
        dbs = DbsApi('https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
        file_list = ["root://"+xrootdserver+"/"+file for file in dbsfiles]

    else:
        raise NotImplementedError(
            f"Data access mode {mode} not implemented"
        )

    cls.n_files = len(file_list)
    return file_list
