import glob
from dbs.apis.dbsClient import DbsApi
# for full list of DBS APIs: https://twiki.cern.ch/twiki/bin/viewauth/CMS/DBS3APIInstructions

def get_file_list(cls):
    mode = cls.config.get('data-access.mode', 'local')
    if mode == 'local':
        file_list = cls.config.get('data-access.files', [])
    elif mode == 'local_dirs':
        dirs = cls.config.get('data-access.directories', [])
        file_list = []
        for dir in dirs:
            file_list.extend(glob.glob(dir+"/**/*.root", recursive = True))
    elif mode == 'dbs_dataset':
        dbsdataset = cls.config.get('data-access.dataset', "")
        xrootdserver = cls.config.get('data-access.xrootdserver', 'eos.cms.rcac.purdue.edu:1094')
        dbs = DbsApi('https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
        file_list = ["root://"+xrootdserver+"/"+file['logical_file_name'] for file in dbs.listFiles(dataset=dbsdataset)]
    elif mode == 'dbs_block':
        dbsblock = cls.config.get('data-access.block', "")
        xrootdserver = cls.config.get('data-access.xrootdserver', 'eos.cms.rcac.purdue.edu:1094')
        dbs = DbsApi('https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
        file_list = ["root://"+xrootdserver+"/"+file['logical_file_name'] for file in dbs.listFiles(block_name=dbsblock)]
    elif mode == 'dbs_file':
        dbsfile = cls.config.get('data-access.file', "")
        xrootdserver = cls.config.get('data-access.xrootdserver', 'cms-xcache.rcac.purdue.edu:1094')
        dbs = DbsApi('https://cmsweb.cern.ch/dbs/prod/global/DBSReader')
        file_list = ["root://"+xrootdserver+"/"+dbsfile]
    else:
        raise NotImplementedError(
            f"Data access modes other than 'local' and 'local_dir' are not yet implemented"
        )

    cls.n_files = len(file_list)
    return file_list
