import numpy as np

def open_nanoaod(file_path, kwargs):
    method = kwargs.get('method', 'nanoevents')

    if method == 'nanoevents':
        from coffea.nanoevents import NanoEventsFactory, NanoAODSchema
        tree = NanoEventsFactory.from_root(
            file_path,
            schemaclass=NanoAODSchema.v6,
            uproot_options={"timeout": 120}
        ).events()
    else:
       raise NotImplementedError("Processing methods other than `nanoevents` are not yet implemented.")
    return tree

def validate_columns(tree, kwargs):
    columns_to_read = kwargs.get('columns_to_read', [])
    columns = {}
    for column in columns_to_read:
        if column in tree.fields:
            columns[column] = tree[column]
        elif "_" in column:
            branch, leaf = column.split("_")
            columns[column] = tree[branch][leaf]
        else:
            raise ValueError(f"Error reading column: {column}")
    return columns

def run_operation(columns, kwargs):
    operation = kwargs.get('operation', None)
    results = {}
    for name, data in columns.items():
        if operation == 'mean':        
            results[name] = np.mean(data)
    return results