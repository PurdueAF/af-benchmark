
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