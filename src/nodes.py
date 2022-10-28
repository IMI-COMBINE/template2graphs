# -*- coding: utf-8 -*-

"""Script to create different nodes in the data"""


import pandas as pd
from py2neo import Node
from py2neo.database import Transaction

pd.set_option('display.max_columns', None)


def add_nodes(
    tx: Transaction,
    df: pd.DataFrame
) -> dict:
    """Add nodes based on template data."""

    # print(df)

    node_dict = {
        'Specimen': {},
        'Bacteria': {},
        'Partner': {},
        'Compound': {},
        'Batch': {},
        'Experiment type': {},
        'Experiment': {},
        'Result': {},
    }

    # Biomaterials
    for species_name, specimen_annotation in df[['SPECIES_NAME', 'BIOMATERIAL_annotation']].values:
        specimen_annotation['species_type'] = species_name

        node_dict["Specimen"][specimen_annotation['name']] = Node(
            "Specimen", **specimen_annotation
        )
        tx.create(node_dict["Specimen"][specimen_annotation['name']])

    # Bacterial strains
    for strain_site, bact_annotation in df[
        ['BACTERIAL_STRAIN_SITE_REF', 'BACTERIAL_STRAIN_NAME_annotation']
    ].values:
        # Omit the rows with no metadata dictionary
        if isinstance(bact_annotation, str):
            continue

        bact_annotation['strain site'] = strain_site

        node_dict["Bacteria"][bact_annotation['name']] = Node(
            "Bacteria", **bact_annotation
        )
        tx.create(node_dict["Bacteria"][bact_annotation['name']])

    # Partner
    for site_idx, site_provenance in df[['SITE', 'PROVENANCE']].values:
        site_annotation = {}

        if pd.notna(site_provenance):
            site_annotation['site contact'] = site_provenance

        node_dict["Partner"][site_idx] = Node(
            "Partner", **site_annotation
        )
        tx.create(node_dict["Partner"][site_idx])

    # Compound
    for compound_idx, compound_exp_idx in df[['CPD_ID', 'EXT_CPD_ID']].values:
        compound_annotation = {}

        if pd.notna(compound_exp_idx):
            compound_annotation['compound experimental id'] = compound_exp_idx

        node_dict["Compound"][compound_idx] = Node(
            "Compound", **compound_annotation
        )
        tx.create(node_dict["Compound"][compound_idx])

    # Batch
    for batch_idx, batch_exp_idx in df[['BATCH_ID', 'EXT_BATCH_ID']].values:
        batch_annotation = {}

        if pd.notna(batch_exp_idx):
            batch_annotation['batch experimental id'] = batch_exp_idx

        node_dict["Batch"][batch_idx] = Node(
            "Batch", **batch_annotation
        )
        tx.create(node_dict["Batch"][batch_idx])

    # Experiment type
    for experiment_type_name, experiment_type_annotation in df[
        ['EXPERIMENT_TYPE', 'EXPERIMENT_TYPE_annotation']
    ].values:

        experiment_type_annotation['experiment type'] = experiment_type_name

        node_dict["Experiment type"][experiment_type_annotation['name']] = Node(
            "Experiment type", **experiment_type_annotation
        )
        tx.create(node_dict["Experiment type"][experiment_type_annotation['name']])
    
    # Experiment
    for experiment_id, experiment_date, experiment_protocol, experiment_ctrl, experiment_medium in df[
        ['EXPID', 'EXPERIMENT_DATE', 'PROTOCOL_NAME', 'CONTROL_GROUP', 'MEDIUM_annotation']
    ].values:
        experiment_annotation = {}

        if pd.notna(experiment_date):
            experiment_annotation['experiment date'] = experiment_date
        if pd.notna(experiment_protocol):
            experiment_annotation['experiment protocol'] = experiment_protocol
        if pd.notna(experiment_ctrl):
            experiment_annotation['experiment control group'] = experiment_ctrl
        if pd.notna(experiment_medium):
            experiment_annotation['experiment medium'] = experiment_medium

        node_dict["Experiment"][experiment_id] = Node(
            "Experiment", **experiment_annotation
        )
        tx.create(node_dict["Experiment"][experiment_id])
   
    # Result
    for result_type, statistical_method, result_operator, result_value, result_status, result_unit_annotation in df[['RESULT_TYPE', 'STATISTICAL_METHOD', 'RESULT_OPERATOR', 'RESULT_VALUE', 'RESULT_STATUS','RESULT_UNIT_annotation']].values:
        Result_annotation = {}

        if pd.notna(statistical_method):
            Result_annotation['statistical method'] = statistical_method
        if pd.notna(result_operator):
            Result_annotation['result operator'] = result_operator
        if pd.notna(result_value):
            Result_annotation['result value'] = result_value
        if pd.notna(result_status):
            Result_annotation['result status'] = result_status
        if pd.notna(result_unit_annotation):
            Result_annotation['result unit'] = result_unit_annotation

        node_dict["Result"][result_type] = Node(
            "Result", **Result_annotation
        )
        tx.create(node_dict["Result"][result_type])

    return node_dict
