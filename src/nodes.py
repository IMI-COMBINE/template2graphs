# -*- coding: utf-8 -*-

"""Script to create different nodes in the data"""


import pandas as pd
import ast
from py2neo import Node
from py2neo.database import Transaction

pd.set_option('display.max_columns', None)


def add_nodes(
    tx: Transaction,
    df: pd.DataFrame
) -> dict:
    """Add nodes based on template data."""

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
        if not isinstance(specimen_annotation, dict):
            specimen_annotation = ast.literal_eval(specimen_annotation)

        if specimen_annotation['name'] in node_dict['Specimen']:
            continue

        specimen_annotation['species_type'] = species_name

        node_dict["Specimen"][specimen_annotation['name']] = Node(
            "Specimen", **specimen_annotation
        )
        tx.create(node_dict["Specimen"][specimen_annotation['name']])

    # Bacterial strains
    for strain_site, bact_annotation in df[
        ['BACTERIAL_STRAIN_SITE_REF', 'BACTERIAL_STRAIN_NAME_annotation']
    ].values:

        if pd.isna(bact_annotation):
            continue
        elif not isinstance(bact_annotation, dict):
            if bact_annotation == '':  # empty strings
                continue
            bact_annotation = ast.literal_eval(
                bact_annotation.replace('nan', 'None')
            )
        elif isinstance(bact_annotation, str): # Omit the rows with no metadata dictionary
            continue

        if len(bact_annotation) < 0:
            continue

        if bact_annotation['name'] in node_dict['Bacteria']:
            continue

        bact_annotation['strain site'] = strain_site

        node_dict["Bacteria"][bact_annotation['name']] = Node(
            "Bacteria", **bact_annotation
        )
        tx.create(node_dict["Bacteria"][bact_annotation['name']])

    # Partner
    for site_idx, site_provenance in df[['SITE', 'PROVENANCE']].values:
        if site_idx in node_dict['Partner']:
            continue

        site_annotation = {}

        if pd.notna(site_provenance):
            site_annotation['site contact'] = site_provenance

        if pd.notna(site_idx):
            site_annotation['name'] = site_idx

        node_dict["Partner"][site_idx] = Node(
            "Partner", **site_annotation
        )
        tx.create(node_dict["Partner"][site_idx])

    # Compound
    for compound_idx, compound_ext_idx in df[['CPD_ID', 'EXT_CPD_ID']].values:
        if compound_idx in node_dict['Compound']:
            continue

        compound_annotation = {}

        if pd.notna(compound_ext_idx):
            compound_annotation['compound external id'] = compound_ext_idx
        if pd.notna(compound_idx):
            compound_annotation['compound id'] = compound_idx

        node_dict["Compound"][compound_idx] = Node(
            "Compound", **compound_annotation
        )
        tx.create(node_dict["Compound"][compound_idx])

    # Batch
    for batch_idx, batch_ext_idx in df[['BATCH_ID', 'EXT_BATCH_ID']].values:
        if batch_idx in node_dict['Batch']:
            continue

        batch_annotation = {}

        if pd.notna(batch_ext_idx):
            batch_annotation['batch external id'] = batch_ext_idx
        if pd.notna(batch_idx):
            batch_annotation['batch id'] = batch_idx

        node_dict["Batch"][batch_idx] = Node(
            "Batch", **batch_annotation
        )
        tx.create(node_dict["Batch"][batch_idx])

    # Experiment type
    for experiment_type_name, experiment_type_annotation in df[
        ['EXPERIMENT_TYPE', 'EXPERIMENT_TYPE_annotation']
    ].values:
        print(experiment_type_name)
        if not isinstance(experiment_type_annotation, dict):
            experiment_type_annotation = ast.literal_eval(experiment_type_annotation)

        if experiment_type_annotation['name'] in node_dict['Experiment type']:
            continue

        node_dict["Experiment type"][experiment_type_annotation['name']] = Node(
            "Experiment type", **experiment_type_annotation
        )
        tx.create(node_dict["Experiment type"][experiment_type_annotation['name']])
    
    # Experiment
    for study_id, experiment_id, experiment_date, experiment_protocol, replicate_num, experiment_ctrl, experiment_medium in df[
        ['STUDYID', 'EXPID', 'EXPERIMENT_DATE', 'PROTOCOL_NAME', 'No of replicates', 'CONTROL_GROUP', 'MEDIUM_annotation']
    ].values:

        if experiment_id in node_dict['Experiment']:
            continue

        if pd.isna(experiment_medium) or experiment_medium == '':
            experiment_medium = {}
        elif not isinstance(experiment_medium, dict):
            experiment_medium = ast.literal_eval(experiment_medium.replace('nan', 'None'))

        experiment_annotation = {}

        if pd.notna(experiment_date):
            experiment_annotation['experiment date'] = experiment_date
        if pd.notna(study_id):
            experiment_annotation['study id'] = study_id
        if pd.notna(experiment_protocol):
            experiment_annotation['experiment protocol'] = experiment_protocol
        if pd.notna(replicate_num):
             experiment_annotation['no. of replicate'] = replicate_num
        if pd.notna(experiment_ctrl):
            experiment_annotation['experiment control group'] = experiment_ctrl
        if pd.notna(experiment_medium):
            experiment_annotation.update(experiment_medium)
        if pd.notna(experiment_id):
            experiment_annotation['experiment id'] = experiment_id

        node_dict["Experiment"][experiment_id] = Node(
            "Experiment", **experiment_annotation
        )
        tx.create(node_dict["Experiment"][experiment_id])
   
    # Result
    for result_type in df['RESULT_TYPE'].values:
        if result_type in node_dict['Result']:
            continue

        Result_annotation = {'type': result_type}

        node_dict["Result"][result_type] = Node(
            "Result", **Result_annotation
        )
        tx.create(node_dict["Result"][result_type])

    return node_dict
