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

    return node_dict
