# -*- coding: utf-8 -*-

"""Script for creation of relations between entities."""

import pandas as pd
import ast
from tqdm import tqdm

from py2neo import Relationship
from py2neo.database import Transaction


def add_relations(
    df: pd.DataFrame,
    node_mapping_dict: dict,
    tx: Transaction
) -> None:
    """Add relations based on template"""

    COLS = [
        'EXPID',
        'CPD_ID',
        'BATCH_ID',
        'SITE',
        'BIOMATERIAL',
        'BACTERIAL_STRAIN_NAME',
        'EXPERIMENT_TYPE',
        'RESULT_TYPE',
        'STATISTICAL_METHOD',
        'RESULT_OPERATOR',
        'RESULT_VALUE',
        'RESULT_STATUS',
        'RESULT_UNIT_annotation',
        'COMMENTS'
    ]

    for rows in tqdm(df[COLS].values, desc="Populating graph with relations"):
        (
            exp_id,
            cpd_id,
            batch_id,
            site_id,
            biomaterial,
            strain_name,
            exp_type,
            result_type,
            method,
            operator,
            value,
            status,
            unit_dict,
            comments
        ) = rows

        """Specimen -> Bacteria edge"""
        if pd.notna(biomaterial) and pd.notna(strain_name):
            specimen_node = node_mapping_dict["Specimen"][biomaterial]
            bacteria_node = node_mapping_dict["Bacteria"][strain_name]
            rel = Relationship(specimen_node, "ASSOCIATED", bacteria_node)
            tx.create(rel)

        """Partner -> Compounds edge"""
        if pd.notna(site_id) and pd.notna(cpd_id):
            partner_node = node_mapping_dict["Partner"][site_id]
            compound_node = node_mapping_dict["Compound"][cpd_id]
            rel = Relationship(partner_node, "ASSOCIATED", compound_node)
            tx.create(rel)

        """Partner -> Experiment edge"""
        if pd.notna(exp_id) and pd.notna(site_id):
            partner_node = node_mapping_dict["Partner"][site_id]
            exp_node = node_mapping_dict["Experiment"][exp_id]
            rel = Relationship(partner_node, "ASSOCIATED", exp_node)
            tx.create(rel)

        """Bacteria -> Compounds edge"""
        if pd.notna(strain_name) and pd.notna(cpd_id):
            compound_node = node_mapping_dict["Compound"][cpd_id]
            bacteria_node = node_mapping_dict["Bacteria"][strain_name]
            rel = Relationship(compound_node, "ASSOCIATED", bacteria_node)
            tx.create(rel)

        """Compound -> Batch edge"""
        if pd.notna(cpd_id) and pd.notna(batch_id):
            compound_node = node_mapping_dict["Compound"][cpd_id]
            batch_node = node_mapping_dict["Batch"][batch_id]
            rel = Relationship(compound_node, "ASSOCIATED", batch_node)
            tx.create(rel)

        """Batch -> Experiment type edge"""
        if pd.notna(batch_id) and pd.notna(exp_type):
            batch_node = node_mapping_dict["Batch"][batch_id]
            exp_type_node = node_mapping_dict["Experiment type"][exp_type]
            rel = Relationship(batch_node, "ASSOCIATED", exp_type_node)
            tx.create(rel)

        """Experiment type -> Experiment edge"""
        if pd.notna(exp_type) and pd.notna(exp_id):
            exp_type_node = node_mapping_dict["Experiment type"][exp_type]
            exp_node = node_mapping_dict["Experiment"][exp_id]
            rel = Relationship(exp_type_node, "ASSOCIATED", exp_node)
            tx.create(rel)

        """Experiment -> Result edge"""
        if pd.notna(exp_id) and pd.notna(result_type):
            exp_node = node_mapping_dict["Experiment"][exp_id]
            result_node = node_mapping_dict["Result"][result_type]

            if pd.isna(unit_dict):
                unit_dict = {}
            elif not isinstance(unit_dict, dict):
                unit_dict = ast.literal_eval(unit_dict.replace('nan', 'None'))

            annotation = {}

            if pd.notna(method):
                annotation['statistical method'] = method
            if pd.notna(operator):
                annotation['result operator'] = operator
            if pd.notna(value):
                annotation['result value'] = value
            if pd.notna(status):
                annotation['result status'] = status
            if pd.notna(unit_dict):
                annotation.update(unit_dict)

            if pd.notna(value) and pd.notna(operator) and 'name' in unit_dict:
                annotation['result'] = str(operator) + str(value) + unit_dict['name']

            if pd.notna(comments):
                annotation['comments'] = comments

            rel = Relationship(exp_node, "ASSOCIATED", result_node, **annotation)
            tx.create(rel)

