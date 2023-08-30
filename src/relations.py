# -*- coding: utf-8 -*-

"""Script for creation of relations between entities."""

import pandas as pd
import ast
from tqdm import tqdm
from py2neo import Relationship, Node
from py2neo.database import Transaction
pd.set_option('display.max_columns', None)


def _create_relation(
    tx: Transaction,
    entity_a: Node,
    entity_b: Node,
    relation_type: str,
    rel_props: dict = {}
):
    tx.create(
        Relationship(entity_a, relation_type, entity_b, **rel_props)
    )


def add_relations(
    invivo_df: pd.DataFrame,
    invitro_df: pd.DataFrame,
    node_mapping_dict: dict,
    tx: Transaction
) -> None:
    """Populate the relations to the graph."""

    """Animal species -> Animal group"""
    for species, animal_group_description, species_name_annotation in invivo_df[
        ['SPECIES_NAME', 'GROUP_DESCRIPTION', 'SPECIES_NAME_annotation']
    ].values:

        if animal_group_description not in node_mapping_dict["Animal group"]:
            continue

        if animal_group_description == 'NOT IN LIST (SEE COMMENT)':
            continue

        if species in node_mapping_dict["Animal species"]:
            a = node_mapping_dict["Animal species"][species]
        elif species_name_annotation in node_mapping_dict["Animal species"]:
            a = node_mapping_dict["Animal species"][species_name_annotation]
        else:
            continue

        _create_relation(
            entity_a=a,
            relation_type="ASSOCIATED",
            entity_b=node_mapping_dict["Animal group"][animal_group_description],
            tx=tx
        )

    # in-vivo
    COLS_invivo_df = [
        'EXPID', 'CPD_ID', 'BATCH_ID', 'SITE', 'BIOMATERIAL', 'BACTERIAL_STRAIN_NAME',
        'EXPERIMENT_TYPE', 'RESULT_TYPE', 'STATISTICAL_METHOD_annotation', 'RESULT_OPERATOR', 'RESULT_VALUE',
        'RESULT_STATUS', 'RESULT_UNIT_annotation', 'COMMENTS', 'TDD', 'GROUP_DESCRIPTION', 'ANIMAL',
        'ANIMAL_ID', 'PLANNED_RELATIVE_TIMEPOINT', 'RELATIVE_TIMEPOINT', 'GROUP_DESCRIPTION', 'PRETREATMENT_BATCH_ID',
        'PRETREATMENT_CPD_ID', 'PRETREATMENT_DOSE', 'PRETREATMENT_DOSING_INFO', 'PRETREATMENT_ROUTE_OF_ADMINSTRATION',
        'INFECTION_ROUTE_annotation', 'ROUTE_OF_ADMINISTRATION_annotation',
        'DOSING_INFO', 'FREQUENCY', 'STUDYID', 'STUDY_TYPE', 'DOSE', 'EXT_BATCH_ID', 'EXT_CPD_ID'
    ]

    for rows in tqdm(invivo_df[COLS_invivo_df].values, desc="Populating graph with relations based on in-vivo df"):
        (
            exp_id, cpd_id, batch_id, site_id, biomaterial, strain_name,
            exp_type, result_type, method_dict, operator, value,
            status, unit_dict, comments, total_drug_dose, animal_group_description, animal_number,
            animal_id, planned_relative_timepoint, relative_timepoint, animal_group, pretreatment_batch_id,
            pretreatment_cpd_id, pretreatment_dose, pretreatment_dosing_info, pretreatment_route_of_administration,
            infection_route_dict, roa_dict, dosing_info, frequency, study_id, study_type, treatment_dose,
            ext_batch_id, ext_cpd_id
        ) = rows

        """Study -> Animal group edge"""
        if pd.notna(study_id) and pd.notna(animal_group_description):
            if animal_group_description != 'NOT IN LIST (SEE COMMENT)':
                _create_relation(
                    entity_a=node_mapping_dict["Study"][study_id],
                    relation_type="ASSOCIATED",
                    entity_b=node_mapping_dict["Animal group"][animal_group_description],
                    tx=tx
                )

        """Animal group -> Animal number"""
        if animal_group_description != 'NOT IN LIST (SEE COMMENT)':
            if (
                animal_group_description in node_mapping_dict["Animal group"] and
                animal_number in node_mapping_dict["Animal number"]
            ):
                _create_relation(
                    entity_a=node_mapping_dict["Animal group"][animal_group_description],
                    relation_type="ASSOCIATED",
                    entity_b=node_mapping_dict["Animal number"][animal_number],
                    tx=tx
                )

        """Study type -> Study edge"""
        if pd.notna(study_type) and pd.notna(study_id):
            _create_relation(
                entity_a=node_mapping_dict["In-vivo study type"][study_type],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Study"][study_id],
                tx=tx
            )

        """Site/Partner -> Animal group edge"""
        if pd.notna(site_id) and pd.notna(animal_group_description):
            if animal_group_description != 'NOT IN LIST (SEE COMMENT)':
                _create_relation(
                    entity_a=node_mapping_dict["Partner"][site_id],
                    relation_type="ASSOCIATED",
                    entity_b=node_mapping_dict["Animal group"][animal_group_description],
                    tx=tx
                )

        """Animal group -> Bacteria edge"""
        if pd.notna(animal_group) and pd.notna(strain_name):
            if animal_group in node_mapping_dict["Animal group"] and strain_name in node_mapping_dict["Bacteria"]:
                annotation = {}

                if 'PRETREATMENT_BATCH_ID' in invivo_df.columns and pd.notna(pretreatment_batch_id):
                    annotation['pretreatment batch id'] = pretreatment_batch_id

                if 'PRETREATMENT_CPD_ID' in invivo_df.columns and pd.notna(pretreatment_cpd_id):
                    annotation['pretreatment compound'] = pretreatment_cpd_id

                if 'PRETREATMENT_DOSE' in invivo_df.columns and pd.notna(pretreatment_dose):
                    annotation['pretreatment dose (mg/kg)'] = pretreatment_dose

                if 'PRETREATMENT_DOSING_INFO' in invivo_df.columns and pd.notna(pretreatment_dosing_info):
                    annotation['pretreatment dosing information'] = pretreatment_dosing_info

                if (
                    'PRETREATMENT_ROUTE_OF_ADMINSTRATION' in invivo_df.columns and
                    pd.notna(pretreatment_route_of_administration)
                ):
                    annotation['pretreatment route of administration'] = pretreatment_route_of_administration

                if pd.isna(infection_route_dict):
                    infection_route_dict = {}
                elif not isinstance(infection_route_dict, dict):
                    if infection_route_dict != '':  # empty strings
                        infection_route_dict = ast.literal_eval(infection_route_dict.replace('nan', 'None'))

                if pd.notna(infection_route_dict):
                    annotation.update(infection_route_dict)

                _create_relation(
                    entity_a=node_mapping_dict["Animal group"][animal_group],
                    relation_type="IS INFECTED",
                    entity_b=node_mapping_dict["Bacteria"][strain_name],
                    tx=tx,
                    rel_props=annotation
                )

        """Study type -> Experiment type edge"""
        if pd.notna(study_type) and pd.notna(exp_type):
            _create_relation(
                entity_a=node_mapping_dict["In-vivo study type"][study_type],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Experiment type"][exp_type],
                tx=tx,
            )

        """Animal group -> Compound edge"""
        if pd.notna(animal_group) and pd.notna(cpd_id):
            if animal_group!= 'NOT IN LIST (SEE COMMENT)':
                annotation = {}

                if pd.notna(cpd_id):
                    annotation['compound'] = cpd_id

                if pd.notna(batch_id):
                    annotation['batch id'] = batch_id

                if pd.notna(ext_cpd_id):
                    annotation['external compound'] = ext_cpd_id

                if pd.notna(ext_batch_id):
                    annotation['external batch id'] = ext_batch_id

                if pd.notna(dosing_info):
                    annotation['treatment dosing information'] = dosing_info

                if pd.notna(frequency):
                    annotation['frequency'] = frequency

                if pd.notna(treatment_dose):
                    annotation['treatment dose (mg/kg)'] = treatment_dose

                if pd.notna(total_drug_dose):
                    annotation['total drug dose (mg/kg)'] = total_drug_dose

                if pd.notna(roa_dict):
                    if not isinstance(roa_dict, dict):
                        if roa_dict != '':  # empty strings
                            roa_dict = ast.literal_eval(roa_dict.replace('nan', 'None'))

                    annotation.update(roa_dict)

                _create_relation(
                    entity_a=node_mapping_dict["Animal group"][animal_group],
                    relation_type="IS TREATED WITH",
                    entity_b=node_mapping_dict["Compound"][cpd_id],
                    tx=tx,
                    rel_props=annotation
                )

        """Batch -> Study type edge"""
        if pd.notna(batch_id) and pd.notna(study_type):
            if batch_id != 'NOT IN LIST (SEE COMMENT)':
                _create_relation(
                    entity_a=node_mapping_dict["Batch"][batch_id],
                    relation_type="ASSOCIATED",
                    entity_b=node_mapping_dict["In-vivo study type"][study_type],
                    tx=tx,
                )

        """Animal number -> Result edge"""
        if pd.notna(animal_number) and pd.notna(result_type):
            annotation = {}

            if pd.notna(value):
                annotation['result value'] = value

            if pd.notna(operator):
                annotation['result operator'] = operator

            if pd.notna(method_dict):
                method_dict = {
                    f'statistical_{i}': j
                    for i, j in method_dict.items()
                }
                annotation.update(method_dict)

            if pd.notna(status):
                annotation['result status'] = status

            if pd.isna(unit_dict):
                unit_dict = {}

            elif not isinstance(unit_dict, dict):
                if unit_dict != '':  # empty strings
                    unit_dict = ast.literal_eval(unit_dict.replace('nan', 'None'))

            if pd.notna(unit_dict):
                annotation.update(unit_dict)

            if pd.notna(value) and pd.notna(operator) and 'name' in unit_dict:
                if unit_dict['name'] == 'No unit':
                    annotation['result'] = str(operator) + ' ' + str(value)
                else:
                    annotation['result'] = str(operator) + ' ' + str(value) + ' ' + unit_dict['name']

            if pd.notna(comments):
                annotation['comments'] = comments

            _create_relation(
                entity_a=node_mapping_dict["Animal number"][animal_number],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Result"][result_type],
                tx=tx,
                rel_props=annotation
            )

        # relations in both in-vivo and in-vitro datasets
        """Specimen -> Bacteria edge"""
        if pd.notna(biomaterial) and strain_name in node_mapping_dict["Bacteria"]:
            _create_relation(
                entity_a=node_mapping_dict["Specimen"][biomaterial],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Bacteria"][strain_name],
                tx=tx,
            )

        """Bacteria -> Compounds edge"""
        if (pd.notna(strain_name) and pd.notna(cpd_id)) and (strain_name in node_mapping_dict["Bacteria"]):
            _create_relation(
                entity_a=node_mapping_dict["Bacteria"][strain_name],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Compound"][cpd_id],
                tx=tx,
            )

        """Partner -> Compounds edge"""
        if pd.notna(site_id) and pd.notna(cpd_id):
            _create_relation(
                entity_a=node_mapping_dict["Partner"][site_id],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Compound"][cpd_id],
                tx=tx,
            )

        """Partner -> Experiment edge"""
        if pd.notna(site_id) and pd.notna(exp_id):
            _create_relation(
                entity_a=node_mapping_dict["Partner"][site_id],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Experiment"][exp_id],
                tx=tx,
            )

        """Compound -> Batch edge"""
        if pd.notna(cpd_id) and pd.notna(batch_id):
            if batch_id != 'NOT IN LIST (SEE COMMENT)':
                _create_relation(
                    entity_a=node_mapping_dict["Compound"][cpd_id],
                    relation_type="ASSOCIATED",
                    entity_b=node_mapping_dict["Batch"][batch_id],
                    tx=tx,
                )

        """Batch -> Experiment type edge"""
        if pd.notna(batch_id) and pd.notna(exp_type):
            _create_relation(
                entity_a=node_mapping_dict["Batch"][batch_id],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Experiment type"][exp_type],
                tx=tx,
            )

        """Experiment type -> Experiment edge"""
        if pd.notna(exp_type) and pd.notna(exp_id):
            _create_relation(
                entity_a=node_mapping_dict["Experiment type"][exp_type],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Experiment"][exp_id],
                tx=tx,
            )

        """Experiment -> Result edge"""
        if pd.notna(exp_id) and pd.notna(result_type):
            annotation = {}

            if pd.notna(value):
                annotation['result value'] = value

            if pd.notna(operator):
                annotation['result operator'] = operator

            if pd.notna(unit_dict):
                annotation.update(unit_dict)

            if pd.notna(method_dict):
                method_dict = ast.literal_eval(method_dict)
                method_dict = {
                    f'statistical_{i}': j
                    for i, j in method_dict.items()
                }
                annotation.update(method_dict)

            if pd.notna(status):
                annotation['result status'] = status

            if pd.notna(value) and pd.notna(operator) and 'name' in unit_dict:
                if unit_dict['name'] == 'No unit':
                    annotation['result'] = str(operator) + ' ' + str(value)
                else:
                    annotation['result'] = str(operator) + ' ' + str(value) + ' ' + unit_dict['name']

            if pd.notna(comments):
                annotation['comments'] = comments

            annotation['ELN'] = f'https://www.sciencecloud.com/experiments/notebook/experiment/{exp_id}'

            _create_relation(
                entity_a=node_mapping_dict["Experiment"][exp_id],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Result"][result_type],
                tx=tx,
                rel_props=annotation
            )


    # in-vitro
    COLS_invitro_df = [
        'EXPID', 'CPD_ID', 'BATCH_ID', 'SITE', 'BIOMATERIAL', 'BACTERIAL_STRAIN_NAME', 'EXPERIMENT_TYPE',
        'RESULT_TYPE', 'STATISTICAL_METHOD_annotation', 'RESULT_OPERATOR', 'RESULT_VALUE', 'RESULT_STATUS',
        'RESULT_UNIT_annotation', 'COMMENTS', 'SPECIES_NAME_annotation', 'SPECIES_NAME', 'STUDYID',
        'EXT_BATCH_ID', 'EXT_CPD_ID'
    ]

    for rows in tqdm(invitro_df[COLS_invitro_df].values, desc="Populating graph with in-vitro data"):
        (
            exp_id, cpd_id, batch_id, site_id, biomaterial, strain_name, exp_type,
            result_type, method_dict, operator, value, status,
            unit_dict, comments, species_name_annotation, species, study_id,
            ext_batch_id, ext_cpd_id
        ) = rows

        # relations in both in-vivo and in-vitro dataset
        """Specimen -> Bacteria edge"""
        if pd.notna(biomaterial) and pd.notna(strain_name):
            if strain_name in node_mapping_dict["Bacteria"]:
                _create_relation(
                    entity_a=node_mapping_dict["Specimen"][biomaterial],
                    relation_type="ASSOCIATED",
                    entity_b=node_mapping_dict["Bacteria"][strain_name],
                    tx=tx,
                )

        """Bacteria -> Compounds edge"""
        if cpd_id in node_mapping_dict["Compound"] and strain_name in node_mapping_dict["Bacteria"]:
            _create_relation(
                entity_a=node_mapping_dict["Bacteria"][strain_name],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Compound"][cpd_id],
                tx=tx,
            )

        """Partner -> Compounds edge"""
        if pd.notna(site_id) and pd.notna(cpd_id):
            _create_relation(
                entity_a=node_mapping_dict["Partner"][site_id],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Compound"][cpd_id],
                tx=tx,
            )

        """Partner -> Experiment edge"""
        if pd.notna(site_id) and pd.notna(exp_id):
            _create_relation(
                entity_a=node_mapping_dict["Partner"][site_id],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Experiment"][exp_id],
                tx=tx,
            )

        """Compound -> Batch edge"""
        if pd.notna(cpd_id) and pd.notna(batch_id):
            _create_relation(
                entity_a=node_mapping_dict["Compound"][cpd_id],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Batch"][batch_id],
                tx=tx,
            )

        """Batch -> Experiment type edge"""
        if pd.notna(batch_id) and pd.notna(exp_type):
            _create_relation(
                entity_a=node_mapping_dict["Batch"][batch_id],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Experiment type"][exp_type],
                tx=tx,
            )

        """Experiment type -> Experiment edge"""
        if pd.notna(exp_type) and pd.notna(exp_id):
            _create_relation(
                entity_a=node_mapping_dict["Experiment type"][exp_type],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Experiment"][exp_id],
                tx=tx,
            )

        """Experiment -> Result edge"""
        if pd.notna(exp_id) and pd.notna(result_type):
            annotation = {}

            if pd.notna(value):
                annotation['result value'] = value

            if pd.notna(operator):
                annotation['result operator'] = operator

            if pd.isna(unit_dict):
                unit_dict = {}
            elif not isinstance(unit_dict, dict):
                if unit_dict != '':  # empty strings
                    unit_dict = ast.literal_eval(unit_dict.replace('nan', 'None'))

            if pd.notna(unit_dict):
                annotation.update(unit_dict)

            if pd.notna(method_dict):
                method_dict = ast.literal_eval(method_dict)
                method_dict = {
                    f'statistical_{i}': j
                    for i, j in method_dict.items()
                }
                annotation.update(method_dict)

            if pd.notna(status):
                annotation['result status'] = status

            if pd.notna(value) and pd.notna(operator) and 'name' in unit_dict:
                if unit_dict['name'] == 'No unit':
                    annotation['result'] = str(operator) + ' ' + str(value)
                else:
                    annotation['result'] = str(operator) + ' ' + str(value) + ' ' + unit_dict['name']

            if pd.notna(comments):
                annotation['comments'] = comments

            annotation['ELN'] = f'https://www.sciencecloud.com/experiments/notebook/experiment/{exp_id}'

            _create_relation(
                entity_a=node_mapping_dict["Experiment"][exp_id],
                relation_type="ASSOCIATED",
                entity_b=node_mapping_dict["Result"][result_type],
                tx=tx,
                rel_props=annotation
            )
