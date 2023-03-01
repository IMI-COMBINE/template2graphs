# -*- coding: utf-8 -*-

"""Script for creation of relations between entities."""

import pandas as pd
import ast
from tqdm import tqdm
from py2neo import Relationship
from py2neo.database import Transaction


def add_relations(
        invivo_df: pd.DataFrame,
        invitro_df: pd.DataFrame,
        node_mapping_dict: dict,
        tx: Transaction
) -> None:
    """Add relations based on template"""
    # Merge node dictionaries
    # node_mapping_dict = {**invivo_node_mapping_dict, **invitro_node_mapping_dict}

    # in-vivo
    COLS_invivo_df = [
        'EXPID', 'CPD_ID', 'BATCH_ID', 'SITE', 'BIOMATERIAL', 'BACTERIAL_STRAIN_NAME',
        'EXPERIMENT_TYPE', 'RESULT_TYPE', 'STATISTICAL_METHOD_annotation', 'RESULT_OPERATOR', 'RESULT_VALUE',
        'RESULT_STATUS', 'RESULT_UNIT_annotation', 'COMMENTS', 'TDD', 'GROUP_DESCRIPTION', 'ANIMAL',
        'ANIMAL_ID', 'PLANNED_RELATIVE_TIMEPOINT', 'RELATIVE_TIMEPOINT', 'GROUP_DESCRIPTION', 'PRETREATMENT_BATCH_ID',
        'PRETREATMENT_CPD_ID', 'PRETREATMENT_DOSE', 'PRETREATMENT_DOSING_INFO', 'PRETREATMENT_ROUTE_OF_ADMINSTRATION',
        # TODO: Typo(ADMINISTRATION) may need to be fixed in the original file and accordingly here,
        'INFECTION_ROUTE_annotation', 'ROUTE_OF_ADMINISTRATION_annotation', 'SPECIES_NAME_annotation', 'SPECIES_NAME',
        'DOSING_INFO', 'FREQUENCY', 'STUDYID', 'STUDY_TYPE', 'DOSE', 'EXT_BATCH_ID', 'EXT_CPD_ID'
    ]

    for rows in tqdm(invivo_df[COLS_invivo_df].values, desc="Populating graph with relations based on in-vivo df"):
        (
            exp_id, cpd_id, batch_id, site_id, biomaterial, strain_name,
            exp_type, result_type, method_dict, operator, value,
            status, unit_dict, comments, total_drug_dose, animal_group_description, animal_number,
            animal_id, planned_relative_timepoint, relative_timepoint, animal_group, pretreatment_batch_id,
            pretreatment_cpd_id, pretreatment_dose, pretreatment_dosing_info, pretreatment_route_of_administration,
            infection_route_dict, RoA_dict, species_name_annotation, species,
            dosing_info, frequency, study_id, study_type, treatment_dose, ext_batch_id, ext_cpd_id
        ) = rows

        # relations existing only in in-vivo
        """Animal species -> Animal group edge"""
        if pd.notna(species) and pd.notna(animal_group_description):
            species_node = node_mapping_dict["Animal species"][species]
            animal_group_node = node_mapping_dict["Animal group"][animal_group_description]
            rel = Relationship(species_node, "ASSOCIATED", animal_group_node)
            tx.create(rel)

        """Animal group -> Animal number edge"""
        if pd.notna(animal_group_description) and pd.notna(animal_number):
            animal_group_node = node_mapping_dict["Animal group"][animal_group_description]
            animal_number_node = node_mapping_dict["Animal number"][animal_number]
            rel = Relationship(animal_group_node, "ASSOCIATED", animal_number_node)
            tx.create(rel)

        """Study type -> Study edge"""
        if pd.notna(study_type) and pd.notna(study_id):
            study_type_node = node_mapping_dict["In-vivo study type"][study_type]
            study_node = node_mapping_dict["Study"][study_id]
            rel = Relationship(study_type_node, "ASSOCIATED", study_node)
            tx.create(rel)

        """Study -> Animal group edge"""
        if pd.notna(study_id) and pd.notna(animal_group_description):
            study_node = node_mapping_dict["Study"][study_id]
            animal_group_node = node_mapping_dict["Animal group"][animal_group_description]
            rel = Relationship(study_node, "ASSOCIATED", animal_group_node)
            tx.create(rel)

        """Partner -> Animal group edge"""
        if pd.notna(site_id) and pd.notna(animal_group_description):
            partner_node = node_mapping_dict["Partner"][site_id]
            animal_group_node = node_mapping_dict["Animal group"][animal_group_description]
            rel = Relationship(partner_node, "ASSOCIATED", animal_group_node)
            tx.create(rel)

        """Animal group -> Bacteria edge"""
        if pd.notna(animal_group) and pd.notna(strain_name) and strain_name != '0' and strain_name != 0:
            animal_group_node = node_mapping_dict["Animal group"][animal_group]
            bacteria_node = node_mapping_dict["Bacteria"][strain_name]

            annotation = {}
            if 'PRETREATMENT_BATCH_ID' in invivo_df.columns and pd.notna(pretreatment_batch_id):
                annotation['pretreatment batch id'] = pretreatment_batch_id
            if 'PRETREATMENT_CPD_ID' in invivo_df.columns and pd.notna(pretreatment_cpd_id):
                annotation['pretreatment compound'] = pretreatment_cpd_id
            if 'PRETREATMENT_DOSE' in invivo_df.columns and pd.notna(pretreatment_dose):
                annotation['pretreatment dose (mg/kg)'] = pretreatment_dose
            if 'PRETREATMENT_DOSING_INFO' in invivo_df.columns and pd.notna(pretreatment_dosing_info):
                annotation['pretreatment dosing information'] = pretreatment_dosing_info
            if 'PRETREATMENT_ROUTE_OF_ADMINSTRATION' in invivo_df.columns and \
                    pd.notna(pretreatment_route_of_administration):
                annotation['pretreatment route of administration'] = pretreatment_route_of_administration

            if pd.isna(infection_route_dict):
                infection_route_dict = {}
            elif not isinstance(infection_route_dict, dict):
                if infection_route_dict == '':  # empty strings
                    continue
                infection_route_dict = ast.literal_eval(infection_route_dict.replace('nan', 'None'))
            if pd.notna(infection_route_dict):
                annotation.update(infection_route_dict)

            rel = Relationship(animal_group_node, "IS INFECTED", bacteria_node, **annotation)
            tx.create(rel)

        """Study type -> Experiment type edge"""
        if pd.notna(study_type) and pd.notna(exp_type):
            study_type_node = node_mapping_dict["In-vivo study type"][study_type]
            exp_type_node = node_mapping_dict["Experiment type"][exp_type]
            rel = Relationship(study_type_node, "ASSOCIATED", exp_type_node)
            tx.create(rel)

        """Animal group -> Compound edge"""
        if pd.notna(animal_group) and pd.notna(cpd_id):
            animal_group_node = node_mapping_dict["Animal group"][animal_group]
            compound_node = node_mapping_dict["Compound"][cpd_id]

            annotation = {}
            if 'CPD_ID' in invivo_df.columns and pd.notna(cpd_id):
                annotation['compound'] = cpd_id
            if 'BATCH_ID' in invivo_df.columns and pd.notna(batch_id):
                annotation['batch id'] = batch_id
            if 'EXT_CPD_ID' in invivo_df.columns and pd.notna(ext_cpd_id):
                annotation['external compound'] = ext_cpd_id
            if 'EXT_BATCH_ID' in invivo_df.columns and pd.notna(ext_batch_id):
                annotation['external batch id'] = ext_batch_id
            if 'DOSING_INFO' in invivo_df.columns and pd.notna(dosing_info):
                annotation['treatment dosing information'] = dosing_info
            if 'FREQUENCY' in invivo_df.columns and pd.notna(frequency):
                annotation['frequency'] = frequency
            if 'DOSE' in invivo_df.columns and pd.notna(treatment_dose):
                annotation['treatment dose (mg/kg)'] = treatment_dose
            if 'TDD' in invivo_df.columns and pd.notna(total_drug_dose):
                annotation['total drug dose (mg/kg)'] = total_drug_dose

            if pd.isna(RoA_dict):
                RoA_dict = {}
            elif not isinstance(RoA_dict, dict):
                if RoA_dict == '':  # empty strings
                    continue
                RoA_dict = ast.literal_eval(RoA_dict.replace('nan', 'None'))
            if pd.notna(RoA_dict):
                annotation.update(RoA_dict)

            rel = Relationship(animal_group_node, "IS TREATED WITH", compound_node, **annotation)
            tx.create(rel)

        """Batch -> Study type edge"""
        if pd.notna(batch_id) and pd.notna(study_type):
            batch_node = node_mapping_dict["Batch"][batch_id]
            study_type_node = node_mapping_dict["In-vivo study type"][study_type]
            rel = Relationship(batch_node, "ASSOCIATED", study_type_node)
            tx.create(rel)

        """Animal number -> Result edge"""
        if pd.notna(animal_number) and pd.notna(result_type):
            animal_number_node = node_mapping_dict["Animal number"][animal_number]
            result_node = node_mapping_dict["Result"][result_type]

            annotation = {}

            if pd.notna(value):
                annotation['result value'] = value
            if pd.notna(operator):
                annotation['result operator'] = operator

            if pd.isna(unit_dict):
                unit_dict = {}
            elif not isinstance(unit_dict, dict):
                if unit_dict == '':  # empty strings
                    continue
                unit_dict = ast.literal_eval(unit_dict.replace('nan', 'None'))
            if pd.notna(unit_dict):
                annotation.update(unit_dict)
            if pd.notna(method_dict):
                annotation['statistical method'] = method_dict
            if pd.notna(status):
                annotation['result status'] = status
            if pd.notna(value) and pd.notna(operator) and 'name' in unit_dict:
                annotation['result'] = str(operator) + str(value) + unit_dict['name']
            if pd.notna(comments):
                annotation['comments'] = comments

            rel = Relationship(animal_number_node, "ASSOCIATED", result_node)
            tx.create(rel)

        # relations in both in-vivo and in-vitro datasets
        """Animal species -> Specimen edge"""
        if pd.notna(species) and pd.notna(biomaterial):
            species_node = node_mapping_dict["Animal species"][species]
            specimen_node = node_mapping_dict["Specimen"][biomaterial]
            rel = Relationship(species_node, "ASSOCIATED", specimen_node)
            tx.create(rel)

        """Specimen -> Bacteria edge"""
        if (pd.notna(biomaterial) and pd.notna(strain_name)) and (strain_name in node_mapping_dict["Bacteria"]):
            specimen_node = node_mapping_dict["Specimen"][biomaterial]
            bacteria_node = node_mapping_dict["Bacteria"][strain_name]
            rel = Relationship(specimen_node, "ASSOCIATED", bacteria_node)
            tx.create(rel)

        """Bacteria -> Compounds edge"""
        if (pd.notna(strain_name) and pd.notna(cpd_id)) and (strain_name in node_mapping_dict["Bacteria"]):
            bacteria_node = node_mapping_dict["Bacteria"][strain_name]
            compound_node = node_mapping_dict["Compound"][cpd_id]
            rel = Relationship(bacteria_node, "ASSOCIATED", compound_node)
            tx.create(rel)

        """Partner -> Compounds edge"""
        if pd.notna(site_id) and pd.notna(cpd_id):
            partner_node = node_mapping_dict["Partner"][site_id]
            compound_node = node_mapping_dict["Compound"][cpd_id]
            rel = Relationship(partner_node, "ASSOCIATED", compound_node)
            tx.create(rel)

        """Partner -> Experiment edge"""
        if pd.notna(site_id) and pd.notna(exp_id):
            partner_node = node_mapping_dict["Partner"][site_id]
            exp_node = node_mapping_dict["Experiment"][exp_id]
            rel = Relationship(partner_node, "ASSOCIATED", exp_node)
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

            annotation = {}

            if pd.notna(value):
                annotation['result value'] = value
            if pd.notna(operator):
                annotation['result operator'] = operator
            if pd.notna(unit_dict):
                annotation.update(unit_dict)
            if pd.notna(method_dict):
                annotation['statistical method'] = method_dict
            if pd.notna(status):
                annotation['result status'] = status
            if pd.notna(value) and pd.notna(operator) and 'name' in unit_dict:
                annotation['result'] = str(operator) + str(value) + unit_dict['name']
            if pd.notna(comments):
                annotation['comments'] = comments

            rel = Relationship(exp_node, "ASSOCIATED", result_node, **annotation)
            tx.create(rel)

    # in-vitro
    COLS_invitro_df = [
        'EXPID', 'CPD_ID', 'BATCH_ID', 'SITE', 'BIOMATERIAL', 'BACTERIAL_STRAIN_NAME', 'EXPERIMENT_TYPE',
        'RESULT_TYPE', 'STATISTICAL_METHOD_annotation', 'RESULT_OPERATOR', 'RESULT_VALUE', 'RESULT_STATUS',
        'RESULT_UNIT_annotation', 'COMMENTS', 'SPECIES_NAME_annotation', 'SPECIES_NAME', 'STUDYID',
        'EXT_BATCH_ID', 'EXT_CPD_ID'
    ]

    for rows in tqdm(invitro_df[COLS_invitro_df].values, desc="Populating graph with relations based on in-vitro df"):
        (
            exp_id, cpd_id, batch_id, site_id, biomaterial, strain_name, exp_type,
            result_type, method_dict, operator, value, status,
            unit_dict, comments, species_name_annotation, species, study_id,
            ext_batch_id, ext_cpd_id
        ) = rows

        # relations in both in-vivo and in-vitro dataset
        """Animal species -> Specimen edge"""
        if pd.notna(species) and pd.notna(biomaterial):
            species_node = node_mapping_dict["Animal species"][species]
            specimen_node = node_mapping_dict["Specimen"][biomaterial]
            rel = Relationship(species_node, "ASSOCIATED", specimen_node)
            tx.create(rel)

        """Specimen -> Bacteria edge"""
        if (pd.notna(biomaterial) and pd.notna(strain_name)) and (strain_name in node_mapping_dict["Bacteria"]):
            specimen_node = node_mapping_dict["Specimen"][biomaterial]
            bacteria_node = node_mapping_dict["Bacteria"][strain_name]
            rel = Relationship(specimen_node, "ASSOCIATED", bacteria_node)
            tx.create(rel)

        """Bacteria -> Compounds edge"""
        if (pd.notna(biomaterial) and pd.notna(strain_name)) and (strain_name in node_mapping_dict["Bacteria"]):
            bacteria_node = node_mapping_dict["Bacteria"][strain_name]
            compound_node = node_mapping_dict["Compound"][cpd_id]
            rel = Relationship(bacteria_node, "ASSOCIATED", compound_node)
            tx.create(rel)

        """Partner -> Compounds edge"""
        if pd.notna(site_id) and pd.notna(cpd_id):
            partner_node = node_mapping_dict["Partner"][site_id]
            compound_node = node_mapping_dict["Compound"][cpd_id]
            rel = Relationship(partner_node, "ASSOCIATED", compound_node)
            tx.create(rel)

        """Partner -> Experiment edge"""
        if pd.notna(site_id) and pd.notna(exp_id):
            partner_node = node_mapping_dict["Partner"][site_id]
            exp_node = node_mapping_dict["Experiment"][exp_id]
            rel = Relationship(partner_node, "ASSOCIATED", exp_node)
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

            annotation = {}

            if pd.notna(value):
                annotation['result value'] = value
            if pd.notna(operator):
                annotation['result operator'] = operator

            if pd.isna(unit_dict):
                unit_dict = {}
            elif not isinstance(unit_dict, dict):
                if unit_dict == '':  # empty strings
                    continue
                unit_dict = ast.literal_eval(unit_dict.replace('nan', 'None'))
            if pd.notna(unit_dict):
                annotation.update(unit_dict)

            if pd.notna(method_dict):
                annotation['statistical method'] = method_dict
            if pd.notna(status):
                annotation['result status'] = status
            if pd.notna(value) and pd.notna(operator) and 'name' in unit_dict:
                annotation['result'] = str(operator) + str(value) + unit_dict['name']
            if pd.notna(comments):
                annotation['comments'] = comments

            rel = Relationship(exp_node, "ASSOCIATED", result_node, **annotation)
            tx.create(rel)
