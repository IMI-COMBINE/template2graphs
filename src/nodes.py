# -*- coding: utf-8 -*-

"""Script to create different nodes in the data"""

import pandas as pd
import ast
from py2neo import Node
from py2neo.database import Transaction

pd.set_option('display.max_columns', None)


def add_nodes(
        tx: Transaction,
        df: pd.DataFrame,
        node_dict: dict
) -> dict:
    """Add nodes based on template data."""

    # Animal species
    for species_annotation in df['SPECIES_NAME_annotation'].values:
        if pd.isna(species_annotation):
            continue
        elif not isinstance(species_annotation, dict):
            if species_annotation == '':  # empty strings
                continue
            species_annotation = ast.literal_eval(
                species_annotation.replace('nan', 'None')
            )
        elif isinstance(species_annotation, str):  # Omit the rows with no metadata dictionary
            continue

        if len(species_annotation) < 0:
            continue

        if species_annotation['name'] in node_dict['Animal species']:
            continue

        node_dict["Animal species"][species_annotation['name']] = Node(
            "Animal species", **species_annotation
        )
        tx.create(node_dict["Animal species"][species_annotation['name']])

    # Animal group
    if 'GROUP_DESCRIPTION' in df.columns:
        for group_description, housing_cage_no_animals, housing_cage_size, housing_food, housing_food_restricted, \
            housing_food_supplement, housing_light_dark_cycle, bacterial_strain_dose in \
                df[['GROUP_DESCRIPTION', 'HOUSING_CAGE_NO_ANIMALS', 'HOUSING_CAGE_SIZE', 'HOUSING_FOOD',
                    'HOUSING_FOOD_RESTRICTED', 'HOUSING_FOOD_SUPPLEMENT', 'HOUSING_LIGHT_DARK_CYCLE',
                    'BACTERIAL_STRAIN_DOSE']].values:

            if group_description in node_dict['Animal group']:
                continue

            animal_group_annotation = {}

            if pd.notna(group_description):
                animal_group_annotation['animal group'] = group_description
            if pd.notna(housing_cage_no_animals):
                animal_group_annotation['no. animals (housing cage)'] = housing_cage_no_animals
            if pd.notna(housing_cage_size):
                animal_group_annotation['cage size'] = housing_cage_size
            if pd.notna(housing_food):
                animal_group_annotation['food'] = housing_food
            if pd.notna(housing_food_restricted):
                animal_group_annotation['food restriction'] = housing_food_restricted
            if pd.notna(housing_food_supplement):
                animal_group_annotation['food supplement'] = housing_food_supplement
            if pd.notna(housing_light_dark_cycle):
                animal_group_annotation['light dark cycle'] = housing_light_dark_cycle
            if pd.notna(bacterial_strain_dose):
                animal_group_annotation['bacterial strain dose'] = bacterial_strain_dose

            node_dict["Animal group"][group_description] = Node(
                "Animal group", **animal_group_annotation
            )
            tx.create(node_dict["Animal group"][group_description])

    # Animal number
    if 'ANIMAL' in df.columns:
        for animal, animal_id, animal_sex_annotation, animal_strain, animal_vendor, animal_bodyweight_range, \
            animal_bodyweight_mean, animal_age_range in \
                df[['ANIMAL', 'ANIMAL_ID', 'ANIMAL_SEX_annotation', 'ANIMAL_STRAIN', 'ANIMAL_VENDOR',
                    'ANIMAL_BODYWEIGHT_RANGE', 'ANIMAL_BODYWEIGHT_MEAN', 'ANIMAL_AGE_RANGE']].values:

            if animal in node_dict['Animal number']:
                continue

            if pd.isna(animal_sex_annotation) or animal_sex_annotation == '':
                animal_sex_annotation = {}
            elif not isinstance(animal_sex_annotation, dict):
                animal_sex_annotation = ast.literal_eval(animal_sex_annotation.replace('nan', 'None'))

            animal_annotation = {}

            if pd.notna(animal):
                animal_annotation['animal'] = animal
            if pd.notna(animal_id):
                animal_annotation['animal id'] = animal_id
            if pd.notna(animal_sex_annotation):
                animal_annotation.update(animal_sex_annotation)
            if pd.notna(animal_age_range):
                animal_annotation['age range'] = animal_age_range
            if pd.notna(animal_strain):
                animal_annotation['animal strain'] = animal_strain
            if pd.notna(animal_vendor):
                animal_annotation['animal vendor'] = animal_vendor
            if pd.notna(animal_bodyweight_mean):
                animal_annotation['body weight mean'] = animal_bodyweight_mean
            if pd.notna(animal_bodyweight_range):
                animal_annotation['body weight range'] = animal_bodyweight_range

            node_dict["Animal number"][animal] = Node(
                "Animal number", **animal_annotation
            )
            tx.create(node_dict["Animal number"][animal])

    # In-vivo study type
    if 'STUDY_TYPE_annotation' in df.columns:
        for study_type_annotation in df['STUDY_TYPE_annotation'].values:
            if pd.isna(study_type_annotation):
                continue
            elif not isinstance(study_type_annotation, dict):
                if study_type_annotation == '':  # empty strings
                    continue
                study_type_annotation = ast.literal_eval(
                    study_type_annotation.replace('nan', 'None')
                )
            elif isinstance(study_type_annotation, str):  # Omit the rows with no metadata dictionary
                continue

            if len(study_type_annotation) < 0:
                continue

            if study_type_annotation['name'] in node_dict['In-vivo study type']:
                continue

            node_dict["In-vivo study type"][study_type_annotation['name']] = Node(
                "In-vivo study type", **study_type_annotation
            )
            tx.create(node_dict["In-vivo study type"][study_type_annotation['name']])

    # Study
    if 'STUDY_PROTOCOL_NAME' in df.columns:
        for study_id, experiment_id, study_protocol_name, study_start_date, provenance_invivo, \
            project_licence_number in df[['STUDYID', 'EXPID', 'STUDY_PROTOCOL_NAME', 'STUDY_START_DATE',
                                          'PROVENANCE', 'PROJECT_LICENCE_NUMBER']].values:

            if study_id in node_dict['Study']:
                continue

            study_annotation = {}

            if pd.notna(study_id):
                study_annotation['study id'] = study_id
            if pd.notna(experiment_id):
                study_annotation['experiment id'] = experiment_id
            if pd.notna(study_protocol_name):
                study_annotation['study protocol name'] = study_protocol_name
            if pd.notna(study_start_date):
                study_annotation['study start date'] = study_start_date
            if pd.notna(provenance_invivo):
                study_annotation['provenance'] = provenance_invivo
            if pd.notna(project_licence_number):
                study_annotation['project licence number'] = project_licence_number

            node_dict["Study"][study_id] = Node(
                "Study", **study_annotation
            )
            tx.create(node_dict["Study"][study_id])

    # Biomaterials/Specimen
    for specimen_annotation in df['BIOMATERIAL_annotation'].values:
        if pd.isna(specimen_annotation):
            continue
        elif not isinstance(specimen_annotation, dict):
            if specimen_annotation == '':  # empty strings
                continue
            specimen_annotation = ast.literal_eval(
                specimen_annotation.replace('nan', 'None')
            )
        elif isinstance(specimen_annotation, str):  # Omit the rows with no metadata dictionary
            continue

        if len(specimen_annotation) < 0:
            continue

        if specimen_annotation['name'] in node_dict['Specimen']:
            continue

        node_dict["Specimen"][specimen_annotation['name']] = Node(
            "Specimen", **specimen_annotation
        )
        tx.create(node_dict["Specimen"][specimen_annotation['name']])

    # Bacterial strains
    if 'BACTERIAL_STRAIN_SITE_REF' not in df.columns:
        for bact_annotation, bact_dose, bact_RoA in \
                df[['BACTERIAL_STRAIN_NAME_annotation', 'BACTERIAL_STRAIN_DOSE',
                    'INFECTION_ROUTE_annotation']].values:
            if pd.isna(bact_annotation):
                continue
            elif not isinstance(bact_annotation, dict):
                if bact_annotation == '':  # empty strings
                    continue
                bact_annotation = ast.literal_eval(
                    bact_annotation.replace('nan', 'None')
                )
            elif isinstance(bact_annotation, str):  # Omit the rows with no metadata dictionary
                continue

            if len(bact_annotation) < 0:
                continue

            if bact_annotation['name'] in node_dict['Bacteria']:
                continue

            if 'BACTERIAL_STRAIN_DOSE' in df.columns:
                bact_annotation['bacterial strain dose'] = bact_dose
            # if 'INFECTION_ROUTE_annotation' in df.columns:
            #     bact_annotation['infection route of administration'] = bact_RoA

            node_dict["Bacteria"][bact_annotation['name']] = Node(
                "Bacteria", **bact_annotation
            )
            tx.create(node_dict["Bacteria"][bact_annotation['name']])

    else:
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
            elif isinstance(bact_annotation, str):  # Omit the rows with no metadata dictionary
                continue

            if len(bact_annotation) < 0:
                continue

            if bact_annotation['name'] in node_dict['Bacteria']:
                continue

            if 'BACTERIAL_STRAIN_SITE_REF' in df.columns:
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

        if pd.notna(compound_idx):
            compound_annotation['compound id'] = compound_idx
        if pd.notna(compound_ext_idx):
            compound_annotation['compound external id'] = compound_ext_idx

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
    for experiment_type_annotation in df['EXPERIMENT_TYPE_annotation'].values:
        if pd.isna(experiment_type_annotation):
            continue
        elif not isinstance(experiment_type_annotation, dict):
            if experiment_type_annotation == '':  # empty strings
                continue
            experiment_type_annotation = ast.literal_eval(
                experiment_type_annotation.replace('nan', 'None')
            )
        elif isinstance(experiment_type_annotation, str):  # Omit the rows with no metadata dictionary
            continue

        if len(experiment_type_annotation) < 0:
            continue

        if experiment_type_annotation['name'] in node_dict['Experiment type']:
            continue

        node_dict["Experiment type"][experiment_type_annotation['name']] = Node(
            "Experiment type", **experiment_type_annotation
        )
        tx.create(node_dict["Experiment type"][experiment_type_annotation['name']])

    # Experiment
    if 'MEDIUM_annotation' not in df.columns:
        for experiment, study_id, experiment_id, experiment_date, experiment_protocol, experiment_ctrl, \
            planned_relative_timepoint, relative_timepoint in \
                df[['Experiment', 'STUDYID', 'EXPID', 'EXPERIMENT_DATE', 'PROTOCOL_NAME',
                    'CONTROL_GROUP', 'PLANNED_RELATIVE_TIMEPOINT', 'RELATIVE_TIMEPOINT']].values:

            if experiment_id in node_dict['Experiment']:
                continue

            experiment_annotation = {}

            if 'Experiment' in df.columns and pd.notna(experiment):
                experiment_annotation['experiment no. in in-vivo study'] = experiment
            if pd.notna(study_id):
                experiment_annotation['study id'] = study_id
            if pd.notna(experiment_id):
                experiment_annotation['experiment id'] = experiment_id
            if pd.notna(experiment_date):
                experiment_annotation['experiment date'] = experiment_date
            if pd.notna(experiment_protocol):
                experiment_annotation['experiment protocol'] = experiment_protocol
            if 'PLANNED_RELATIVE_TIMEPOINT' in df.columns and pd.notna(planned_relative_timepoint):
                experiment_annotation['planned relative timepoint'] = planned_relative_timepoint
            if 'RELATIVE_TIMEPOINT' in df.columns and pd.notna(relative_timepoint):
                experiment_annotation['relative timepoint'] = relative_timepoint
            if pd.notna(experiment_ctrl):
                experiment_annotation['experiment control group'] = experiment_ctrl

            node_dict["Experiment"][experiment_id] = Node(
                "Experiment", **experiment_annotation
            )
            tx.create(node_dict["Experiment"][experiment_id])
    else:
        for study_id, experiment_id, experiment_date, experiment_protocol, replicate_num, experiment_ctrl, \
            experiment_medium in \
                df[['STUDYID', 'EXPID', 'EXPERIMENT_DATE', 'PROTOCOL_NAME',
                    'No of replicates', 'CONTROL_GROUP', 'MEDIUM_annotation']].values:

            if experiment_id in node_dict['Experiment']:
                continue

            if pd.isna(experiment_medium) or experiment_medium == '':
                experiment_medium = {}
            elif not isinstance(experiment_medium, dict):
                experiment_medium = ast.literal_eval(experiment_medium.replace('nan', 'None'))

            experiment_annotation = {}

            if pd.notna(study_id):
                experiment_annotation['study id'] = study_id
            if pd.notna(experiment_id):
                experiment_annotation['experiment id'] = experiment_id
            if pd.notna(experiment_date):
                experiment_annotation['experiment date'] = experiment_date
            if pd.notna(experiment_protocol):
                experiment_annotation['experiment protocol'] = experiment_protocol
            if 'No of replicates' in df.columns and pd.notna(replicate_num):
                experiment_annotation['no. of replicate'] = replicate_num
            if pd.notna(experiment_ctrl):
                experiment_annotation['experiment control group'] = experiment_ctrl
            if 'MEDIUM_annotation' in df.columns and pd.notna(experiment_medium):
                experiment_annotation.update(experiment_medium)

            node_dict["Experiment"][experiment_id] = Node(
                "Experiment", **experiment_annotation
            )

            tx.create(node_dict["Experiment"][experiment_id])

    # Result
    for result_type in df['RESULT_TYPE'].values:
        if result_type in node_dict['Result']:
            continue

        Result_annotation = {}

        if pd.notna(result_type):
            Result_annotation['type'] = result_type

        node_dict["Result"][result_type] = Node(
            "Result", **Result_annotation
        )
        tx.create(node_dict["Result"][result_type])

    return node_dict
