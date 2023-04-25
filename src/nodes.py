# -*- coding: utf-8 -*-

"""Script to create different nodes in the data"""

import pandas as pd
from ast import literal_eval
from py2neo import Node
from py2neo.database import Transaction


def _format_text(text):
    replace_items = [
        ('§', ''), ('—', '-'), ('μ', 'u'), ('\n', ''), ('★', ''), ('™', ''),
        ('β', 'beta'), ('“”', '"'), ('®', ''), ('”', '"'), ('“', '"'), ('×', ''),
        ('−', '-'), ('°', ''), ('', ''), (' ', ''), ('±', ''), (' ', ''),
        ('˜', ''), ('κ', 'kappa'), ('δ', 'delta'), ('Å', 'A'), ('α', 'alpha'),
        ('λ', 'lambda'), ('‘', ''), ('”', ''), ('ε', 'epsilon'), ('═', '='), ('′', ''),
        ('σ', 'sigma'), ('ν', ''), ('ö', 'oe'), ('é', 'e'), ('í', 'i'), ("O'", 'o'), ('ø', 'o'),
        ('ñ', 'n'), ('ä', 'a'), ('π', 'pi'), ('’', ''), ('□', ''), ('□', ''), ('‘', ''), ('’', ''),
        ('γ', 'gamma'), ('″', '"'), ('≥', ''), ('¾', '3/4'), ('·', '.'), ('³', '3'), ('–', '-'),
        ('‐', '-'), ('…', ''), ('å', 'a'), ('£', ''), ('Δ', 'delta')
    ]
    for item in replace_items:
        text = text.replace(item[0], item[1])
    return text


def add_nodes(
    tx: Transaction,
    df: pd.DataFrame,
    node_dict: dict
) -> dict:
    """Add nodes based on template data."""

    # Animal species
    for specie_name, species_annotation in df[['SPECIES_NAME', 'SPECIES_NAME_annotation']].values:
        if pd.isna(specie_name):
            continue

        if pd.isna(species_annotation):
            species_annotation = {'name': specie_name}

        if isinstance(species_annotation, str):
            species_annotation = literal_eval(species_annotation)

        if species_annotation['name'] in node_dict['Animal species']:
            continue

        node_dict["Animal species"][species_annotation['name']] = Node(
            "Animal species", **species_annotation
        )
        tx.create(node_dict["Animal species"][species_annotation['name']])

    # Animal group
    if 'GROUP_DESCRIPTION' in df.columns:
        for row in df[
            ['GROUP_DESCRIPTION', 'HOUSING_CAGE_NO_ANIMALS', 'HOUSING_CAGE_SIZE', 'HOUSING_FOOD',
             'HOUSING_FOOD_RESTRICTED', 'HOUSING_FOOD_SUPPLEMENT', 'HOUSING_LIGHT_DARK_CYCLE']
        ].values:
            (
                group_description,
                housing_cage_no_animals,
                housing_cage_size,
                housing_food,
                housing_food_restricted,
                housing_food_supplement,
                housing_light_dark_cycle,
            ) = row

            if group_description in node_dict['Animal group']:
                continue

            # Skip the rows with no entries
            if group_description == 'NOT IN LIST (SEE COMMENT)' or pd.isna(group_description):
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

            node_dict["Animal group"][group_description] = Node(
                "Animal group", **animal_group_annotation
            )
            tx.create(node_dict["Animal group"][group_description])

    # Animal number
    if 'ANIMAL' in df.columns:
        for row in df[
            ['ANIMAL', 'ANIMAL_ID', 'ANIMAL_SEX_annotation', 'ANIMAL_STRAIN', 'ANIMAL_VENDOR',
             'ANIMAL_BODYWEIGHT_RANGE', 'ANIMAL_BODYWEIGHT_MEAN', 'ANIMAL_AGE_RANGE']
        ].values:
            (
                animal,
                animal_id,
                animal_sex_annotation,
                animal_strain,
                animal_vendor,
                animal_bodyweight_range,
                animal_bodyweight_mean,
                animal_age_range
            ) = row

            if pd.isna(animal):
                continue

            if animal in node_dict['Animal number']:
                continue

            animal_annotation = {}

            if pd.notna(animal):
                animal_annotation['animal'] = animal

            if pd.notna(animal_id):
                animal_annotation['animal id'] = animal_id

            if pd.notna(animal_sex_annotation):
                if isinstance(animal_sex_annotation, str):
                    animal_sex_annotation = literal_eval(animal_sex_annotation)
                animal_sex_annotation['animal sex curie'] = animal_sex_annotation.pop('curie')
                animal_sex_annotation['animal sex'] = animal_sex_annotation.pop('name')
                animal_annotation.update(animal_sex_annotation)

            if pd.notna(animal_strain):
                animal_annotation['animal strain'] = animal_strain

            if pd.notna(animal_vendor):
                animal_annotation['animal vendor'] = animal_vendor

            if pd.notna(animal_bodyweight_mean):
                animal_annotation['body weight mean'] = animal_bodyweight_mean

            if pd.notna(animal_bodyweight_range):
                animal_annotation['body weight range'] = animal_bodyweight_range

            if pd.notna(animal_age_range):
                animal_annotation['age range'] = animal_age_range

            node_dict["Animal number"][animal] = Node(
                "Animal number", **animal_annotation
            )
            tx.create(node_dict["Animal number"][animal])

    # In-vivo study type
    if 'STUDY_TYPE_annotation' in df.columns:
        for study_type, study_type_annotation in df[['STUDY_TYPE', 'STUDY_TYPE_annotation']].values:
            if pd.isna(study_type):
                continue

            if pd.isna(study_type_annotation):
                study_type_annotation = {'name': study_type}

            if isinstance(study_type_annotation, str):
                study_type_annotation = literal_eval(study_type_annotation)

            if study_type_annotation['name'] in node_dict['In-vivo study type']:
                continue

            node_dict["In-vivo study type"][study_type_annotation['name']] = Node(
                "In-vivo study type", **study_type_annotation
            )
            tx.create(node_dict["In-vivo study type"][study_type_annotation['name']])

    # Study
    if 'STUDY_PROTOCOL_NAME' in df.columns:
        for row in df[
            ['STUDYID', 'EXPID', 'STUDY_PROTOCOL_NAME', 'STUDY_START_DATE', 'PROVENANCE', 'PROJECT_LICENCE_NUMBER']
        ].values:
            (
                study_id,
                experiment_id,
                study_protocol_name,
                study_start_date,
                provenance_invivo,
                project_licence_number
            ) = row

            if pd.isna(study_id):
                continue

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
                study_annotation['provenance'] = _format_text(provenance_invivo)

            if pd.notna(project_licence_number):
                study_annotation['project licence number'] = project_licence_number

            node_dict["Study"][study_id] = Node(
                "Study", **study_annotation
            )
            tx.create(node_dict["Study"][study_id])

    # Biomaterials/Specimen
    for specimen, specimen_annotation in df[['BIOMATERIAL', 'BIOMATERIAL_annotation']].values:
        if pd.isna(specimen):
            continue

        if pd.isna(specimen_annotation):
            specimen_annotation = {'name': specimen}

        if isinstance(specimen_annotation, str):
            specimen_annotation = literal_eval(specimen_annotation)

        if specimen_annotation['name'] in node_dict['Specimen']:
            continue

        node_dict["Specimen"][specimen_annotation['name']] = Node(
            "Specimen", **specimen_annotation
        )
        tx.create(node_dict["Specimen"][specimen_annotation['name']])

    # Bacterial strains
    if 'BACTERIAL_STRAIN_SITE_REF' not in df.columns:
        for row in df[
            ['BACTERIAL_STRAIN_NAME', 'BACTERIAL_STRAIN_NAME_annotation', 'BACTERIAL_STRAIN_DOSE', 'INFECTION_ROUTE_annotation']
        ].values:
            (
                bact_strain_name,
                bact_annotation,
                bact_dose,
                bact_RoA
            ) = row

            if pd.isna(bact_strain_name) or bact_strain_name == 'NOT IN LIST (SEE COMMENT)':
                continue

            if pd.isna(bact_annotation):
                bact_annotation = {'name': bact_strain_name}

            if isinstance(bact_annotation, str):
                bact_annotation = literal_eval(bact_annotation)

            if bact_annotation['name'] in node_dict['Bacteria']:
                continue

            if pd.notna(bact_dose):
                bact_annotation['bacterial strain dose'] = bact_dose

            if pd.notna(bact_RoA):
                bact_annotation['bacterial strain ROA'] = bact_RoA

            if bact_annotation['name'] == '0':
                continue

            node_dict["Bacteria"][bact_annotation['name']] = Node(
                "Bacteria", **bact_annotation
            )
            tx.create(node_dict["Bacteria"][bact_annotation['name']])
    else:
        for row in df[['BACTERIAL_STRAIN_NAME', 'BACTERIAL_STRAIN_SITE_REF', 'BACTERIAL_STRAIN_NAME_annotation']].values:
            (
                bact_strain_name,
                strain_site,
                bact_annotation
            ) = row

            if pd.isna(bact_strain_name):
                continue

            if pd.isna(bact_annotation):
                bact_annotation = {'name': bact_strain_name}

            if isinstance(bact_annotation, str):
                bact_annotation = literal_eval(bact_annotation)

            bact_annotation['name'] = _format_text(bact_annotation['name'])

            if bact_annotation['name'] in node_dict['Bacteria']:
                continue

            if 'BACTERIAL_STRAIN_SITE_REF' in df.columns:
                if pd.notna(strain_site):
                    bact_annotation['strain site'] = strain_site

            node_dict["Bacteria"][bact_annotation['name']] = Node(
                "Bacteria", **bact_annotation
            )
            tx.create(node_dict["Bacteria"][bact_annotation['name']])

    # Partner
    for site_idx, site_provenance in df[['SITE', 'PROVENANCE']].values:
        if pd.isna(site_idx):
            continue

        if site_idx in node_dict['Partner']:
            continue

        site_annotation = {
            'name': site_idx
        }

        if pd.notna(site_provenance):
            site_annotation['site contact'] = _format_text(site_provenance)

        node_dict["Partner"][site_idx] = Node(
            "Partner", **site_annotation
        )
        tx.create(node_dict["Partner"][site_idx])

    # Compound
    for compound_idx, compound_ext_idx in df[['CPD_ID', 'EXT_CPD_ID']].values:
        if pd.isna(compound_idx) or compound_idx == 0:
            continue

        if compound_idx in node_dict['Compound']:
            continue

        compound_annotation = {
            'name': compound_idx
        }

        if pd.notna(compound_ext_idx):
            compound_annotation['compound external id'] = compound_ext_idx

        node_dict["Compound"][compound_idx] = Node(
            "Compound", **compound_annotation
        )
        tx.create(node_dict["Compound"][compound_idx])

    # Batch
    for batch_idx, batch_ext_idx in df[['BATCH_ID', 'EXT_BATCH_ID']].values:
        if pd.isna(batch_idx) or batch_idx == 'NOT IN LIST (SEE COMMENT)':
            continue

        if batch_idx in node_dict['Batch']:
            continue

        batch_annotation = {
            'batch id': batch_idx
        }

        if pd.notna(batch_ext_idx):
            batch_annotation['batch external id'] = batch_ext_idx

        node_dict["Batch"][batch_idx] = Node(
            "Batch", **batch_annotation
        )
        tx.create(node_dict["Batch"][batch_idx])

    # Experiment type
    for experiment_type, experiment_type_annotation in df[['EXPERIMENT_TYPE', 'EXPERIMENT_TYPE_annotation']].values:
        if pd.isna(experiment_type):
            continue

        if pd.isna(experiment_type_annotation):
            experiment_type_annotation = {'name': experiment_type}

        if isinstance(experiment_type_annotation, str):
            experiment_type_annotation = literal_eval(experiment_type_annotation)

        if experiment_type_annotation['name'] in node_dict['Experiment type']:
            continue

        node_dict["Experiment type"][experiment_type_annotation['name']] = Node(
            "Experiment type", **experiment_type_annotation
        )
        tx.create(node_dict["Experiment type"][experiment_type_annotation['name']])

    # Experiment
    if 'MEDIUM_annotation' not in df.columns:
        cols = [
            'Experiment',
            'STUDYID',
            'EXPID',
            'EXPERIMENT_DATE',
            'PROTOCOL_NAME',
            'CONTROL_GROUP',
            'PLANNED_RELATIVE_TIMEPOINT',
            'RELATIVE_TIMEPOINT'
        ]
        for row in df[cols].values:
            (
                experiment,
                study_id,
                experiment_id,
                experiment_date,
                experiment_protocol,
                control_group,
                planned_relative_timepoint,
                relative_timepoint
            ) = row

            if pd.isna(experiment_id):
                continue

            if experiment_id in node_dict['Experiment']:
                continue

            experiment_annotation = {}

            if pd.notna(experiment):
                experiment_annotation['No. of in-vivo experiments'] = experiment

            if pd.notna(study_id):
                experiment_annotation['study id'] = study_id

            if pd.notna(experiment_id):
                experiment_annotation['experiment id'] = experiment_id

            if pd.notna(experiment_date):
                experiment_annotation['experiment date'] = experiment_date

            if pd.notna(experiment_protocol):
                experiment_annotation['experiment protocol'] = experiment_protocol

            if pd.notna(control_group):
                experiment_annotation['experiment control group'] = control_group

            if pd.notna(planned_relative_timepoint):
                experiment_annotation['planned relative time point'] = planned_relative_timepoint

            if pd.notna(relative_timepoint):
                experiment_annotation['relative time point'] = relative_timepoint

            node_dict["Experiment"][experiment_id] = Node(
                "Experiment", **experiment_annotation
            )
            tx.create(node_dict["Experiment"][experiment_id])
    else:
        cols = [
                'STUDYID',
                'EXPID',
                'EXPERIMENT_DATE',
                'PROTOCOL_NAME',
                'No of replicates',
                'CONTROL_GROUP',
                'MEDIUM_annotation'
        ]
        for row in df[cols].values:
            (
                study_id,
                experiment_id,
                experiment_date,
                experiment_protocol,
                replicate_num,
                control_group,
                experiment_medium
            ) = row

            if pd.isna(experiment_id):
                continue

            if experiment_id in node_dict['Experiment']:
                continue

            experiment_annotation = {}

            if pd.notna(study_id):
                experiment_annotation['study id'] = study_id

            if pd.notna(experiment_id):
                experiment_annotation['experiment id'] = experiment_id

            if pd.notna(experiment_date):
                experiment_annotation['experiment date'] = experiment_date

            if pd.notna(experiment_protocol):
                experiment_annotation['experiment protocol'] = experiment_protocol

            if pd.notna(replicate_num):
                experiment_annotation['no. of replicate'] = replicate_num

            if pd.notna(control_group):
                experiment_annotation['experiment control group'] = control_group

            if pd.notna(experiment_medium):
                if isinstance(experiment_medium, str):
                    experiment_medium = literal_eval(experiment_medium)

                experiment_medium['medium_curie'] = experiment_medium.pop('curie')
                experiment_medium['medium_name'] = experiment_medium.pop('name')
                experiment_annotation.update(experiment_medium)

            node_dict["Experiment"][experiment_id] = Node(
                "Experiment", **experiment_annotation
            )

            tx.create(node_dict["Experiment"][experiment_id])

    # Result
    for result_type in df['RESULT_TYPE'].values:

        if pd.isna(result_type):
            continue

        if result_type in node_dict['Result']:
            continue

        result_annotation = {'type': result_type}

        node_dict["Result"][result_type] = Node(
            "Result", **result_annotation
        )
        tx.create(node_dict["Result"][result_type])

    return node_dict
