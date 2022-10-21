# -*- coding: utf-8 -*-

"""Cleaning and ontology harmonization of the data."""
import pandas as pd
from tqdm import tqdm

DATA_DIR = '../data'

pd.set_option('display.max_columns', None)


def get_bacterial_mapper() -> dict:
    """Method to get bacterial strain dictionary."""

    bacteria_dict = {}

    tmp_df = pd.read_csv(f'{DATA_DIR}/bacterial_strain.tsv', sep='\t')

    COMMON_COLS = [
        'Curie',
        'Sample',
        'Category'
    ]

    val_column = tmp_df.columns.to_list()[0]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    # tmp_df.dropna(subset=['Curie'], inplace=True)

    for bacteria_idx, values in tqdm(
        tmp_df.iterrows(), total=tmp_df.shape[0], desc='Ontology for bacteria'
    ):
        bacteria_dict[bacteria_idx] = {
            'curie': values['Curie'],
            'name': bacteria_idx,
            'sample': values['Sample'],
            'category': values['Category']
        }

    return bacteria_dict


def get_biomaterials_mapper() -> dict:
    """Method to get biomaterials dictionary."""

    biomaterials_dict = {}

    tmp_df = pd.read_csv(f'{DATA_DIR}/biomaterials.tsv', sep='\t')

    COMMON_COLS = [
        'Curie'
    ]

    val_column = tmp_df.columns.to_list()[0]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    for biomaterials_idx, values in tqdm(
        tmp_df.iterrows(), total=tmp_df.shape[0], desc='Ontology for biomaterials'
    ):
        biomaterials_dict[biomaterials_idx] = {
            'curie': values['Curie'],
            'name': biomaterials_idx
        }

    return biomaterials_dict


def get_experimental_type_mapper() -> dict:
    """Method to get experimental type dictionary."""

    experimental_type_dict = {}

    tmp_df = pd.read_csv(f'{DATA_DIR}/experimental_type.tsv', sep='\t')

    COMMON_COLS = [
        'Modified name',
        'Definition',
        'Curie',
        'Name'
    ]

    val_column = tmp_df.columns.to_list()[0]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=['Curie'], inplace=True)

    for experimental_type_idx, values in tqdm(
        tmp_df.iterrows(), total=tmp_df.shape[0], desc='Ontology for experimental types'
    ):
        experimental_type_dict[experimental_type_idx] = {
            'curie': values['Curie'],
            'name': experimental_type_idx,
            'experimental_type': values['Name'],
            'modified_name': values['Modified name'],
            'definition': values['Definition']
        }

    return experimental_type_dict


def get_custom_mapper() -> dict:
    """Method to get custom (GNA-NOW) dictionary."""

    custom_dict = {}

    tmp_df = pd.read_csv(f'{DATA_DIR}/gna_ontology.tsv', sep='\t')

    COMMON_COLS = [
        'Term name',
    ]

    val_column = tmp_df.columns.to_list()[1]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=['Identifier'], inplace=True)

    for custom_idx, values in tqdm(
        tmp_df.iterrows(), total=tmp_df.shape[0], desc='Custom ontology'
    ):
        custom_dict[custom_idx] = {
            'curie': custom_idx,
            'name': values['Term name']
        }

    return custom_dict


def get_medium_mapper() -> dict:
    """Method to get medium dictionary."""

    medium_dict = {}

    tmp_df = pd.read_csv(f'{DATA_DIR}/medium.tsv', sep='\t')

    COMMON_COLS = [
        'Medium',
        'Medium_pH',
        'Medium_additives',
        'Curie',
        'Name'
    ]
    val_column = tmp_df.columns.to_list()[0]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=['Curie'], inplace=True)

    for medium_idx, values in tqdm(
        tmp_df.iterrows(), total=tmp_df.shape[0], desc='Ontology for medium'
    ):
        medium_dict[ medium_idx] = {
            'curie': values['Curie'],
            'name':  medium_idx,
            'medium_acronym': values['Medium'],
            'medium_name': values['Name'],
            'medium_pH': values['Medium_pH'],
            'medium_additives': values['Medium_additives']
        }

    return medium_dict


def get_result_unit_mapper() -> dict:
    """Method to get result unit dictionary."""

    result_unit_dict = {}

    tmp_df = pd.read_csv(f'{DATA_DIR}/result_unit.tsv', sep='\t')

    COMMON_COLS = [
        'Curie',
        'Name'
    ]

    val_column = tmp_df.columns.to_list()[0]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=['Curie'], inplace=True)

    for result_unit_idx, values in tqdm(
        tmp_df.iterrows(), total=tmp_df.shape[0], desc='Ontology for result units'
    ):
        result_unit_dict[result_unit_idx] = {
            'curie': values['Curie'],
            'name': result_unit_idx,
            'unit_full_name': values['Name']
        }

    return result_unit_dict


def get_roa_mapper() -> dict:
    """Method to get roa dictionary."""

    roa_dict = {}

    tmp_df = pd.read_csv(f'{DATA_DIR}/roa.tsv', sep='\t')

    COMMON_COLS = [
        'Curie',
        'Name',
        'Xrefs',
        'synonyms'
    ]

    val_column = tmp_df.columns.to_list()[0]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=['Curie'], inplace=True)

    for roa_idx, values in tqdm(
        tmp_df.iterrows(), total=tmp_df.shape[0], desc='Ontology for route of administration'
    ):
        roa_dict[roa_idx] = {
            'curie': values['Curie'],
            'name': roa_idx,
            'roa_full_name': values['Name'],
            'xrefs': values['Xrefs'],
            'synonyms': values['synonyms']
        }

    return roa_dict


def get_ontology_mapper() -> dict:
    """Method to map terms from template to controlled ontologies."""

    ontology_dict = {}

    bacteria_dict = get_bacterial_mapper()
    ontology_dict['BACTERIAL_STRAIN_NAME'] = bacteria_dict

    biomaterial = get_biomaterials_mapper()
    ontology_dict['BIOMATERIAL'] = biomaterial

    exp_type = get_experimental_type_mapper()
    ontology_dict['EXPERIMENT_TYPE'] = exp_type

    medium = get_medium_mapper()
    ontology_dict['MEDIUM'] = medium

    result_unit = get_result_unit_mapper()
    ontology_dict['RESULT_UNIT'] = result_unit

    # TODO: Ask Gesa about which column the data is for RoA?
    # roa = get_roa_mapper()
    # ontology_dict['RESULT_UNIT'] = result_unit

    return ontology_dict


def harmonize_data(df: pd.DataFrame):

    data_mapper = get_ontology_mapper()

    df.replace('#NA (not applicable)', '', inplace=True)

    ANNOTATION_COLS = [
        'BIOMATERIAL',
        'BACTERIAL_STRAIN_NAME',
        'EXPERIMENT_TYPE',
        'MEDIUM',
        'RESULT_UNIT'
    ]

    for column in ANNOTATION_COLS:
        df[f'{column}_annotation'] = df[column].map(
            lambda x:  data_mapper[column][x] if x in data_mapper[column] else ''
        )

    return df
