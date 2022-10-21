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
    tmp_df.dropna(subset=['Curie'], inplace=True)

    for bacteria_idx, values in tqdm(tmp_df.iterrows(), total=tmp_df.shape[0]):
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

    for biomaterials_idx, values in tqdm(tmp_df.iterrows(), total=tmp_df.shape[0]):
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

    for experimental_type_idx, values in tqdm(tmp_df.iterrows(), total=tmp_df.shape[0]):
        experimental_type_dict[experimental_type_idx] = {
            'curie': values['Curie'],
            'experimental_type': experimental_type_idx,
            'name': values['Name'],
            'modified_name': values['Modified name'],
            'definition': values['Definition']
        }

    return experimental_type_dict

def get_custom_mapper() -> dict:
    """Method to get custom (GNA-NOW) dictionary."""

    custom_dict = {}

    tmp_df = pd.read_csv(f'{DATA_DIR}/gna_ontology.tsv', sep='\t')

    COMMON_COLS = [
        'Identifier',
    ]

    val_column = tmp_df.columns.to_list()[1]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=['Identifier'], inplace=True)

    for custom_idx, values in tqdm(tmp_df.iterrows(), total=tmp_df.shape[0]):
        custom_dict[custom_idx] = {
            'curie': values['Identifier'],
            'name': custom_idx
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

    for medium_idx, values in tqdm(tmp_df.iterrows(), total=tmp_df.shape[0]):
        medium_dict[ medium_idx] = {
            'curie': values['Curie'],
            'name':  medium_idx,
            'medium': values['Medium'],
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

    for result_unit_idx, values in tqdm(tmp_df.iterrows(), total=tmp_df.shape[0]):
        result_unit_dict[result_unit_idx] = {
            'curie': values['Curie'],
            'name': result_unit_idx,
            'definition': values['Name']
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

    for roa_idx, values in tqdm(tmp_df.iterrows(), total=tmp_df.shape[0]):
        roa_dict[roa_idx] = {
            'curie': values['Curie'],
            'rout': roa_idx,
            'name': values['Name'],
            'Xrefs': values['Xrefs'],
            'synonyms': values['synonyms']
        }

    return roa_dict

def get_ontology_mapper() -> dict:
    """Method to map terms from template to controlled ontologies."""

    file_names = [
        '',
        'biomaterials',
        'dummy_data',
        'experimental_type'
        'gna_ontology',
        'medium',
        'result_unit'
        'roa',
        'statistical_method'
    ]

    ontology_dict = {}

    return {}


def harmonize_data(df: pd.DataFrame):

    data_mapper = get_ontology_mapper()
    pass


if __name__ == '__main__':
    get_roa_mapper()

