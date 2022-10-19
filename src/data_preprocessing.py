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
    pass


def get_experimental_type_mapper() -> dict:
    pass


def get_custom_mapper() -> dict:
    pass


def get_medium_mapper() -> dict:
    pass


def get_result_unit_mapper() -> dict:
    pass


def get_roa_mapper() -> dict:
    pass


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
    get_bacterial_mapper()

