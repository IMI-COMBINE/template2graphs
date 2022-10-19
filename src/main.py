# -*- coding: utf-8 -*-

"""Main file for running the KG."""

import pandas as pd

from src.data_preprocessing import harmonize_data

pd.set_option('display.max_columns', None)

DATA_DIR = '../data'


def get_data():
    df = pd.read_csv(f'{DATA_DIR}/dummy_data.tsv', sep='\t', skiprows=4)
    df.drop([
        'Variable',
        'BACTERIAL_STRAIN_SITE_REF',
        '#NA (not applicable)',
        '#NA (not applicable).1',
        'StdDev'
    ], inplace=True, axis=1)

    df = harmonize_data(df)




def create_graph():
    pass


if __name__ == '__main__':
    get_data()