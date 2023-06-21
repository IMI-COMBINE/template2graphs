# -*- coding: utf-8 -*-

"""Main file for running the KG."""

import logging
import os
import pandas as pd
import json
import numpy as np
import zipfile
from tqdm import tqdm
from py2neo import Graph

from nodes import add_nodes
from relations import add_relations
from data_preprocessing import harmonize_data
from constants import DATA_DIR, _NEW_PASSWORD, _URI, _ADMIN

logger = logging.getLogger('__name__')


def intersection(list1, list2):
    """Function to merge dataframes based on their common columns"""
    temp = set(list2)
    common_cols = [column for column in list1 if column in temp]
    return common_cols


def get_invitro_data(
    xl: pd.ExcelFile,
) -> pd.DataFrame:
    """Main function to read and harmonize in vitro data."""

    sheet_names = xl.sheet_names
    if 'Data' in sheet_names:
        name = 'Data'
    elif 'Data ' in sheet_names:
        name = 'Data '
    else:
        raise ValueError('Invalid sheet -', sheet_names)

    df_invitro = xl.parse(sheet_name=name, skiprows=4, dtype=str)
    df_invitro.drop([
        'Variable',
        '#NA (not applicable)',
        '#NA (not applicable).1',
        'StdDev'
    ], inplace=True, axis=1, errors='ignore')

    return harmonize_data(df_invitro)


def get_invivo_data(
    xl: pd.ExcelFile,
) -> dict[str, pd.DataFrame]:
    """Main function to read an Excel file and returns harmonize in vivo data."""

    sheet_names = xl.sheet_names
    prefixes = ['ExperimentResults', 'StudyDetails', 'Treatment']
    matching_sheet_names = [name for name in sheet_names if any(name.startswith(x) for x in prefixes)]

    df_invivo = {}
    experiment = pd.DataFrame(dtype=str)  # Merging all experiments columns together

    for sheet in matching_sheet_names:
        if sheet == "Treatment":
            start_row = 6
        else:
            start_row = 5

        tmp_df = xl.parse(sheet_name=sheet, skiprows=start_row, dtype=str)
        tmp_df.drop([
            'Variable',
            '#NA (not applicable)',
            '#NA (not applicable).1',
            'StdDev'
        ], inplace=True, axis=1, errors='ignore')

        tmp_df.dropna(axis=0, how='all', inplace=True)

        # QC checking - Removing experiments with empty data
        if 'GROUP_DESCRIPTION' in tmp_df.columns:
            exmp_vals = tmp_df['GROUP_DESCRIPTION'].values
            if '0' in exmp_vals or np.nan in exmp_vals:
                continue

        if tmp_df.empty:
            print(sheet)

        df_invivo[sheet] = tmp_df

        # rbind different experiments
        if sheet.startswith('Experiment'):
            df_invivo[sheet]['Experiment'] = sheet
            experiment = pd.concat([experiment, tmp_df], ignore_index=True)

    # merge different sheets based on their common columns
    common_cols = intersection(df_invivo['StudyDetails'].columns, experiment.columns)
    df_invivo_all = pd.merge(df_invivo['StudyDetails'], experiment, how="outer", on=common_cols)

    common_cols = intersection(df_invivo['Treatment'].columns, experiment.columns)
    df_invivo_all = pd.merge(df_invivo['Treatment'], df_invivo_all, how="outer", on=common_cols)

    df_invivo_all = harmonize_data(df_invivo_all)

    return df_invivo_all


def create_graph(invivo_df: pd.DataFrame, invitro_df: pd.DataFrame, use_local: bool = False):
    """Main function to create and populate the graph."""

    if use_local:
        # Local GNA-NOW instance
        graph_url = 'bolt://localhost:7687'
        graph_admin_name = 'yojana'
        graph_pass = 'tooba65'
    else:
        graph_url = _URI
        graph_admin_name = _ADMIN
        graph_pass = _NEW_PASSWORD

    graph = Graph(
        graph_url,
        auth=(graph_admin_name, graph_pass),
    )
    tx = graph.begin()
    graph.delete_all()  # delete existing data

    node_dict = {
        'Animal species': {},
        'Animal group': {},
        'Animal number': {},
        'In-vivo study type': {},
        'Study': {},
        'Specimen': {},
        'Bacteria': {},
        'Partner': {},
        'Compound': {},
        'Batch': {},
        'Experiment type': {},
        'Experiment': {},
        'Result': {}
    }

    # Creating nodes for invivo experiments
    logger.warning('Creating nodes for invivo experiments')
    node_dict = add_nodes(
        tx=tx,
        df=invivo_df,
        node_dict=node_dict
    )

    # Populating nodes for invitro experiments
    logger.warning('Creating nodes for invitro experiments')
    node_map = add_nodes(
        tx=tx,
        df=invitro_df,
        node_dict=node_dict
    )

    with open(f'{DATA_DIR}/node_dict.json', 'w') as f:
        json.dump(node_map, f, indent=2, ensure_ascii=False)

    graph.commit(tx)

    tx = graph.begin()

    # Creating edges between the nodes
    logger.warning('Creating relations')
    add_relations(
        invivo_df=invivo_df,
        invitro_df=invitro_df,
        node_mapping_dict=node_map,
        tx=tx
    )

    graph.commit(tx)


def get_data():
    """Unzipping data from J esp folder locally."""
    directory = 'J:\esp\projects\ESP Projects Active\IMI2_GNA-NOW_AMR_Pillar_C\GeneratedData\WP1_T1-5_Datamanagement\Archiving_NOSO-2G\GNA-NOW_NOSO-2G_archive_v2\GNA-NOW_NOSO-2G_ownCloud-Archive'
    file_name = os.listdir(directory)[0]
    with zipfile.ZipFile(f'{directory}/{file_name}', 'r') as zip_ref:
        zip_ref.extractall('.')
    return None


def load_data():
    """Loading data recursively."""

    data_dir = 'Neuer Ordner'
    if not os.path.exists(data_dir):
        get_data()

    invivo_dfs = []
    invitro_dfs = []

    # Recursive function to load data
    for path, dirs, files in tqdm(list(os.walk(data_dir, topdown=True))):

        # Skip NOSO-2G Bioaster directors due to proteomics data
        [dirs.remove(d) for d in dirs if d == 'NOSO-2G_Bioaster']

        for name in files:
            if not name.endswith('GW.xlsx'):
                continue
            excel_file = pd.ExcelFile(os.path.join(path, name))
            sheet_names = excel_file.sheet_names

            if len(sheet_names) > 3:
                df = get_invivo_data(excel_file)
                df['file_path'] = os.path.join(path, name)

                invivo_dfs.append(df)
            else:
                df = get_invitro_data(excel_file)
                df['file_path'] = os.path.join(path, name)
                invitro_dfs.append(df)

    print('No.of in-vivo data points', len(invivo_dfs))
    print('No.of in-vitro data points', len(invitro_dfs))
    invivo_df = pd.concat(invivo_dfs, ignore_index=True)
    invivo_df.to_csv(f'{DATA_DIR}/invivo_data.tsv', index=False, sep='\t')
    invitro_df = pd.concat(invitro_dfs, ignore_index=True)
    invitro_df.to_csv(f'{DATA_DIR}/invitro_data.tsv', index=False, sep='\t')

    return invivo_df, invitro_df


if __name__ == '__main__':

    if not os.path.exists(f'{DATA_DIR}/invivo_data.tsv'):
        edge_data_invivo, edge_data_invitro = load_data()
    else:
        edge_data_invivo = pd.read_csv(
            f'{DATA_DIR}/invivo_data.tsv', sep='\t', dtype=str, low_memory=False
        )
        edge_data_invitro = pd.read_csv(
            f'{DATA_DIR}/invitro_data.tsv', sep='\t', dtype=str, low_memory=False
        )

        cmp_subset = ['EOAI4017617', 'EOAI4017616']
        edge_data_invitro = edge_data_invitro[edge_data_invitro.CPD_ID.isin(cmp_subset)]
        edge_data_invivo = edge_data_invivo[edge_data_invivo.CPD_ID.isin(cmp_subset)]

    create_graph(invivo_df=edge_data_invivo, invitro_df=edge_data_invitro)









