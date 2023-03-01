# -*- coding: utf-8 -*-

"""Main file for running the KG."""

import os
import pandas as pd
from datetime import datetime
from py2neo import Graph

from nodes import add_nodes
from relations import add_relations
from data_preprocessing import harmonize_data

pd.set_option('display.max_columns', None)

DATA_DIR = '../data'


def intersection(list1, list2):
    """Function to merge dataframes based on their common columns"""
    temp = set(list2)
    common_cols = [column for column in list1 if column in temp]
    return common_cols


def get_invitro_data() -> pd.DataFrame:
    """Main function to read and harmonize in vitro data."""

    df_invitro = pd.read_csv(f'{DATA_DIR}/invitro_dummy_data.tsv', sep='\t', skiprows=4)
    df_invitro.drop([
        'Variable',
        '#NA (not applicable)',
        '#NA (not applicable).1',
        'StdDev'
    ], inplace=True, axis=1, errors='ignore')

    df_invitro = harmonize_data(df_invitro)

    return df_invitro


def get_invivo_data() -> dict[str, pd.DataFrame]:
    """Main function to read an Excel file and returns harmonize in vivo data."""

    xl = pd.ExcelFile(f'{DATA_DIR}/invivo_dummy_data.xlsx')
    sheet_names = xl.sheet_names
    prefixes = ['ExperimentResults', 'StudyDetails', 'Treatment']
    matching_sheet_names = [name for name in sheet_names if any(name.startswith(x) for x in prefixes)]

    df_invivo = {}
    for sheet in matching_sheet_names:
        if sheet == "Treatment":
            start_row = 6
        else:
            start_row = 5
        df_invivo[sheet] = pd.read_excel(f'{DATA_DIR}/invivo_dummy_data.xlsx', sheet_name=sheet,
                                         skiprows=range(0, start_row))

        df_invivo[sheet].drop([
            'Variable',
            '#NA (not applicable)',
            '#NA (not applicable).1',
            'StdDev',
            'COMMENT'
        ], inplace=True, axis=1, errors='ignore')
        df_invivo[sheet] = df_invivo[sheet].dropna(axis=0, how='all')

        experiment = pd.DataFrame()
        # rbind different experiments
        if sheet.startswith('Experiment'):
            df_invivo[sheet]['Experiment'] = sheet
            experiment = pd.concat([experiment, df_invivo[sheet]], ignore_index=True)

    # merge different sheets based on their common columns
    common_cols = intersection(df_invivo['StudyDetails'].columns, experiment.columns)
    df_invivo_all = pd.merge(df_invivo['StudyDetails'], experiment, how="outer", on=common_cols)

    common_cols = intersection(df_invivo['Treatment'].columns, experiment.columns)
    df_invivo_all = pd.merge(df_invivo['Treatment'], df_invivo_all, how="outer", on=common_cols)

    df_invivo_all = harmonize_data(df_invivo_all)

    return df_invivo_all


def export_triples(
        graph: Graph
):
    """Exporting triples of the graph"""

    DATE = datetime.today().strftime('%d_%b_%Y')
    t = graph.run(
        'Match (n)-[r]-(m) Return n.name, n.curie, type(r), m.name, m.curie'
    ).to_data_frame()

    graph_dir = f'{DATA_DIR}/graph'
    os.makedirs(graph_dir, exist_ok=True)
    return t.to_csv(f'{graph_dir}/base_triples-{DATE}.tsv', sep='\t', index=False)


def create_graph(invivo_df: pd.DataFrame, invitro_df: pd.DataFrame):
    """Main function to create and populate the graph."""

    # TODO: FixMe
    graph_url = 'bolt://localhost:7687'
    graph_admin_name = 'neo4j'
    graph_pass = 'tooba65'

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
    node_dict = add_nodes(
        tx=tx,
        df=invivo_df,
        node_dict=node_dict
    )

    # Populating nodes for invitro experiments
    node_map = add_nodes(
        tx=tx,
        df=invitro_df,
        node_dict=node_dict
    )

    # Creating edges between the nodes
    add_relations(
        invivo_df=invivo_df,
        invitro_df=invitro_df,
        node_mapping_dict=node_map,
        tx=tx
    )

    graph.commit(tx)

    export_triples(graph)


if __name__ == '__main__':
    if os.path.isfile(f'{DATA_DIR}/processed_invitro_template.tsv'):
        edge_data_invitro = pd.read_csv(f'{DATA_DIR}/processed_invitro_template.tsv', sep='\t')
    else:
        edge_data_invitro = get_invitro_data()
        edge_data_invitro.to_csv(f'{DATA_DIR}/processed_invitro_template.tsv', sep='\t', index=False)

    if os.path.isfile(f'{DATA_DIR}/processed_invivo_template.tsv'):
        edge_data_invivo = pd.read_csv(f'{DATA_DIR}/processed_invivo_template.tsv', sep='\t')
    else:
        edge_data_invivo = get_invivo_data()
        edge_data_invivo.to_csv(f'{DATA_DIR}/processed_invivo_template.tsv', sep='\t', index=False)

    print('creaing the graph')
    create_graph(invivo_df=edge_data_invivo, invitro_df=edge_data_invitro)









