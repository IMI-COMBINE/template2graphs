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
DATA = 'invivo'

def get_data() -> pd.DataFrame:
    df = pd.read_csv(f'{DATA_DIR}/{DATA}_dummy_data.tsv', sep='\t', skiprows=4)
    df.drop([
        'Variable',
        '#NA (not applicable)',
        '#NA (not applicable).1',
        'StdDev'
    ], inplace=True, axis=1, errors='ignore')

    df = harmonize_data(df)

    return df


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


def create_graph(data_df: pd.DataFrame):
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

    # Creating nodes
    node_map = add_nodes(
        tx=tx,
        df=data_df
    )

    # Creating edges
    add_relations(
        df=data_df,
        node_mapping_dict=node_map,
        tx=tx
    )

    graph.commit(tx)

    export_triples(graph)


if __name__ == '__main__':
    if os.path.isfile(f'{DATA_DIR}/processed_{DATA}_template.tsv'):
        edge_data = pd.read_csv(f'{DATA_DIR}/processed_{DATA}_template.tsv', sep='\t')
    else:
        edge_data = get_data()
        edge_data.to_csv(f'{DATA_DIR}/processed_{DATA}_template.tsv', sep='\t', index=False)

    create_graph(data_df=edge_data)

