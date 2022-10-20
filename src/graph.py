# -*- coding: utf-8 -*-

"""Script for populating the Neo4J database."""

import os
import pandas as pd
from datetime import datetime
from py2neo import Graph

from src.nodes import add_nodes
from src.relations import add_relations

pd.set_option('display.max_columns', None)

DATA_DIR = '../data'


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


def create_graph():
    """Main function to create and populate the graph."""

    # TODO: FixMe
    FRAUNHOFER_URL = ''
    FRAUNHOFER_ADMIN_NAME = ''
    FRAUNHOFER_ADMIN_PASS = ''

    graph = Graph(
        FRAUNHOFER_URL,
        auth=(FRAUNHOFER_ADMIN_NAME, FRAUNHOFER_ADMIN_PASS),
    )
    tx = graph.begin()
    graph.delete_all()  # delete existing data

    # Load processed data
    data_df = pd.read_csv(f'{DATA_DIR}/processed_template.tsv', sep='\t')

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
    create_graph()