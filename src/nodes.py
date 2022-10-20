# -*- coding: utf-8 -*-

"""Script to create different nodes in the data"""


import pandas as pd
from py2neo import Node
from py2neo.database import Transaction


def add_nodes(
    tx: Transaction,
    df: pd.DataFrame
) -> dict:
    """Add nodes based on template data."""

    node_dict = {
        'Specimen': {},
        'Bacteria': {},
        'Partner': {},
        'Compound': {},
        'Batch': {},
        'Experiment type': {},
        'Experiment': {},
        'Result': {},
    }

    # TODO: Adapt example for use case
    for name, email, orcid in df.values:
        person_property = {}

        if pd.notna(name):
            person_property["name"] = name

        if pd.notna(email):
            person_property["email"] = email

        if pd.notna(orcid):
            person_property["orcid"] = orcid

        node_dict["Person"][name] = Node("Person", **person_property)
        tx.create(node_dict["Person"][name])

    return node_dict
