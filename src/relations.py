# -*- coding: utf-8 -*-

"""Script for creation of relations between entities."""

import pandas as pd
from tqdm import tqdm

from py2neo import Relationship
from py2neo.database import Transaction


def add_relations(
    df: pd.DataFrame,
    node_mapping_dict: dict,
    tx: Transaction
) -> None:
    """Add relations based on template"""

    for rows in tqdm(df.values, desc="Populating graph with relations"):
        (
            # TODO: Change this
            person_name,
            institute_name,
            project_1_name,
            project_2_name,
            pathogen_1_name,
            pathogen_2_name,
            pathogen_3_name,
            skill_1_name,
            skill_2_name,
            skill_3_name,
            skill_4_name,
        ) = rows

        # TODO: Example for creation below
        # Person - [WORKS_AT] -> Institute
        person_node = node_mapping_dict["Person"][person_name]
        institute_node = node_mapping_dict["Institute"][institute_name]
        works_at = Relationship(person_node, "WORKS_AT", institute_node)
        tx.create(works_at)

        # TODO
        """Specimen -> Bacteria edge"""

        """Partner -> Compounds edge"""

        """Partner -> Experiment edge"""

        """Bacteria -> Compounds edge"""

        """Compound -> Batch edge"""

        """Batch -> Experiment type edge"""

        """Experiment type -> Experiment edge"""

        """Experiment -> Result edge"""

