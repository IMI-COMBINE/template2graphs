# -*- coding: utf-8 -*-

"""Main file for running the KG."""

import logging
import os
import pandas as pd
import json
from typing import Dict
import numpy as np
from tqdm import tqdm
from py2neo import Graph

from nodes import add_nodes
from relations import add_relations
from data_preprocessing import harmonize_data
from constants import DATA_DIR

logger = logging.getLogger("__name__")


def intersection(list1, list2):
    """Function to merge dataframes based on their common columns
    :param list1: List of columns from dataframe 1
    :param list2: List of columns from dataframe 2
    """
    temp = set(list2)
    common_cols = [column for column in list1 if column in temp]
    return common_cols


def get_invitro_data(
    xl: pd.ExcelFile,
) -> pd.DataFrame:
    """Main function to read and harmonize in vitro data.
    :param xl: Excel file
    :return: Harmonized in-vitro data
    """
    df_invitro = xl.parse(skiprows=5, dtype=str)
    df_invitro.drop(
        ["Variable", "#NA (not applicable)", "#NA (not applicable).1", "StdDev"],
        inplace=True,
        axis=1,
        errors="ignore",
    )
    df_invitro.columns = [
        col.rstrip().lstrip() for col in df_invitro.columns
    ]  # remove leading and trailing spaces

    return harmonize_data(df_invitro)


def get_invivo_data(
    xl: pd.ExcelFile,
) -> pd.DataFrame:
    """Main function to read an Excel file and returns harmonize in vivo data.
    :param xl: Excel file
    :return: Harmonized in-vivo data
    """

    sheet_names = xl.sheet_names
    prefixes = ["ExperimentResults", "StudyDetails", "Treatment"]
    matching_sheet_names = [
        name for name in sheet_names if any(name.startswith(x) for x in prefixes)
    ]

    df_invivo = {}
    experiment = pd.DataFrame(dtype=str)  # Merging all experiments columns together

    for sheet in matching_sheet_names:
        if sheet == "Treatment":
            start_row = 6
        else:
            start_row = 5

        tmp_df = xl.parse(sheet_name=sheet, skiprows=start_row, dtype=str)
        tmp_df.drop(
            ["Variable", "#NA (not applicable)", "#NA (not applicable).1", "StdDev"],
            inplace=True,
            axis=1,
            errors="ignore",
        )

        tmp_df.dropna(axis=0, how="all", inplace=True)

        # QC checking - Removing experiments with empty data
        if "GROUP_DESCRIPTION" in tmp_df.columns:
            exmp_vals = tmp_df["GROUP_DESCRIPTION"].values
            if np.nan in exmp_vals:
                continue

        if tmp_df.empty:
            logger.warning(f"Empty in-vivo sheet: {sheet}")
            continue

        df_invivo[sheet] = tmp_df

        # merging all experiments together
        if "experiment" in sheet.lower():
            tmp_df["Experiment"] = sheet
            experiment = pd.concat([experiment, tmp_df], ignore_index=True)

    # merge different sheets based on their common columns
    common_cols_1 = intersection(df_invivo["StudyDetails"].columns, experiment.columns)
    study_exp_df = pd.merge(
        df_invivo["StudyDetails"], experiment, how="outer", on=common_cols_1
    )

    common_cols_2 = intersection(df_invivo["Treatment"].columns, study_exp_df.columns)
    df_invivo_all = pd.merge(
        df_invivo["Treatment"], study_exp_df, how="outer", on=common_cols_2
    )

    df_invivo_all.columns = [
        col.rstrip().lstrip() for col in df_invivo_all.columns
    ]  # remove leading and trailing spaces

    df_invivo_all = harmonize_data(df_invivo_all)

    return df_invivo_all


def create_graph(
    invivo_df: pd.DataFrame, invitro_df: pd.DataFrame, credentials: Dict[str, str]
):
    """Main function to create and populate the graph.
    :param invivo_df: In-vivo data
    :param invitro_df: In-vitro data
    :param credentials: Graph credentials
    """
    graph = Graph(
        credentials["uri"],
        auth=(credentials["user"], credentials["password"]),
    )
    tx = graph.begin()
    graph.delete_all()  # delete existing data

    node_dict = {
        "Animal species": {},
        "Animal group": {},
        "Animal number": {},
        "In-vivo study type": {},
        "Study": {},
        "Specimen": {},
        "Bacteria": {},
        "Partner": {},
        "Compound": {},
        "Batch": {},
        "Experiment type": {},
        "Experiment": {},
        "Result": {},
    }

    # Creating nodes for invivo experiments
    logger.warning("Creating nodes for invivo experiments")
    node_dict = add_nodes(tx=tx, df=invivo_df, node_dict=node_dict)

    # Populating nodes for invitro experiments
    logger.warning("Creating nodes for invitro experiments")
    node_map = add_nodes(tx=tx, df=invitro_df, node_dict=node_dict)

    with open(f"{DATA_DIR}/node_dict.json", "w") as f:
        json.dump(node_map, f, indent=2, ensure_ascii=False)

    graph.commit(tx)

    tx = graph.begin()

    # Creating edges between the nodes
    logger.warning("Creating relations")
    add_relations(
        invivo_df=invivo_df, invitro_df=invitro_df, node_mapping_dict=node_map, tx=tx
    )

    graph.commit(tx)


def load_data(exp_dir: str) -> None:
    """Loading data recursively.
    :param exp_dir: Directory containing the experiments
    :return: Dataframes for in-vivo and in-vitro data
    """
    invivo_dfs = []
    invitro_dfs = []

    # Recursive function to load data
    for path, dirs, files in tqdm(list(os.walk(exp_dir, topdown=True))):
        for name in files:
            if not name.endswith(".xlsx"):
                continue
            excel_file = pd.ExcelFile(os.path.join(path, name))
            sheet_names = excel_file.sheet_names

            if len(sheet_names) > 3:
                df = get_invivo_data(excel_file)
                invivo_dfs.append(df)
            else:
                df = get_invitro_data(excel_file)
                invitro_dfs.append(df)

    invivo_df = pd.concat(invivo_dfs, ignore_index=True)
    invivo_df.to_csv(f"{DATA_DIR}/processed_invivo_data.tsv", index=False, sep="\t")

    invitro_df = pd.concat(invitro_dfs, ignore_index=True)
    invitro_df.to_csv(f"{DATA_DIR}/processed_invitro_data.tsv", index=False, sep="\t")

    logger.warning(f"No.of in-vivo data points: {len(invivo_df)}")
    logger.warning(f"No.of in-vitro data points: {len(invitro_df)}")

    return None


if __name__ == "__main__":

    if not os.path.exists(f"{DATA_DIR}/processed_invivo_data.tsv"):
        load_data(exp_dir="../data/exps")  # Load data from experiments

    edge_data_invivo = pd.read_csv(
        f"{DATA_DIR}/processed_invivo_data.tsv",
        sep="\t",
        dtype=str,
        low_memory=False,
    )
    edge_data_invitro = pd.read_csv(
        f"{DATA_DIR}/processed_invitro_data.tsv",
        sep="\t",
        dtype=str,
        low_memory=False,
    )

    # Neo4j graph connection details - Change as per your setup
    graph_url = "bolt://localhost:7687"
    graph_admin_name = "template2graph"
    graph_pass = "gnanow2024-database"

    create_graph(
        invivo_df=edge_data_invivo,
        invitro_df=edge_data_invitro,
        credentials={
            "uri": graph_url,
            "user": graph_admin_name,
            "password": graph_pass,
        },
    )
