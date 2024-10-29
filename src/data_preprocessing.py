# -*- coding: utf-8 -*-

"""Cleaning and ontology harmonization of the data."""
import pandas as pd
import logging
from constants import MAPPING_DIR, ANNOTATION_COLS

logger = logging.getLogger("__name__")


def get_bacterial_mapper() -> dict:
    """Method to get bacterial strain dictionary."""

    bacteria_dict = {}

    tmp_df = pd.read_csv(f"{MAPPING_DIR}/bacterial_strain.tsv", sep="\t")

    COMMON_COLS = ["Curie", "Sample", "Category"]

    val_column = tmp_df.columns.to_list()[0]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=["Curie"], inplace=True)

    for bacteria_idx, values in tmp_df.iterrows():
        bacteria_dict[bacteria_idx] = {
            "curie": values["Curie"],
            "name": bacteria_idx,
            "sample": values["Sample"] if pd.notna(values["Sample"]) else "",
            "category": values["Category"] if pd.notna(values["Category"]) else "",
        }

    return bacteria_dict


def get_biomaterials_mapper() -> dict:
    """Method to get biomaterials dictionary."""

    biomaterials_dict = {}

    tmp_df = pd.read_csv(f"{MAPPING_DIR}/biomaterials.tsv", sep="\t")

    COMMON_COLS = ["Curie"]

    val_column = tmp_df.columns.to_list()[0]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=["Curie"], inplace=True)

    for biomaterials_idx, values in tmp_df.iterrows():
        biomaterials_dict[biomaterials_idx] = {
            "curie": values["Curie"],
            "name": biomaterials_idx,
        }

    return biomaterials_dict


def get_experimental_type_mapper() -> dict:
    """Method to get experimental type dictionary."""

    experimental_type_dict = {}

    tmp_df = pd.read_csv(f"{MAPPING_DIR}/experimental_type.tsv", sep="\t")

    COMMON_COLS = ["Modified name", "Definition", "Curie", "Name"]

    val_column = tmp_df.columns.to_list()[0]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=["Curie"], inplace=True)

    for experimental_type_idx, values in tmp_df.iterrows():
        experimental_type_dict[experimental_type_idx] = {
            "curie": values["Curie"],
            "name": experimental_type_idx,
            "experimental_type": values["Name"] if pd.notna(values["Name"]) else "",
            "modified_name": (
                values["Modified name"] if pd.notna(values["Modified name"]) else ""
            ),
            "definition": (
                values["Definition"] if pd.notna(values["Definition"]) else ""
            ),
        }

    return experimental_type_dict


def get_custom_mapper() -> dict:
    """Method to get custom (GNA-NOW) dictionary."""

    custom_dict = {}

    tmp_df = pd.read_csv(f"{MAPPING_DIR}/gna_ontology.tsv", sep="\t")

    COMMON_COLS = [
        "Term name",
    ]

    val_column = tmp_df.columns.to_list()[1]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=["Identifier"], inplace=True)

    for custom_idx, values in tmp_df.iterrows():
        custom_dict[custom_idx] = {
            "curie": custom_idx,
            "name": values["Term name"] if pd.notna(values["Term name"]) else "",
        }

    return custom_dict


def get_medium_mapper() -> dict:
    """Method to get medium dictionary."""

    medium_dict = {}

    tmp_df = pd.read_csv(f"{MAPPING_DIR}/medium.tsv", sep="\t")

    COMMON_COLS = ["Medium", "Medium_pH", "Medium_additives", "Curie", "Name"]
    val_column = tmp_df.columns.to_list()[0]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=["Curie"], inplace=True)

    for medium_idx, values in tmp_df.iterrows():
        medium_dict[medium_idx] = {
            "curie": values["Curie"],
            "name": medium_idx,
            "medium_acronym": values["Medium"] if pd.notna(values["Medium"]) else "",
            "medium_name": values["Name"] if pd.notna(values["Name"]) else "",
            "medium_pH": values["Medium_pH"] if pd.notna(values["Medium_pH"]) else "",
            "medium_additives": (
                values["Medium_additives"]
                if pd.notna(values["Medium_additives"])
                else ""
            ),
        }

    return medium_dict


def get_result_unit_mapper() -> dict:
    """Method to get result unit dictionary."""

    result_unit_dict = {}

    tmp_df = pd.read_csv(f"{MAPPING_DIR}/result_unit.tsv", sep="\t")

    COMMON_COLS = ["Curie", "Name"]

    val_column = tmp_df.columns.to_list()[0]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=["Curie"], inplace=True)

    for result_unit_idx, values in tmp_df.iterrows():
        result_unit_dict[result_unit_idx] = {
            "curie": values["Curie"],
            "name": result_unit_idx,
            "unit_full_name": values["Name"] if pd.notna(values["Name"]) else "",
        }

    return result_unit_dict


def get_roa_mapper() -> dict:
    """Method to get roa dictionary."""

    roa_dict = {}

    tmp_df = pd.read_csv(f"{MAPPING_DIR}/roa.tsv", sep="\t")

    COMMON_COLS = ["Curie", "Name", "Xrefs", "synonyms"]

    val_column = tmp_df.columns.to_list()[0]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=["Curie"], inplace=True)

    for roa_idx, values in tmp_df.iterrows():
        roa_dict[roa_idx] = {
            "curie": values["Curie"],
            "name": roa_idx,
            "roa_full_name": values["Name"] if pd.notna(values["Name"]) else "",
            "xrefs": values["Xrefs"] if pd.notna(values["Xrefs"]) else "",
            "synonyms": values["synonyms"] if pd.notna(values["synonyms"]) else "",
        }

    return roa_dict


def get_sex_mapper() -> dict:
    """Method to get sex dictionary."""

    sex_dict = {}

    tmp_df = pd.read_csv(f"{MAPPING_DIR}/sex.tsv", sep="\t")

    COMMON_COLS = ["Curie"]

    val_column = tmp_df.columns.to_list()[0]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=["Curie"], inplace=True)

    for sex_idx, values in tmp_df.iterrows():
        sex_dict[sex_idx] = {"curie": values["Curie"], "name": sex_idx}

    return sex_dict


def get_species_mapper() -> dict:
    """Method to get species dictionary."""

    species_dict = {}

    tmp_df = pd.read_csv(f"{MAPPING_DIR}/species.tsv", sep="\t")

    COMMON_COLS = ["Curie", "Name"]

    val_column = tmp_df.columns.to_list()[0]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=["Curie"], inplace=True)

    for species_idx, values in tmp_df.iterrows():
        species_dict[species_idx] = {
            "curie": values["Curie"],
            "name": species_idx,
            "species_name": values["Name"] if pd.notna(values["Name"]) else "",
        }

    return species_dict


def get_study_type_mapper() -> dict:
    """Method to get study type dictionary."""

    study_type_dict = {}

    tmp_df = pd.read_csv(f"{MAPPING_DIR}/study_type.tsv", sep="\t")

    COMMON_COLS = ["Curie"]

    val_column = tmp_df.columns.to_list()[0]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=["Curie"], inplace=True)

    for study_type_idx, values in tmp_df.iterrows():
        study_type_dict[study_type_idx] = {
            "curie": values["Curie"],
            "name": study_type_idx,
        }

    return study_type_dict


def get_statistical_method_mapper() -> dict:
    """Method to statistical method dictionary."""

    statistical_method_dict = {}

    tmp_df = pd.read_csv(f"{MAPPING_DIR}/statistical_method.tsv", sep="\t")

    COMMON_COLS = ["Curie", "Name"]

    val_column = tmp_df.columns.to_list()[0]
    COMMON_COLS.insert(0, val_column)

    tmp_df = tmp_df[COMMON_COLS]
    tmp_df.set_index(val_column, inplace=True)

    # Drop columns with no ontology mapping
    tmp_df.dropna(subset=["Curie"], inplace=True)

    for statistical_method_idx, values in tmp_df.iterrows():
        statistical_method_dict[statistical_method_idx] = {
            "curie": values["Curie"],
            "name": statistical_method_idx,
            "statistical_method": values["Name"] if pd.notna(values["Name"]) else "",
        }

    return statistical_method_dict


def get_ontology_mapper() -> dict:
    """Method to map terms from template to controlled ontologies."""

    ontology_dict = {
        "BIOMATERIAL": get_biomaterials_mapper(),
        "BACTERIAL_STRAIN_NAME": get_bacterial_mapper(),
        "EXPERIMENT_TYPE": get_experimental_type_mapper(),
        "MEDIUM": get_medium_mapper(),
        "RESULT_UNIT": get_result_unit_mapper(),
        "ROUTE_OF_ADMINISTRATION": get_roa_mapper(),
        "INFECTION_ROUTE": get_roa_mapper(),
        "PRETREATMENT_ROUTE_OF_ADMINSTRATION": get_roa_mapper(),
        "ANIMAL_SEX": get_sex_mapper(),
        "SPECIES_NAME": get_species_mapper(),
        "STATISTICAL_METHOD": get_statistical_method_mapper(),
        "STUDY_TYPE": get_study_type_mapper(),
    }

    return ontology_dict


def harmonize_data(df: pd.DataFrame):
    """Main function to harmonise terms in template with ontology."""

    data_mapper = get_ontology_mapper()

    df.replace("#NA (not applicable)", "", inplace=True)

    for column in ANNOTATION_COLS:
        if column not in df.columns:
            continue

        df[column].fillna("", inplace=True)  # Replace all nans with empty values
        df[f"{column}_annotation"] = df[column].map(
            lambda x: (
                data_mapper[column][x.rstrip().lstrip()]
                if x.rstrip().lstrip() in data_mapper[column]
                else ""
            )
        )

    return df
