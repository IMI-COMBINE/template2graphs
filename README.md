# From spreadsheet lab data templates to knowledge graphs: A FAIR data journey in the domain of AMR research

This repository contains the Python scripts for converting Excel-based Lab Data Templates to property-based knowledge graphs to assist lab scientists in systematically navigating the experimental space.

## Publication

If you like the work and want to use it, please cite our pre-print:
> Gadiya, Y., Abbassi-Daloii, T., Ioannidis, V., Juty, N., Stie Kallesoe, C., Attwood, M., Kohler, M., Gribbon, P. and Witt, G., 2024. From spreadsheet lab data templates to knowledge graphs: A FAIR data journey in the domain of AMR research. _bioRxiv_, pp.2024-07. https://doi.org/10.1101/2024.07.18.604030

More datasets and templates can be found on Zenodo:
> Witt, G., Gadiya, Y., Abbassi-Daloii, T., Ioannidis, V., Juty, N., Kallesøe, C. S., Attwood, M., Kohler, M., & Gribbon, P. (2024). Supplementary data files for the manuscript titled "From spreadsheet lab data templates to knowledge graphs: A FAIR data journey in the domain of AMR research" [Data set]. Zenodo. https://doi.org/10.5281/zenodo.12720580

## How to use the repository?

The repository is a workflow to generate a knowledge graph from lab data template allowing for querying and visualizing experimental data in a meaningful and efficient manner.

### Directory overview

```
├── GNA-NOW Graph schema.pdf
├── LICENSE
├── README.md
├── data
│   ├── exps
│   │   ├── dummy
│   │   │   ├── invitro_dummy_data.xlsx
│   │   │   ├── invivo_dummy_data.xlsx
│   │   │   ├── node_dict.json
│   │   │   ├── processed_invitro_data.tsv
│   │   │   └── processed_invivo_data.tsv
│   │   └── noso-502
│   │   │   ├── invitro_NBT_MIC.xlsx
│   │   │   └── invivo_EMC_DF_Ec.xlsx
│   │   │   └── invivo_EMC_DF_Kp.xlsx
│   │   │   └── invivo_EMC_PK_Ec.xlsx
│   │   │   ├── node_dict.json
│   │   │   ├── processed_invitro_data.tsv
│   │   │   └── processed_invivo_data.tsv
│   ├── mapping_files
│   │   ├── bacterial_strain.tsv
│   │   ├── biomaterials.tsv
│   │   ├── experimental_type.tsv
│   │   ├── gna_ontology.tsv
│   │   ├── medium.tsv
│   │   ├── result_unit.tsv
│   │   ├── roa.tsv
│   │   ├── sex.tsv
│   │   ├── species.tsv
│   │   ├── statistical_method.tsv
│   │   └── study_type.tsv
├── requirements.txt
└── src
    ├── constants.py
    ├── data_preprocessing.py
    ├── main.py
    ├── nodes.py
    └── relations.py
```

* The [exps directory](data/exps/) consists of a list of experiment directory with pre-filled template for *in vitro* and *in vivo* studies. Here we just have "dummy" template, but have also shown "dummy2" as an example for adding future experimental runs.
* The [mapping directory](data/mapping_files/) consists of ontology mapped terms of the template. This is catered towards the experiments developed and performed in the project and can be easily adapted for other use cases using the ontology service [OLS](https://www.ebi.ac.uk/ols4).
* The [src directory](src/) consists of all the python scripts required to transform the lab data template to knowledge graphs.

### Prerequisite

The graph is being built on Neo4J. Hence, it is recommended to open a Neo4J instance Desktop version prior to running the scripts.

### Step-by-step process

1. Getting the base python environment ready
```bash
git clone https://github.com/IMI-COMBINE/template2graphs.git
cd template2graphs
conda create --name=template_graph python=3.9
conda activate template_graph
pip install -r requirements.txt
```

2. Making the Lab data templates ready for graph ingestion
* Ensure that all your experiments are located in a directory form under `exps` folder. This way each experiment can be cataloged into specific directory using [FAIR data management guidelines](https://rdmkit.elixir-europe.org/data_organisation)
* Go to the [main.py](src/main.py) file and either change the credential details to your login details or add a new user with the credentials listed and provide admin access to the database.
```bash
cd src
python main.py
```

The Neo4J graph is now populated and can be explored by the users.

## Funding
This work and the authors were primarily funded by the following projects: FAIRplus (IMI 802750), COMBINE (IMI 853967), and GNA NOW (IMI 853979).

