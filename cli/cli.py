from unittest import result
from click import argument, echo, group
from jinja2 import Environment, FileSystemLoader
from os.path import abspath, isfile
from yaml import safe_load
from zipfile import ZipFile

import os
import urllib
import zipfile

def get_config(path):
    config = {}
    if isfile(path) == False:
        return config
    with open(path, "r") as f:
        config = safe_load(f)
    return config


def get_template(name):
    path = abspath("templates")
    env = Environment(
        loader=FileSystemLoader(path), trim_blocks=True, lstrip_blocks=True
    )
    template = env.get_template(name)
    return template


@group()
def cli():
    pass

@cli.command()
@argument("source")
def gendagtg(source):
    _gendagtg(source)

def _gendagtg(source):
    l1_dataset = f"l1_{source}"
    l1_config = get_config(f"ingestion/{l1_dataset}.yaml")
    l1_config["dataset"] = l1_dataset

    l2_dataset = f"l2_{source}"
    l2_config = get_config(f"ingestion/{l2_dataset}.yaml")

    output_name = f"ingest_{source}"
    template = get_template("ingestion_dag_task_groups.py.j2")

    template.stream(
        {
            "l1_config": l1_config,
            "l2_config": {"dataset": l2_dataset, "tables": l2_config},
            "source": source,
        }
    ).dump(f"dags/{output_name}.py")
    echo(f"Rendered DAG: {source}")

@cli.command()
@argument("source")
@argument("datawarehouse", required=False)
def gendagdwh(source, datawarehouse):
    _gendagdwh(source, datawarehouse)

def _gendagdwh(source, datawarehouse, **context):
    l1_dataset = f"l1_{source}"
    l1_config = get_config(f"ingestion/{l1_dataset}.yaml")
    l1_config["dataset"] = l1_dataset

    l2_dataset = f"l2_{source}"
    l2_config = get_config(f"ingestion/{l2_dataset}.yaml")

    l3_dataset = ""
    l3_config = {}
    if datawarehouse is not None:
        l3_dataset = f"l3_{datawarehouse}"
        l3_config = get_config(context.get("l3_config", f"datawarehouse/{l3_dataset}.yaml"))
    
    # create query for scd
    if "scd" in list(l3_config.keys()):
        for table in list(l3_config["scd"].keys()):
            scd_template = get_template("l3_scd_sql.j2")
            scd_initial_template = get_template("scd_sql_initial_template.j2")
            with open(f"dags/sql/l3_{datawarehouse}.{table}_base_query.sql", "r") as base_query:
                base_query = base_query.read()
                scd_template.stream(
                    {
                        "table_destination": f"l3_{datawarehouse}.{table}",
                        "base_query": base_query
                    }
                ).dump(f"dags/sql/l3_{datawarehouse}.{table}.sql")

                scd_initial_template.stream(
                    {
                        "table_destination": f"l3_{datawarehouse}.{table}",
                        "base_query": base_query,
                        "l3_config": l3_config["scd"][table],
                        "table_name": table
                    }
                ).dump(f"dags/sql/scd_initial_sql/l3_{datawarehouse}.{table}.initial.sql")


    l4_dataset = ""
    l4_config = {}
    if datawarehouse is not None:
        l4_dataset = f"l4_{datawarehouse}"
        l4_config = get_config(context.get("l4_config", f"datawarehouse/{l4_dataset}.yaml"))
    
    
    output_name = context.get("output_name", f"dwh_{datawarehouse}_pipeline")
    
    template = get_template("ingestion_dwh_dag.py.j2")

    template.stream(
        {
            "l1_config": l1_config,
            "l2_config": {"dataset": l2_dataset, "tables": l2_config},
            "l3_config": {"dataset": l3_dataset, "tables": l3_config},
            "l4_config": {"dataset": l4_dataset, "tables": l4_config},
            "source": source,
            "datawarehouse": datawarehouse,
            "decryption_source": {
                "marketing": "marketing",
            }[source],
            "dag_id": output_name,
        }
    ).dump(f"dags/{output_name}.py")
    echo(f"Rendered DWH: {source} - {datawarehouse}")

@cli.command()
def gendagall():
    sources = [
        "marketing"
    ]

    for source in sources:
        _gendagtg(source)
        # _gensql(source)

    sources_dwh = ["marketing"]
    dwh = ["marketing"]

    l4_file_exist = ["marketing"]
    for source, warehouse in zip(sources_dwh, dwh):
        l4_config_file_name = ""
        if warehouse in l4_file_exist:
            l4_config_file_name = f"datawarehouse/l4_{warehouse}.yaml"

        _gendagdwh(
            source,
            warehouse,
            output_name=f"dwh_{warehouse}_scd_pipeline",
            l3_config=f"datawarehouse/l3_{warehouse}.yaml",
            l4_config=l4_config_file_name
        )

if __name__ == "__main__":
    cli()
