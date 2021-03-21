# In[]
import os
import json
import argparse
import click
import pandas as pd

from loguru import logger
from pathlib import Path


def load_data(path):
    data = []
    with open(path, "r") as f:
        for index, line in enumerate(f):
            try:
                data.append(json.loads(line))
            except Exception as err:
                logger.error(f"{index}: {line}", err)
    df = pd.DataFrame(data)
    df = df.drop_duplicates()
    dirname = Path(path).parent.__str__()
    name = Path(path).name
    return df, dirname, name


def convert_json_to_excel(path, typ):
    df, dirname, name = load_data(path)
    save_name = os.path.join(dirname, f"{name}_{typ}.xls")
    df.to_excel(save_name)
    logger.success(f"转换成功，保存到{save_name}")


def convert_json_to_csv(path, typ):
    df, dirname, name = load_data(path)
    save_name = os.path.join(dirname, f"{name}_{typ}.csv")
    df.to_csv(save_name)
    logger.success(f"转换成功，保存到{save_name}")


@click.command()
@click.option("--save_file_type", help="Save File Type", default="csv")
@click.option("--typ", help="Types of housing", default="chengjiao")
@click.option("--path", help="Filename")
def main(path, typ, save_file_type):
    if save_file_type == "txt":
        return
    if save_file_type == "csv":
        try:
            convert_json_to_csv(path, typ)
        except Exception as err:
            logger.error("convert failed: {}".format(err))
    else:
        try:
            convert_json_to_excel(path, typ)
        except Exception as err:
            logger.error("convert failed: {}".format(err))
            convert_json_to_csv(path, typ)


if __name__ == '__main__':
    main()
