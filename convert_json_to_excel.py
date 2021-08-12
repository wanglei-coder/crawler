# In[]
import json
import os
from pathlib import Path

import click
import pandas as pd
from loguru import logger


def load_data(path):
    data = []
    with open(path, "r") as f:
        for index, line in enumerate(f):
            try:
                _dict = json.loads(line)
                if isinstance(_dict, dict):
                    data.append(_dict)
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


def save_dict_to_csv(mapping, save_path, columns):
    df = pd.DataFrame([mapping])
    if os.path.exists(save_path):
        df.to_csv(save_path, mode="a", index=False, header=False, columns=columns)
    else:
        df.to_csv(save_path, mode="a", index=False, header=True, columns=columns)


def get_column_name(path):
    with open(path, "r") as f:
        for line in f:
            _dict = json.loads(line)
            if not isinstance(_dict, dict):
                continue
            return list(_dict.keys())


def merge_multiple_csv(path_list, save_path):
    df_total = None
    column_name = None
    df_list = []
    for path in path_list:
        df = pd.read_csv(path)
        if not column_name:
            column_name = list(df.columns)
        df = df[column_name]
        df_list.append(df)

    df_total = pd.concat(df_list)
    df_total = df_total.drop_duplicates(subset=["房间代号"], keep="first")
    df_total.to_csv(save_path)


def merge_multiple_file(path_list, save_path):
    """ 合并多个txt文件 并保存为csv文件"""
    columns = None
    if not isinstance(path_list, list):
        path_list = [path_list]

    columns = get_column_name(path_list[0])
    if not columns:
        return

    for path in path_list:
        with open(path, "r") as f:
            for idx, line in enumerate(f):
                try:
                    _dict = json.loads(line)
                    if isinstance(_dict, dict):
                        save_dict_to_csv(_dict, save_path, columns)
                except Exception as err:
                    logger.error(f"{path}, {idx}, {err}, {line}")


def custom_format(path, typ, save_file_type):
    """ 转换文件成csv"""
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


@click.command()
@click.option("--save_file_type", help="Save File Type", default="csv")
@click.option("--typ", help="Types of housing", default="chengjiao")
@click.option("--path", help="Filename")
def main(path, typ, save_file_type):
    custom_format(path, typ, save_file_type)


if __name__ == '__main__':
    main()
