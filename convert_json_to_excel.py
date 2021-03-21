# In[]
import os
import json
import argparse
import pandas as pd

from loguru import logger
from pathlib import Path
# python convert_json_to_excel.py --path C:\Users\Chaley\PycharmProjects\home_link\77.txt --typ ershoufang
# python convert_json_to_excel.py --path C:\Users\Chaley\PycharmProjects\home_link\zb_xiaoqu.txt --typ xiaoqu
parser = argparse.ArgumentParser()
parser.add_argument("--path", type=str)
parser.add_argument("--typ", type=str, default="ershoufang")
args = parser.parse_args()


def load_data(path):
    data = []
    with open(path, "r") as f:
        for index, line in enumerate(f):
            try:
                data.append(json.loads(line))
            except Exception as err:
                logger.error(f"{index}: {line}", err)
    return data


def convert_json_to_excel(path, typ):
    data = load_data(path)
    df = pd.DataFrame(data)
    df = df.drop_duplicates()
    dirname = Path(path).parent.__str__()
    name = Path(path).name
    save_name = os.path.join(dirname, f"{name}_{typ}.xls")
    df.to_excel(save_name)
    logger.success(f"转换成功，保存到{save_name}")


def convert_json_to_csv(path, typ):
    data = load_data(path)
    df = pd.DataFrame(data)
    df = df.drop_duplicates()
    dirname = Path(path).parent.__str__()
    name = Path(path).name
    save_name = os.path.join(dirname, f"{name}_{typ}.csv")
    df.to_csv(save_name)
    logger.success(f"转换成功，保存到{save_name}")


def main():
    path = args.path
    typ = args.typ
    try:
        convert_json_to_excel(path, typ)
    except Exception as err:
        logger.error("不能转换成excel，尝试转化成csv", err)
        convert_json_to_csv(path, typ)

if __name__ == '__main__':
    main()