from convert_json_to_excel import custom_format, convert_json_to_csv, merge_multiple_file, main, merge_multiple_csv
from loguru import logger
from home_link_v1 import HomeLinkSpiderV1

if __name__ == '__main__':
    # HomeLinkSpiderV1("gz").start_crawler_by_district_name("番禺")
    # merge_multiple_file(r"D:\gz.txt", r"D:\gz2.txt")
    path_list = [r"D:\gz.csv", r"D:\gz2.csv"]
    merge_multiple_csv(path_list, r"D:\gz_all.csv")
