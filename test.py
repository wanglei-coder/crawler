from convert_json_to_excel import custom_format, convert_json_to_csv, merge_multiple_file, main, merge_multiple_csv
from loguru import logger
from home_link_v1 import HomeLinkSpiderV1
from neighborhood_v1 import NeighborhoodSpiderV1

if __name__ == '__main__':
    # HomeLinkSpiderV1("gz").start_crawler_by_district_name("番禺")
    # merge_multiple_file(r"D:\gz.txt", r"D:\gz2.txt")
    # path_list = [r"bj_xiaoqu.txt"]
    # merge_multiple_csv(path_list, r"D:\bj_all.csv")
    # n = NeighborhoodSpiderV1(city_abbreviation="bj", city_zh_name="北京")
    # n.start_crawler_from_file("bj_xiaoqu.json", start=3000)
    convert_json_to_csv(r"D:\bj_xiaoqu.txt", "xiaoqu")
