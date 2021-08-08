from convert_json_to_excel import custom_format, convert_json_to_csv, merge_multiple_file, main
from loguru import logger

if __name__ == '__main__':
    # custom_format(path=r"E:\OpenSourceCode\crawler(1)\gz.txt", typ="ershoufang",
    #               save_file_type="csv")

    merge_multiple_file([r"E:\OpenSourceCode\crawler(1)\gz.txt"] * 3, save_path="test.csv")
    # try:
    #     convert_json_to_csv(path=r"E:\OpenSourceCode\crawler(1)\gz.txt", typ="ershoufang")
    # except Exception as err:
    #     logger.exception(err)
