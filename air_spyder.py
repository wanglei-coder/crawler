import datetime
import json
import os
import time

import click
import pandas as pd
from bs4 import BeautifulSoup
from loguru import logger
from lxml import etree
from selenium import webdriver

from tools import requests_get


def date_range(start, end, step=1, format="%Y%m"):
    strptime, strftime = datetime.datetime.strptime, datetime.datetime.strftime
    days = (strptime(end, format) - strptime(start, format)).days
    return set([strftime(strptime(start, format) + datetime.timedelta(i), format) for i in
                range(0, days, step)])


def get_all_city_names():
    city_names = []
    html = requests_get("https://www.aqistudy.cn/historydata/daydata.php").text
    soup = BeautifulSoup(html, 'html.parser')
    for elem in soup.find_all('a'):
        if "city=" in elem.__str__():
            city_names.append(elem.text)
    city_names = list(set(city_names))
    return city_names


class AirSpyder:
    def __init__(self, city_name, start_time=None, stop_time=None, dirname="."):
        self.city_name = city_name
        self.dirname = dirname
        self.start_time = start_time
        self.stop_time = stop_time
        self.domain = "https://www.aqistudy.cn/historydata/daydata.php?city={}&month={}"
        option = webdriver.ChromeOptions()
        option.add_argument("start-maximized")
        option.add_argument("--disable-blink-features=AutomationControlled")
        option.add_experimental_option("excludeSwitches", ["enable-automation"])
        option.add_experimental_option("useAutomationExtension", False)
        self.browser = webdriver.Chrome(options=option)
        time.sleep(2)

    @staticmethod
    def get_selector(url):
        if isinstance(url, etree._Element):
            return url
        if isinstance(url, str):
            html = requests_get(url).text
            return etree.HTML(html)
        raise TypeError(url)

    @staticmethod
    def _get_date(elem: str):
        try:
            d = int(elem.replace("年", "").replace("月", ""))
            return d
        except Exception as err:
            logger.error(err)
            return None

    def get_dates(self):
        """ 获取城市有数据的日期"""
        if self.start_time:
            if not self.stop_time:
                self.stop_time = datetime.date.today().strftime("%Y%m")
            if int(self.stop_time) < int(self.start_time):
                self.start_time, self.stop_time = self.stop_time, self.start_time
            dates = date_range(self.start_time, self.stop_time, step=1, format="%Y%m")
            dates = list(dates)
            return sorted(dates)

        url = "https://www.aqistudy.cn/historydata/daydata.php?city={}".format(self.city_name)
        try:
            selector = self.get_selector(url)
            pattern = "/html/body/div[3]/div[1]/div[2]/div[2]/div[2]/ul/li[position()<=last()]/a"
            return sorted([self._get_date(elem.text) for elem in selector.xpath(pattern)])
        except Exception as err:
            logger.error(err)
            raise Exception(err)

    @staticmethod
    def save_json(mapping, save_path):
        """ 保存成json"""
        if not isinstance(mapping, str):
            mapping = json.dumps(mapping)
        with open(save_path, "a+") as fd:
            fd.write(mapping + "\n")

    @staticmethod
    def bulk_save_json(records, save_path):
        with open(save_path, "a+") as fd:
            for record in records:
                record = json.dumps(record)
                fd.write(record + "\n")

    def get_one(self, date):
        self.browser.get(self.domain.format(self.city_name, date))
        time.sleep(3)
        df: pd.DataFrame = pd.read_html(self.browser.page_source, header=0)[0]
        if not df.empty:
            logger.info(df)
            df["city"] = self.city_name
            save_path = os.path.join(self.dirname, self.city_name + ".csv")
            df.to_csv(save_path, mode="a", index=None)
            logger.success(f"[City]: {self.city_name}   [Date]:{date}    [DONE]")
        else:
            logger.success(f"[City]: {self.city_name}   [Date]:{date}    [None]")

    def start_crawler(self):
        dates = self.get_dates()
        for d in dates:
            try:
                self.get_one(d)
            except Exception as err:
                logger.error(f"[City]: {self.city_name}   [Date]:{d}    [FAILED]: {err}")


@click.command()
@click.option("--city_name", help="Chinese city names", default="安庆")
@click.option("--start_time", help="开始日期", default=None)
@click.option("--stop_time", help="结束日期", default=None)
@click.option("--dirname", help="Save dirname", default=".")
@click.option("--alone", help="", default="one")
def main(city_name, start_time, stop_time, dirname, alone):
    """
    1. 需要安装selenium，pip install selenium

    2. 需要安装pandas，pip install pandas

    3. 需要下载和谷歌浏览器版本一样的无头浏览器，下载地址：http://npm.taobao.org/mirrors/chromedriver/,
       解压后放在anaconda/bin目录下

    Command:
    - 指定日期

    python air_spyder.py --city_name "北京" --start_time 201409 --stop_time 201607

    - 不指定日期

    python air_spyder.py --city_name "北京"

    - 爬所有城市

    python air_spyder.py --start_time 201409 --stop_time 201607 --alone all

    python air_spyder.py --alone all
    """
    if alone == "one":
        AirSpyder(city_name, start_time, stop_time, dirname).start_crawler()
    else:
        city_names = get_all_city_names()
        logger.info(city_names)
        raise
        for city_name in city_names:
            AirSpyder(city_name, start_time, stop_time, dirname).start_crawler()


if __name__ == '__main__':
    main()
