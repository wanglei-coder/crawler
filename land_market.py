import time
import json
import os
import re
import requests
import copy
import pandas as pd
import click

from bs4 import BeautifulSoup
from lxml import etree
from selenium import webdriver

DomainUrl = "https://www.landchina.com"


def extract_all_url(tree: etree.HTML, prefix="/DesktopModule/BizframeExtendMdl/workList"):
    url_list = []
    for link in tree.xpath("//@href"):
        link_str = str(link)
        if link_str.startswith(prefix):
            link_str = DomainUrl + link_str
            url_list.append(link_str)
    return url_list


def save_json(mapping, save_path):
    """ 保存成json"""
    if not isinstance(mapping, str):
        mapping = json.dumps(mapping)
    with open(save_path, "a+") as fd:
        fd.write(mapping + "\n")


def save_csv(mapping, save_path):
    columns = ['宗地编号', '宗地总面积', '宗地坐落', '出让年限', '容积率', '建筑密度', '绿化率',
               '建筑限高', '主要用途',  '面积', '投资强度', '保证金', '估价报告备案号',
               '起始价', '加价幅度', '挂牌开始时间', '挂牌截止时间']
    df = pd.DataFrame([mapping])
    if os.path.exists(save_path):
        df.to_csv(save_path, mode="a", index=None,
                  header=False, columns=columns)
    else:
        df.to_csv(save_path, mode="a", index=None,
                  header=True, columns=columns)


class DecodeText:
    def __init__(self):
        self.font_map = {}
        self.load_font_table()

    def load_font_table(self):
        with open("font_table.txt", encoding="utf-8") as fp:
            for line in fp:
                infos = line.split(':')
                self.font_map[infos[0].strip()[2:]] = infos[1].strip()

    @staticmethod
    def cn_to_unicode(string, need_str=True):
        out = []

        for s in string:
            # 获得该字符的数值
            val = ord(s)

            # 小于0xff则为ASCII码，手动构造\u00xx格式
            if val <= 0xff:
                hex_str = hex(val).replace('0x', '').zfill(4)
                # 这里不能以unicode_escape编码，不然会自动增加一个'\\'
                res = bytes('\\u' + hex_str, encoding='utf-8')
            else:
                res = s.encode("unicode_escape")

            out.append(res)

        # 转换为str类
        if need_str:
            out_str = ''
            for s in out:
                out_str += str(s, encoding='utf-8')
            return out_str
        else:
            return out

    @staticmethod
    def unicode_to_cn(string):
        if isinstance(string, bytes):
            temp = str(string, encoding='utf-8')
            return temp.encode('utf-8').decode('unicode_escape')
        else:
            return string.encode('utf-8').decode('unicode_escape')

    def html_trans(self, content):
        content_new = copy.deepcopy(content)
        for i in content:
            str_ = self.cn_to_unicode(i)
            new_str = self.font_map.get(str_[2:].upper(), '')
            if new_str:
                content_new = content_new.replace(i, new_str)
        return content_new

    def convert(self, string):
        return self.html_trans(string)


class TableParser:
    def __init__(self, url):
        self.url = url
        self.decode_text = DecodeText()

    def by_pandas(self):
        return pd.read_html(self.url, encoding="gbk")

    @staticmethod
    def is_valid_table(table: pd.DataFrame):
        flag = ["宗箹编号", "出让鹽限"]
        for f in flag:
            if f not in table.__str__():
                return False
        return True

    def get_valid_table(self):
        tables = []
        for table in self.by_pandas():
            if self.is_valid_table(table):
                tables.append(table)
        return tables

    def by_bs4(self):
        req = requests.get(self.url)
        soup = BeautifulSoup(req.content.decode(encoding="gbk"))
        tables = soup.find_all(
            'table', attrs={'width': "100%", 'border': '1', 'cellpadding': '1'})
        for table in tables:
            td_list = table.find_all("td")

    def convert_text(self, dict_):
        new_dict = {}
        for k, v in dict_.items():
            new_dict[k] = self.decode_text.convert(v)
        return new_dict

    @staticmethod
    def extract_table2dict(table: pd.DataFrame):
        # item_key_list = ['宗地编号', '宗地总面积', '宗地坐落', '出让年限', '容积率', '建筑密度', '绿化率',
        #                  '建筑限高', '主要用途', '用途名称', '面积', '投资强度', '保证金', '估价报告备案号',
        #                  '起始价', '加价幅度', '挂牌开始时间', '挂牌截止时间']
        try:
            dict_ = {
                '宗地编号': table.loc[0, 1],
                '宗地总面积': table.loc[0, 3],
                '宗地坐落': table.loc[0, 5],
                '出让年限': table.loc[1, 1],
                '容积率': table.loc[1, 3],
                '建筑密度': table.loc[1, 5],
                '绿化率': table.loc[2, 1],
                '建筑限高': table.loc[2, 3],
                '主要用途': table.loc[4, 0],
                '面积': table.loc[7, 3],
                '投资强度': table.loc[8, 1],
                '保证金': table.loc[8, 3],
                '估价报告备案号': table.loc[8, 5],
                '起始价': table.loc[9, 1],
                '加价幅度': table.loc[9, 3],
                '挂牌开始时间': table.loc[10, 1],
                '挂牌截止时间': table.loc[10, 3],
            }
        except Exception:
            dict_ = {}
        return dict_

    def extract(self):
        items = []
        tables = self.get_valid_table()
        for table in tables:
            table = table.fillna("无")
            dict_ = self.extract_table2dict(table)
            if not dict_:
                continue
            dict_ = self.convert_text(dict_)
            items.append(dict_)
        return items


class LandMarketCrawler:
    def __init__(self, start_page, end_page, suffix="/default.aspx?tabid=261"):
        self.domain = DomainUrl + suffix

        self.start_page = int(start_page)
        self.end_page = int(end_page)

        self.page = 0
        # self.domain = "https://www.landchina.com/default.aspx?tabid=261"
        option = webdriver.ChromeOptions()
        option.add_argument('--headless')
        option.add_argument("start-maximized")
        option.add_argument("--disable-blink-features=AutomationControlled")
        option.add_experimental_option(
            "excludeSwitches", ["enable-automation"])
        option.add_experimental_option("useAutomationExtension", False)
        self.browser = webdriver.Chrome(options=option)
        time.sleep(2)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.browser.close()

    def first_page(self):
        self.browser.get(self.domain)

    def click_page(self, page):
        self.page = page
        self.browser.execute_script(
            "QueryAction.GoPage('TAB',{})".format(page))

    def click_next_page(self):
        self.page += 1
        self.browser.execute_script(
            "QueryAction.GoPage('TAB',{})".format(self.page))

    def get_tree(self):
        return etree.HTML(self.browser.page_source)

    def get_url(self):
        return extract_all_url(self.get_tree())

    def get_total_page(self):
        pass

    def get_all_by_page(self):
        save_path = f"{self.start_page}_{self.end_page}.csv"
        self.first_page()
        dicts = []
        urls = []
        for page in range(self.start_page, self.end_page):
            self.click_page(page)
            time.sleep(0.5)
            urls.extend(self.get_url())
        for url in urls:
            d = TableParser(url).extract()
            for v in d:
                save_csv(v, save_path)
                print(v)
            # dicts.extend(d)
        # return dicts


@click.command()
@click.option("--start_page", help="开始页数", default="安庆")
@click.option("--end_page", help="结束页数", default=None)
def main(start_page, end_page):
    """
     Command:
    - 指定日期

    python land_market.py --start_page 1 --end_page 4
    """
    with LandMarketCrawler(start_page, end_page) as crawler:
        crawler.get_all_by_page()


if __name__ == '__main__':
    main()
