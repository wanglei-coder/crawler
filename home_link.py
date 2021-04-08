# -*- coding: utf-8 -*-
"""
Created on 2020/7/7 13:31

@Author: Mamba

@Purpose:

@ModifyRecord:
"""
import json
import time
from dataclasses import dataclass
from typing import List, Optional

import click
import js2xml
from loguru import logger
from lxml import etree

from config import PATTERN
from tools import requests_get


# //*[@id="introduction"]/div[1]/div[1]/div[2]/ul/li[position()<=last()
@dataclass(unsafe_hash=True)
class Region:
    """ 区域名称和网址后缀"""
    name: str
    url: str


@dataclass(unsafe_hash=False)
class House:
    """房间网页完整地址， 标题，小区名字"""
    url: str = ""
    title: str = ""
    block_name: str = ""
    district: Optional[str] = None
    county: Optional[str] = None

    def __hash__(self):
        return hash(self.url)


class HomeLinkSpider:
    def __init__(self, city_abbreviation="km"):
        """
        Parameters
        ----------
        city_abbreviation: str, 市级别的缩写，如 成都-cd、上海-sh
        """
        self.city_abbreviation = city_abbreviation
        self.domain = "https://{}.lianjia.com".format(city_abbreviation)
        self.sub_domain = "https://{}.lianjia.com/chengjiao/".format(
            city_abbreviation)

    @staticmethod
    def get_selector(url):
        if isinstance(url, etree._Element):
            return url
        if isinstance(url, str):
            html = requests_get(url).text
            return etree.HTML(html)
        raise TypeError(url)

    def get_region(self, elem):
        name, url = elem.text, f"{self.domain}{elem.attrib['href']}"
        return Region(name, url)

    def get_districts(self) -> List[Region]:
        """ 得到区级别

        Returns
        -------
        List[SubRegion], [SubRegion(name='浦东', url_suffix='/chengjiao/pudong/'), ...]
        """
        try:
            selector = self.get_selector(self.sub_domain)
            pattern = "/html/body/div[3]/div[1]/dl[2]/dd/div/div/a[position()<=last()]"
            return [self.get_region(elem) for elem in selector.xpath(pattern)]
        except Exception as err:
            logger.error(err)

    def get_counties(self, region: Optional[Region]) -> List[Region]:
        """ 得到县级别

        Returns
        -------

        """
        try:
            selector = self.get_selector(region.url)
            pattern = "/html/body/div[3]/div[1]/dl[2]/dd/div/div[2]/a[position()<=last()]"
            return [self.get_region(elem) for elem in selector.xpath(pattern)]
        except Exception as err:
            logger.error(err)

    def get_total_page(self, selector):
        """ 这个县的页面个数

        Parameters
        ----------
        selector : str or etree._Element

        Returns
        -------
        tuple
        """
        try:
            pattern = "/html/body/div[5]/div[1]/div[5]/div[2]/div"
            selector = self.get_selector(selector)
            attrib = json.loads(selector.xpath(pattern)[0].attrib["page-data"])
            return attrib["totalPage"], attrib["curPage"]
        except Exception as err:
            logger.error(err)

    def get_house_from_current_page(self, selector):
        try:
            selector = self.get_selector(selector)
            pattern = "/html/body/div[5]/div[1]/ul/li[position()<=last()]/div/div[1]/a"
            house_list = []
            for house_info in selector.xpath(pattern):
                # 房间网页地址， 标题，小区名字
                house_url = house_info.attrib["href"]
                house_title = house_info.text
                block_name = house_title.split(" ")[0]
                house_list.append(House(house_url, house_title, block_name))
            return house_list
        except Exception as err:
            logger.error(err)

    @staticmethod
    def get_lng_lat(script):
        """ 获得经纬度

        Parameters
        ----------
        script

        Returns
        -------
        tuple
        """
        script = js2xml.parse(script, encoding='utf-8', debug=False)
        script = js2xml.pretty_print(script)
        script_selector = etree.HTML(script)
        lng, lat = script_selector.xpath(PATTERN["lng"])[0].split(",")
        return lng, lat, script_selector

    def get_house_all_info(self, house: House):
        """ 一个房间的所有信息

        Parameters
        ----------
        house : House

        Returns
        -------
        dict
        """
        try:
            selector = self.get_selector(house.url)
            script = selector.xpath("/html/body/script[11]/text()")[0]
            lng, lat, script_selector = self.get_lng_lat(script)
            query = selector.xpath

            def query_modify(item_name):
                try:
                    return query(PATTERN[item_name])[0].strip()
                except Exception as e:
                    logger.info("没有此项{}, 错误原因{}, 继续爬取".format(item_name, e))
                    return None

            item = {
                "房间代号": script_selector.xpath(PATTERN["房间代号"])[0],
                "城市": script_selector.xpath(PATTERN["城市"])[0],
                "区": house.district,
                "县": house.county,
                "小区": script_selector.xpath(PATTERN["小区"])[0],
                "lng": lng,
                "lat": lat,
                "单价": query(PATTERN["单价"])[0],
                "成交价格": query(PATTERN["成交价格"])[0],
                "挂牌价格": query(PATTERN["挂牌价格"])[0],
                "挂牌时间": query_modify("挂牌时间"),
                "成交时间": query(PATTERN["成交时间"])[0].split(" ")[0],
                "房屋户型": query_modify("房屋户型"),
                "房屋朝向": query_modify("房屋朝向"),
                "所在楼层": query_modify("所在楼层"),
                "配备电梯": query_modify("配备电梯"),
                "装修情况": query_modify("装修情况"),
                "建筑面积": query_modify("建筑面积"),
                "建筑类型": query_modify("建筑类型"),
                "建筑年代": query_modify("建筑年代"),
                "建筑结构": query_modify("建筑结构"),
                "梯户比例": query_modify("梯户比例"),
                "供暖方式": query_modify("供暖方式"),
                "交易权属": query_modify("交易权属"),
                "房屋用途": query_modify("房屋用途"),
                "房权所属": query_modify("房权所属"),
                "成交周期": query_modify("成交周期"),
                "调价次数": query_modify("调价次数"),
                "带看次数": query_modify("带看次数"),
                "浏览次数": query_modify("浏览次数"),
                "关注人数": query_modify("关注人数"),
                "成交小区均价": query_modify("成交小区均价"),
            }
            return item
        except Exception as err:
            # logger.info(err)
            logger.exception(err)

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

    def start_crawler(self):
        logger.info("Start crawler")
        districts = self.get_districts()
        if not districts:
            logger.error("没有区级区域", districts)
            return
        for district in districts:
            counties = self.get_counties(district)
            print(counties)
            if not counties:
                logger.error(f"{district} 没有县级区域")
                continue
            for county in counties:
                total_page = self.get_total_page(county.url)
                if not total_page:
                    continue
                total_page = total_page[0]
                for i in range(1, total_page):
                    page_url = f"{county.url}pg{i}"
                    house_list = self.get_house_from_current_page(page_url)
                    if not house_list:
                        logger.error(f"{page_url}：该页面没有房子")
                        continue
                    for house in house_list:
                        house.district = district.name
                        house.county = county.name
                        house_info = self.get_house_all_info(house)
                        if not house_info:
                            continue
                        self.save_json(
                            house_info, f"{self.city_abbreviation}.txt")
                        logger.info(house_info)
                        time.sleep(4)
        logger.info("Finished all")

    def start_crawler_counties(self, districts_list, counties_list):
        """
        Parameters
        ----------
        districts_list : list, 需要爬的区列表，such as ["通州"]
        counties_list : list, 需要爬的县列表，such as ['北关', '大兴其它', '果园', '九棵树(家乐福)', '临河里', '梨园', '潞苑', '马驹桥', '乔庄', '首都机场', '通州北苑', '通州其它', '万达', '武夷花园', '亦庄', '玉桥']
        """
        logger.info("Start crawler")
        districts = self.get_districts()
        districts = list(filter(lambda x: x.name in districts_list, districts))
        if not districts:
            logger.error("没有区级区域")
            return
        for district in districts:
            counties = self.get_counties(district)
            counties = list(
                filter(lambda x: x.name in counties_list, counties))
            if not counties:
                logger.error(f"{district} 没有县级区域")
                continue
            for county in counties:
                total_page = self.get_total_page(county.url)
                if not total_page:
                    continue
                total_page = total_page[0]
                for i in range(1, total_page):
                    page_url = f"{county.url}pg{i}"
                    house_list = self.get_house_from_current_page(page_url)
                    if not house_list:
                        logger.error(f"{page_url}：该页面没有房子")
                        continue
                    for house in house_list:
                        house.district = district.name
                        house.county = county.name
                        house_info = self.get_house_all_info(house)
                        if not house_info:
                            continue
                        self.save_json(house_info, f"{self.city_abbreviation}.txt")
                        print(house_info)
                        time.sleep(2)

        logger.info("Finished all")


@click.command()
@click.option("--city_abbreviation", help="A brief spelling of Chinese city names", default="bd")
def main(city_abbreviation):
    HomeLinkSpider(city_abbreviation).start_crawler()


if __name__ == '__main__':
    main()
    # HomeLinkSpider("cd").start_crawler()  # 重点改这里
    """
    districts_list = ["从化"]
    counties_list = ['鳌头镇', '北兴镇', '赤草', '河滨北路', '江埔街', '旧城区', '良口镇',
                     '神岗镇', '太平镇', '旺城片区', '温泉镇', '新城片区']
    HomeLinkSpider("gz").start_crawler_counties(
        #districts_list, counties_list[12:])  # 修改这里
    """
