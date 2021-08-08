# -*- coding: utf-8 -*-
"""
Created on 2020/7/26 10:32

@Author: Mamba

@Purpose:

@ModifyRecord:
"""
import json
import random
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Optional

import click
import js2xml
import requests
from loguru import logger
from lxml import etree
from retry import retry

from tools import ua_list


def get_useragent():
    """ 获取随机user_agent"""
    return random.choice(ua_list)


@retry(tries=5, delay=1)
def requests_get(url):
    """ 增加headers 和 proxies

    Parameters
    ----------
    url : str, url 链接
    """
    headers = {'User-Agent': get_useragent(), }
    response = requests.get(url=url, headers=headers)
    if response.status_code != requests.codes.ok:
        raise Exception('request_get error!!!!')
    return response


@dataclass
class Region:
    """ 区域名称和网址后缀"""
    name: str
    url: str


@dataclass
class Location:
    lng: Optional[float] = None  # 经度
    lat: Optional[float] = None  # 维度


@dataclass
class DetailInfo:
    building_age: Optional[float] = None  # 2000
    building_types: Optional[str] = None  # 板楼
    property_cost: Optional[str] = None  # 1.55至2.5元/平米/月
    property_company: Optional[str] = None  # 上海万科物业管理有限公司
    property_developers: Optional[str] = None  # 上海万邦企业集团有限公司
    num_building: Optional[float] = None  # 楼栋总数
    num_house: Optional[float] = None  # 房屋总数


@dataclass
class Neighborhood(Location, DetailInfo):
    city_name: Optional[str] = None  # 上海
    district: Optional[str] = None  # 浦东
    county: Optional[str] = None  # 北蔡
    name: Optional[str] = None  # 万邦都市花园
    url: Optional[str] = None
    address: Optional[str] = None  # (浦东北蔡)龙阳路1880弄
    unit_price: Optional[float] = None  # 74944

    def as_dict(self):
        return OrderedDict({
            "城市": self.city_name,
            "行政区": self.district,
            "板块": self.county,
            "小区名称": self.name,
            "地址": self.address,
            "挂牌均价(元/m2)": self.unit_price,
            "建筑年代": self.building_age,
            "建筑类型": self.building_types,
            "物业费用": self.property_cost,
            "物业公司": self.property_company,
            "开发商": self.property_developers,
            "楼栋总数(栋)": self.num_building,
            "房屋总数(户)": self.num_house,
            "经度": self.lng,
            "纬度": self.lat,
        })


@dataclass
class Xpath:
    district: Optional[
        str] = "/html/body/div[3]/div[1]/dl[2]/dd/div/div/a[position()<=last()]"
    county: Optional[
        str] = "/html/body/div[3]/div[1]/dl[2]/dd/div/div[2]/a[position()<=last()]"
    total_page: Optional[str] = "/html/body/div[4]/div[1]/div[3]/div[2]/div"
    neighborhood: Optional[
        str] = "/html/body/div[4]/div[1]/ul/li[position()<=last()]/div[1]/div[1]/a"
    building_age: Optional[
        str] = "/html/body/div[6]/div[2]/div[2]/div[1]/span[2]"
    building_types: Optional[
        str] = "/html/body/div[6]/div[2]/div[2]/div[2]/span[2]"
    property_cost: Optional[
        str] = "/html/body/div[6]/div[2]/div[2]/div[3]/span[2]"
    property_company: Optional[
        str] = "/html/body/div[6]/div[2]/div[2]/div[4]/span[2]"
    property_developers: Optional[
        str] = "/html/body/div[6]/div[2]/div[2]/div[5]/span[2]"
    num_building: Optional[
        str] = "/html/body/div[6]/div[2]/div[2]/div[6]/span[2]"
    num_house: Optional[str] = "/html/body/div[6]/div[2]/div[2]/div[7]/span[2]"
    unit_price: Optional[str] = "/html/body/div[6]/div[2]/div[1]/div/span[1]"
    lng_lat_script: Optional[str] = "/html/body/script[9]/text()"
    lng_lat: Optional[
        str] = "//property[@name = 'resblockPosition']/string/text()"
    address: Optional[str] = "/html/body/div[4]/div/div[1]/div"


class NeighborhoodSpider:
    """
    get neighborhood data
    """

    def __init__(self, city_abbreviation="nb", city_zh_name="湖州", typ="xiaoqu"):
        """
        city_abbreviation: str, 城市名称英文简写
        city_zh_name: str, 城市中文
        """
        self.city_abbreviation = city_abbreviation
        self.domain = "https://{}.lianjia.com".format(city_abbreviation)
        self.sub_domain = "https://{}.lianjia.com/{}/".format(
            city_abbreviation, typ)
        self.city_zh_name = city_zh_name
        self.typ = typ

    def get_districts(self):
        selector = etree.HTML(requests_get(self.sub_domain).text)
        return [Region(item.text, item.attrib["href"]) for item in
                selector.xpath(Xpath.district)]

    def get_counties(self, region: Region):
        url = self.domain + region.url
        selector = etree.HTML(requests_get(url).text)
        return [Region(item.text, item.attrib["href"]) for item in
                selector.xpath(Xpath.county)]

    def get_total_page(self, region: Region):
        try:
            url = self.domain + region.url
            selector = etree.HTML(requests_get(url).text)
            attrib = json.loads(selector.xpath(
                Xpath.total_page)[0].attrib["page-data"])
            return attrib["totalPage"]
        except Exception as err:
            logger.error(err)

    @staticmethod
    def get_neighborhood_from_current_page(url):
        selector = etree.HTML(requests_get(url).text)
        return [Neighborhood(name=item.text, url=item.attrib["href"]) for item
                in
                selector.xpath(Xpath.neighborhood)]

    @staticmethod
    def selector_xpath(selector, pattern):
        try:
            return selector.xpath(pattern)[0].text
        except Exception as err:
            logger.error("have no this pattern data: {}".format(err))
            return None

    def get_neighborhood_detail_info(self, neighborhood: Neighborhood):
        selector = etree.HTML(requests_get(neighborhood.url).text)
        neighborhood.building_age = self.selector_xpath(
            selector, Xpath.building_age)
        neighborhood.building_types = self.selector_xpath(
            selector, Xpath.building_types)
        neighborhood.property_cost = self.selector_xpath(
            selector, Xpath.property_cost)
        neighborhood.property_company = self.selector_xpath(
            selector, Xpath.property_company)
        neighborhood.property_developers = self.selector_xpath(
            selector, Xpath.property_developers)
        neighborhood.num_building = self.selector_xpath(
            selector, Xpath.num_building)
        neighborhood.num_house = self.selector_xpath(selector, Xpath.num_house)
        neighborhood.unit_price = self.selector_xpath(
            selector, Xpath.unit_price)
        try:
            lng_lat_script = selector.xpath(Xpath.lng_lat_script)[0]
            lng, lat, _ = self.get_lng_lat(lng_lat_script)
        except Exception as err:
            logger.error("unable to extract lng and lat: {}".format(err))
            lng, lat = None, None
        neighborhood.lng = lng
        neighborhood.lat = lat
        neighborhood.address = self.selector_xpath(selector, Xpath.address)
        return neighborhood

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
        lng, lat = script_selector.xpath(Xpath.lng_lat)[0].split(",")
        return lng, lat, script_selector

    @staticmethod
    def save_json(mapping, save_path):
        """ 保存成json"""
        if not isinstance(mapping, str):
            mapping = json.dumps(mapping)
        with open(save_path, "a+") as fd:
            fd.write(mapping + "\n")

    def start_crawler(self):
        save_path = f"{self.city_abbreviation}_{self.typ}.txt"
        districts = self.get_districts()
        counties_list = [(self.get_counties(district), district)
                         for district in districts]
        logger.info(f"districts: {districts}")
        logger.info(f"counties_list: {counties_list}")
        for counties, district in counties_list:
            for county in counties:
                total_page = self.get_total_page(county)
                if not total_page:
                    continue
                for page in range(1, total_page + 1):
                    page_url = f"{self.domain}{county.url}pg{page}"
                    try:
                        neighborhood_list = self.get_neighborhood_from_current_page(page_url)
                    except Exception as err:
                        logger.error("获取页面小区列表错误", err)
                        time.sleep(8)  # 5
                        continue
                    for neighborhood in neighborhood_list:
                        try:
                            neighborhood.county = county.name
                            neighborhood.district = district.name
                            neighborhood.city_name = self.city_zh_name
                            neighborhood = self.get_neighborhood_detail_info(neighborhood)
                            logger.info(neighborhood)
                            self.save_json(neighborhood.as_dict(), save_path)
                            time.sleep(5)  # 3
                        except Exception as err:
                            logger.error(err)
                            time.sleep(12)  # 10
                            continue


@click.command()
@click.option("--city_zh_name", help="The chinese city name", default="北京")
@click.option("--city_abbreviation",
              help="A brief spelling of Chinese city names", default="bj")
def main(city_abbreviation, city_zh_name):
    NeighborhoodSpider(city_abbreviation=city_abbreviation,
                       city_zh_name=city_zh_name).start_crawler()


if __name__ == '__main__':
    main()
