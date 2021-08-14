import json
import time
import click
import requests
from lxml import etree
from neighborhood import NeighborhoodSpider, Region, Xpath, Neighborhood
from loguru import logger
from dataclasses import asdict
from convert_json_to_excel import convert_json_to_csv, custom_format

logger.add("neighborhood.log")


class NeighborhoodSpiderV1(NeighborhoodSpider):
    def __init__(self, city_abbreviation="nb", city_zh_name="湖州", typ="xiaoqu"):
        super(NeighborhoodSpiderV1, self).__init__(city_abbreviation, city_zh_name, typ)
        self.neighborhood_list = []
        self.save_path_name = f"{self.city_abbreviation}_{self.typ}"

    def get_districts(self):
        selector = etree.HTML(requests.get(self.sub_domain).text)
        for item in selector.xpath(Xpath.district):
            try:
                yield Region(item.text, item.attrib["href"])
            except Exception as err:
                logger.error(err)

    def get_counties_list(self):
        for district in self.get_districts():
            try:
                yield self.get_counties(district), district
            except Exception as err:
                logger.error(err)

    @staticmethod
    def get_neighborhood_from_current_page(url):
        selector = etree.HTML(requests.get(url).text)
        for item in selector.xpath(Xpath.neighborhood):
            yield Neighborhood(name=item.text, url=item.attrib["href"])

    def start_crawler_neighborhood_list(self):
        save_path_url = f"{self.save_path_name}.json"  # 保存爬取的链接
        for counties, district in self.get_counties_list():
            # if district.name in ["通州", "东城", "西城", "朝阳", "海淀", "丰台", "石景山"]:
            #     continue
            for county in counties:
                total_page = self.get_total_page(county)
                if not total_page:
                    continue
                for page in range(1, total_page + 1):
                    page_url = f"{self.domain}{county.url}pg{page}"
                    time.sleep(4)
                    for neighborhood in self.get_neighborhood_from_current_page(page_url):
                        try:
                            neighborhood.county = county.name
                            neighborhood.district = district.name
                            neighborhood.city_name = self.city_zh_name
                            self.neighborhood_list.append(neighborhood)
                            logger.info(asdict(neighborhood))
                            self.save_json(asdict(neighborhood), save_path_url)
                        except Exception as err:
                            logger.error(err)
                        finally:
                            time.sleep(0)

        self.get_all_neighborhood()

    def get_neighborhood_list_from_file(self, filename):
        _url_list = []
        with open(filename, "rb") as f:
            for idx, line in enumerate(f):
                try:
                    _dict = json.loads(line)
                    if not isinstance(_dict, dict):
                        continue
                except Exception as err:
                    logger.error(err)
                    continue
                neighborhood = Neighborhood(**_dict)
                if neighborhood.url not in _url_list:
                    self.neighborhood_list.append(neighborhood)
                    _url_list.append(neighborhood.url)
        logger.info(f"neighborhood_list: {len(self.neighborhood_list)}")

    def get_neighborhood(self, neighborhood: Neighborhood, idx=0):
        save_path = f"{self.save_path_name}.txt"
        try:
            neighborhood = self.get_neighborhood_detail_info(neighborhood)
            logger.info(f"idx: {idx}, {neighborhood}")
            self.save_json(neighborhood.as_dict(), save_path)
        except Exception as err:
            logger.error(err)
        finally:
            time.sleep(4)

    def get_all_neighborhood(self, start=0, end=0):
        if end == 0:
            end = 1 << 64
        _url_list = []
        neighborhood_list = []
        # 去重
        for neighborhood in self.neighborhood_list:
            if neighborhood.url not in _url_list:
                neighborhood_list.append(neighborhood)
        del _url_list
        for idx, neighborhood in enumerate(neighborhood_list):
            if not start <= idx <= end:
                continue
            self.get_neighborhood(neighborhood, idx)
        custom_format(f"{self.save_path_name}.txt", self.typ)

    def start_crawler_from_file(self, filename, start=0, end=0):
        self.get_neighborhood_list_from_file(filename)
        self.get_all_neighborhood(start=start, end=end)


def crawler_neighborhood(city_abbreviation, city_zh_name, file, start, end):
    n_v1 = NeighborhoodSpiderV1(city_abbreviation=city_abbreviation, city_zh_name=city_zh_name)
    if file != "":
        n_v1.start_crawler_from_file(file, start, end)
    else:
        n_v1.start_crawler_neighborhood_list()


@click.command()
@click.option("--city_zh_name", help="The chinese city name", default="北京")
@click.option("--city_abbreviation", help="A brief spelling of Chinese city names", default="bj")
@click.option("--file", help="from file", default="")
@click.option("--start", help="start index", default=0)
@click.option("--end", help="end index", default=0)
def main(city_abbreviation, city_zh_name, file, start, end):
    """
    python neighborhood_v1.py --city_zh_name 北京 --city_abbreviation bj
    python neighborhood_v1.py --city_zh_name 北京 --city_abbreviation bj --file bj_xiaoqu.json --start 0 --end 5000
    """
    crawler_neighborhood(city_abbreviation, city_zh_name, file, start, end)


if __name__ == '__main__':
    main()
