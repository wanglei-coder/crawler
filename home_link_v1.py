import json
import time
from dataclasses import asdict

import click
import redis
from loguru import logger

from convert_json_to_excel import custom_format
from home_link import HomeLinkSpider, House

RedisHost = "139.198.190.139"
RedisPort = 6379
RedisPassword = "68270854"
RedisDB = 0


class HomeLinkSpiderV1(HomeLinkSpider):
    def __init__(self, city_abbreviation="km", use_redis=False):
        self.url_list = []
        self.url_set = set()
        super(HomeLinkSpiderV1, self).__init__(city_abbreviation)
        # self.city_abbreviation = city_abbreviation
        self.use_redis = use_redis
        self.redis_client = None

    def get_url_list(self, district_name_list=None):
        logger.info("start getting url")
        districts = self.get_districts()

        if not districts:
            logger.error("没有区级区域", districts)
            return
        for district in districts:
            if district.name not in district_name_list:
                continue
            try:
                counties = self.get_counties(district)
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
                            if house.url in self.url_set:
                                continue
                            house.district = district.name
                            house.county = county.name
                            logger.info(house)
                            self.url_set.add(house.url)
                            self.url_list.append(house)
                            self.save_house_json(house)
                            if self.use_redis:
                                self.save_house_redis(house)
                        time.sleep(4)
            except Exception as err:
                logger.exception(f"district: {err}")
                continue

    def get_url_list_path(self):
        try:
            return self.city_abbreviation + "_url_list.json"
        except Exception as err:
            logger.info(err)
            return "url_list.json"

    def save_house_json(self, house: House):
        if isinstance(house, House):
            house = asdict(house)
        if isinstance(house, dict):
            self.save_json(house, self.get_url_list_path())
            logger.success(f"store house to file: {house}")

    def save_house_redis(self, house):

        if self.redis_client is None:
            self.redis_client = redis.Redis(host=RedisHost,
                                            password=RedisPassword,
                                            port=RedisPort,
                                            db=RedisDB)

        if not hasattr(self.redis_client, "ping"):
            return

        if not self.redis_client.ping():
            return

        if isinstance(house, House):
            house = asdict(house)
        try:
            self.redis_client.lpush(self.city_abbreviation, json.dumps(house))
            logger.success(f"store house to redis: {house}")
        except Exception as err:
            logger.error(f"failed to push redis: {house}, err: {err}")

    def _start_crawler_house(self, house):
        if isinstance(house, dict):
            house = House(url=house.get("url", None),
                          title=house.get("title", None),
                          block_name=house.get("block_name", ""),
                          district=house.get("district", ""),
                          county=house.get("county", "")
                          )

        if not isinstance(house, House):
            return

        house_info = self.get_house_all_info(house)
        self.save_json(house_info, f"{self.city_abbreviation}.txt")
        logger.success(house_info)

    def start_crawler_house(self, house):
        try:
            self._start_crawler_house(house)
        except Exception as err:
            logger.error(f"start_crawler err: {err}")
        finally:
            time.sleep(4)

    def start_crawler(self):
        self.get_url_list()
        length = len(self.url_list)
        logger.info(f"self.url_list: {length}")

        self.start_crawler_house_list(self.url_list)

    def start_crawler_by_district_name(self, district_name_list):
        if isinstance(district_name_list, str):
            district_name_list = [district_name_list]
        self.get_url_list(district_name_list=district_name_list)
        length = len(self.url_list)
        logger.info(f"self.url_list: {length}")

        self.start_crawler_house_list(self.url_list)

    def start_crawler_house_list(self, house_list):
        for house in house_list:
            self.start_crawler_house(house)
        logger.info("save to csv file...")
        custom_format(path=f"{self.city_abbreviation}.txt", typ="ershoufang", save_file_type="csv")

    def start_crawler_from_redis(self):
        pool = redis.Redis(host=RedisHost, password=RedisPassword, port=RedisPort, db=RedisDB)
        if not pool.ping():
            logger.error("redis disconnect")
            return

        while True:
            body = pool.brpop(self.city_abbreviation)
            house = json.loads(body)
            self._start_crawler_house(house)

    def start_crawler_from_file(self, filename, start=0, end=0):
        """
        start: 开始行数
        end: 结束行数
        """
        if end == 0:
            end = 1 << 64
        with open(filename, "r") as f:
            for idx, line in enumerate(f):
                if start <= idx <= end:
                    house = json.loads(line)
                    self.start_crawler_house(house)
        logger.info("save to csv file...")
        custom_format(path=f"{self.city_abbreviation}.txt", typ="ershoufang", save_file_type="csv")


def crawler_home_link(city_abbreviation, source, file, start, end):
    if source == "":
        HomeLinkSpiderV1(city_abbreviation, use_redis=False).start_crawler()
        return

    if source == "file":
        HomeLinkSpiderV1(city_abbreviation).start_crawler_from_file(file, start, end)
        return

    if source == "redis":
        HomeLinkSpiderV1(city_abbreviation).start_crawler_from_redis()


@click.command()
@click.option("--city_abbreviation", help="A brief spelling of Chinese city names", default="bd")
@click.option("--source", help="A brief spelling of Chinese city names", default="")
@click.option("--file", help="A brief spelling of Chinese city names", default="cd.json")
@click.option("--start", help="A brief spelling of Chinese city names", default=0)
@click.option("--end", help="A brief spelling of Chinese city names", default=0)
def main(city_abbreviation, source, file, start, end):
    """
    python home_link_v1.py --city_abbreviation gz
    python home_link_v1.py --city_abbreviation gz --source file --file gz_url_list.json --start 60000
    """
    crawler_home_link(city_abbreviation, source, file, start, end)


if __name__ == '__main__':
    main()
# python home_link_v1.py --city_abbreviation gz --source file --file gz_url_list.json --start 60000
