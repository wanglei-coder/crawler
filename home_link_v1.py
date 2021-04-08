import time

import click
from loguru import logger

from convert_json_to_excel import main as formater
from home_link import HomeLinkSpider


class HomeLinkSpiderV1(HomeLinkSpider):
    def __init__(self, city_abbreviation="km"):
        self.url_list = []
        self.url_set = set()
        super(HomeLinkSpiderV1, self).__init__(city_abbreviation)

    def get_url_list(self):
        logger.info("start getting url")
        districts = self.get_districts()
        if not districts:
            logger.error("没有区级区域", districts)
            return
        for district in districts:
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
                        else:
                            house.district = district.name
                            house.county = county.name
                            self.url_set.add(house.url)
                            self.url_list.append(house)
                    time.sleep(4)

    def start_crawler(self):
        self.get_url_list()
        for house in self.url_list:
            house_info = self.get_house_all_info(house)
            self.save_json(house_info, f"{self.city_abbreviation}.txt")
            logger.info(house_info)
            time.sleep(4)
        logger.info("save to csv file...")
        formater(path=f"{self.city_abbreviation}.txt", typ="ershoufang", save_file_type="csv")


@click.command()
@click.option("--city_abbreviation", help="A brief spelling of Chinese city names", default="bd")
def main(city_abbreviation):
    HomeLinkSpider(city_abbreviation).start_crawler()


if __name__ == '__main__':
    main()
