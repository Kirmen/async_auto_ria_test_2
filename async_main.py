import asyncio
import configparser

import aiohttp
import schedule
from bs4 import BeautifulSoup
from furl import furl

from checkers import check_is_url_in_db
from db_tools import create_db, create_database_dump
from scrap_tools import scrap_data, find_last_page


async def get_page_data(headers, url, session, page, semaphore):
    furl_url = furl(url)
    furl_url.args['page'] = str(page)

    try:
        async with semaphore:
            async with session.get(url=str(furl_url), headers=headers) as response:
                response_text = await response.text()

                hrefs = BeautifulSoup(response_text, "lxml").find_all('a', class_='m-link-ticket')
                all_hrefs = [h.get('href') for h in hrefs]
                fresh_car_hrefs = check_is_url_in_db(all_hrefs)

                for fresh_href in fresh_car_hrefs:
                    try:

                        async with session.get(url=str(fresh_href), headers=headers) as detail_response:

                            detail_response_text = await detail_response.text()
                            await scrap_data(detail_response_text, session, fresh_href, headers)
                    except Exception as e_detail:
                        print(f"Error in detail request: {e_detail}, {fresh_href=}")
                        with open('error_pages.html', 'a', encoding='utf-8') as file:
                            file.write(detail_response_text)

    except Exception as e_page:
        print(f"Error on page {page} request: {e_page}")


async def gather_data(headers, url='https://auto.ria.com/uk/search/?indexName=auto&page=0&size=100',
                      semaphore_limit=20):
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(semaphore_limit)
        page_counter = await find_last_page(url, headers, session)
        tasks = []

        for page in range(0, page_counter + 1):
            task = asyncio.create_task(get_page_data(headers, url, session, page, semaphore))
            tasks.append(task)

        await asyncio.gather(*tasks)


async def main():
    create_db()

    config = configparser.ConfigParser()
    config.read('conf.config')
    auto_ria_start_url = config.get('AUTORIA', 'START_URL')
    headers = {'user-agent': config.get('HEADERS', 'USER_AGENT')}

    schedule.every().day.at("15:06").do(
        lambda: asyncio.create_task(gather_data(url=auto_ria_start_url, headers=headers)))
    schedule.every().day.at("00:00").do(create_database_dump)

    while True:
        await asyncio.sleep(1)
        schedule.run_pending()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
