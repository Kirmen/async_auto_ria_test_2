import datetime
import json
import math
import re

from bs4 import BeautifulSoup

from db_tools import DatabaseManager, put_to_db


async def find_last_page(url: str, headers: dict, session) -> int:
    async with session.get(url=str(url), headers=headers) as resp:
        resp_content = await resp.text()
        soup = BeautifulSoup(resp_content, 'lxml')

        script_tag_with_count_of_cars = soup.find('script',
                                                  string=re.compile(
                                                      r'window\.ria\.server\.resultsCount = Number\(\d+\);'))

        if script_tag_with_count_of_cars:
            script_content = script_tag_with_count_of_cars.text
            results_count_match = re.search(r'window\.ria\.server\.resultsCount = Number\((\d+)\);', script_content)

            if results_count_match:
                results_count = int(results_count_match.group(1))
                float_number = int(results_count)
                rounded_number = math.ceil(float_number / 100)
                return rounded_number
            else:
                print("Значення не знайдено.")
        else:
            print("Потрібний тег <script> не знайдено.")


async def scrap_data(response_text, session, url, headers):
    errors = (AttributeError, IndexError)
    soup = BeautifulSoup(response_text, "lxml")

    try:
        title = soup.find('h1').text
    except errors:
        title = None

    try:
        price_usd = int(
            soup.find('div', class_='price_value').find('strong').get_text(strip=True).replace(' ', '')[:-1])
    except Exception:
        try:
            price_usd = int(
                soup.find('div', class_='price_value--additional').find('span').find('span').get_text(
                    strip=True).replace(' ', ''))
        except Exception:
            price_usd = None
            print('price bag', url)

    try:
        username = soup.find('div', class_='seller_info_name').text.strip()
    except errors:
        try:
            username = soup.find('h4', class_='seller_info_name').text.strip()
        except errors:
            username = None

    try:
        odo_element = soup.find('span', string='Пробіг від продавця').find_next()
        odometer = int(re.findall(r'\b\d+\b', odo_element.text)[0]) * 1000
    except errors:
        try:
            odometer = int(
                re.findall(r'\b\d+\b', soup.find('div', id='details').find('span', string='Пробіг').find_next().text)[
                    0]) * 1000
        except errors:

            odometer = None

    id = \
        re.findall(r'\b\d+\b', soup.find_all('ul', class_='mb-10-list unstyle size13 mb-15')[0].find_all('li')[1].text)[
            0]
    data_hash = soup.select_one('[class*="js-user-secure"]').get('data-hash')
    phone_url = f'https://auto.ria.com/users/phones/{id}?hash={data_hash}'
    async with session.get(url=phone_url, headers=headers) as phone_response:
        response_str = await phone_response.text()
        data = json.loads(response_str)
        phone_number = ''.join(filter(str.isdigit, data.get('formattedPhoneNumber')))

    photo_gal = soup.find('div', class_='preview-gallery').find_all('source')
    images_urls = []
    for photo in photo_gal:
        photo_small = photo.get('srcset')
        photo_full = re.sub(r'(\d+)s', r'\1f', photo_small)
        images_urls.append(photo_full)

    images_count = len(images_urls)

    try:
        car_number = soup.find('span', class_='state-num').text[:10]
    except errors:
        car_number = None

    try:
        car_vin = soup.find('span', class_='label-vin').text
    except errors:
        try:
            car_vin = soup.find('span', class_='vin-code').text
        except errors:
            car_vin = None

    datetime_found = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    with DatabaseManager() as cursor:
        put_to_db(cursor, url, title, price_usd, odometer, username, phone_number, images_urls, images_count,
                  car_number, car_vin, datetime_found)
