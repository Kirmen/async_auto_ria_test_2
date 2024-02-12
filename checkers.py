from typing import List

from db_tools import DatabaseManager, is_url_in_db


def check_is_url_in_db(hrefs: List[str]) -> List[str]:
    fresh_urls = []
    with DatabaseManager() as cursor:
        for auto_ria_url in hrefs:
            if not is_url_in_db(auto_ria_url, cursor):
                fresh_urls.append(auto_ria_url)
    return fresh_urls
