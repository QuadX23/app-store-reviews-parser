import argparse
import csv
import json
import logging
import os
import sys
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from functools import wraps
from operator import itemgetter
from pathlib import Path
from typing import Optional, Tuple, List, Union
from urllib.parse import unquote, quote

from requests_html import HTMLSession

APPS_BASE_URL = 'https://apps.apple.com'

MAX_REVIEWS = 5200
REVIEWS_PER_PAGE = 10
BASE_PATH = Path(os.path.dirname(__file__))
LOG_FILE_PATH = BASE_PATH / '.log'


def setup_logger(logger_name: str, log_file: Union[str, os.PathLike], stream=sys.stdout, file_mode: str = 'a+'):
    """Setup logger settings"""
    formatter = logging.Formatter(
        fmt='[{asctime}] [{name}] {levelname}: {message}',
        datefmt='%m/%d/%Y %H:%M:%S',
        style='{'
    )
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler(stream=stream)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file, mode=file_mode, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


LOGGER = setup_logger('AppStore Parser', LOG_FILE_PATH)


# TODO convert datetime str to datetime
@dataclass()
class DeveloperResponse:
    developer_id: int
    body: str
    modified: str


@dataclass()
class Review:
    user_name: str
    title: str
    review: str
    is_edited: bool
    last_update: str
    rating: int
    developer_response: Optional[DeveloperResponse]


def auth_required(func):
    """
    Auth if required
    """

    @wraps(func)
    def wrapped(self, *args, **kwargs):
        if not self._auth_token:
            self._auth()
        return func(self, *args, **kwargs)

    return wrapped


class AppStoreParser:
    """
    AppStore app reviews parser
    """

    def __init__(self, app_name: str, app_id: int):
        """
        Initialize parser

        :str app_name: App Store app name
        :int app_id: App Store app id
        """
        self.app_name = app_name
        self.app_id = app_id
        self.__session = HTMLSession()
        self._auth_token = None

    def _auth(self):
        """
        Get App Store auth token
        """
        LOGGER.info('Authorization in App Store...')
        app_url = f'{APPS_BASE_URL}/ru/app/{self.app_name}/id{self.app_id}'
        response = self.__session.get(app_url)
        app_meta = response.html.find('meta[name="web-experience-app/config/environment"]', first=True)
        app_data = json.loads(unquote(app_meta.attrs['content']))
        auth_token = app_data['MEDIA_API']['token']
        self._auth_token = auth_token
        LOGGER.info('Successfully authorized in App Store')

    def get_reviews(self) -> List[Review]:
        """
        Parse all available App reviews
        """
        count = 0
        next_page = f'/v1/catalog/ru/apps/{self.app_id}/reviews?l=ru&offset=0'
        reviews = []
        while next_page:
            next_page, _reviews = self._get_reviews(offset=count)
            count += len(_reviews)
            reviews.extend(_reviews)

        return reviews

    def get_reviews_page(self, page: int) -> List[Review]:
        """
        Parse App reviews page

        :int page: page number
        """
        assert page > 0
        page_offset = (page - 1) * REVIEWS_PER_PAGE
        _, reviews = self._get_reviews(offset=page_offset)
        return reviews

    @auth_required
    def get_app_info(self) -> dict:
        """
        Return App info
        """
        response = self.__session.get(
            f'https://amp-api.apps.apple.com/v1/catalog/RU/apps/{self.app_id}',
            headers={
                'origin': APPS_BASE_URL,
                'referer': quote(f'{APPS_BASE_URL}/ru/app/{self.app_name}/id{self.app_id}'),
                'authorization': f'Bearer {self._auth_token}'
            },
            params={
                'platform': 'web',
                'additionalPlatforms': 'appletv,ipad,iphone,mac',
                'l': 'ru-ru',
                # 'extend': 'description,developerInfo,editorialVideo,eula,fileSizeByDevice,messagesScreenshots,privacyPolicyUrl,privacyPolicyText,promotionalText,screenshotsByType,supportURLForLanguage,versionHistory,videoPreviewsByType,websiteUrl',
                # 'include': 'genres,developer,reviews,merchandised-in-apps,customers-also-bought-apps,developer-other-apps,app-bundles,top-in-apps,eula'
            }
        )
        response.raise_for_status()
        return response.json()

    def get_app_rating_count(self) -> int:
        """
        Return App rating
        """
        app_info = self.get_app_info()
        app_attributes = app_info['data'][0]['attributes']
        user_rating = app_attributes['userRating']
        rating_count = user_rating['ratingCount']
        return rating_count

    @auth_required
    def _get_reviews(self, offset: int = 0) -> Tuple[str, List[Review]]:
        """
        Return list of app Review with offset

        :int offset: reviews offset
        """
        LOGGER.info(f'Scanning reviews from page #{offset // REVIEWS_PER_PAGE:03d}')
        response = self.__session.get(
            f'https://amp-api.apps.apple.com/v1/catalog/ru/apps/{self.app_id}/reviews',
            headers={
                'origin': APPS_BASE_URL,
                'referer': quote(f'{APPS_BASE_URL}/ru/app/{self.app_name}/id{self.app_id}'),
                'authorization': f'Bearer {self._auth_token}'
            },
            params={
                'l': 'ru',
                'additionalPlatforms': 'appletv,ipad,iphone,mac',
                'platform': 'web',
                'offset': offset
            }
        )
        response.raise_for_status()
        page_json = response.json()
        reviews_data = page_json['data']
        reviews = []
        for review in reviews_data:
            review_attrs = review['attributes']
            developer_response = review_attrs.get('developerResponse')
            if developer_response is not None:
                developer_response = DeveloperResponse(
                    developer_id=developer_response['id'],
                    body=developer_response['body'],
                    modified=developer_response['modified']
                )
            reviews.append(Review(
                user_name=review_attrs['userName'],
                review=review_attrs['review'],
                is_edited=review_attrs['isEdited'],
                last_update=review_attrs['date'],
                rating=review_attrs['rating'],
                title=review_attrs['title'],
                developer_response=developer_response,
            ))

        return page_json.get('next'), reviews


def write_reviews(reviews: List[Review], output_file_path: Union[str, os.PathLike]):
    """
    Write list of Review in CSV file

    :List[Review] reviews: reviews to write
    :Union[str, os.PathLike] output_file_path: output file path
    """
    fieldnames = (
        'user_name',
        'title',
        'review',
        'is_edited',
        'last_update',
        'rating',
    )
    LOGGER.info(f'Writing reviews in {output_file_path}...')
    with open(output_file_path, 'w', newline='', encoding='utf-8') as f:
        csv_writer = csv.DictWriter(f, fieldnames=fieldnames)
        csv_writer.writeheader()
        for review in reviews:
            _review = review.__dict__.copy()
            del _review['developer_response']
            csv_writer.writerow(_review)

    LOGGER.info(f'{len(reviews)} reviews successfully written to {output_file_path}')


def parse_parallel(parser: AppStoreParser, max_workers=20) -> List[Review]:
    """
    Parse app reviews in parallel

    :AppStoreParser parser: parser object
    :int max_workers: the maximum number of threads that can be used to parse reviews
    """

    rating_count = parser.get_app_rating_count()
    LOGGER.info(f'App "{parser.app_name}" has {rating_count} reviews')
    if rating_count > MAX_REVIEWS:
        rating_count = MAX_REVIEWS
        LOGGER.warning(f'App "{parser.app_name}" has more than {MAX_REVIEWS} reviews')

    last_page = rating_count // REVIEWS_PER_PAGE
    LOGGER.info(f'Reviews to scan: {rating_count}')

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        pages_range = range(1, last_page + 1)
        future_to_page = {
            executor.submit(parser.get_reviews_page, page): page for page in pages_range
        }
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                reviews = future.result()
            except Exception as exc:
                LOGGER.error(f'Exception on page #{page:03d}: {exc!r}')
            else:
                LOGGER.info(f'Page #{page:03d} successfully scanned')
                results.append((page, reviews))

    results.sort(key=itemgetter(0))
    reviews = [r for reviews_from_page in results for r in reviews_from_page[1]]
    LOGGER.info(f'Scanned reviews: {len(reviews)}')
    return reviews


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='App Store app reviews parser')
    parser.add_argument('app_name', type=str, help='App Store app name')
    parser.add_argument('app_id', type=int, help='App Store app name')
    args = parser.parse_args()

    parser = AppStoreParser(app_name=args.app_name, app_id=args.app_id)
    reviews = parse_parallel(parser)
    output_path = BASE_PATH / f'{args.app_name}.csv'
    write_reviews(reviews, output_path)
