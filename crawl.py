import csv
import itertools
import logging
import requests
import socket
import urllib

from BeautifulSoup import BeautifulSoup
from hashlib import md5
from itertools import izip
from memcache import Client


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Crawler(object):
    def __init__(self):
        self.cache = self.get_cache()
        self.step = 25


    def get_cache(self):
        try:
            socket.create_connection(('localhost', 11211))
            logger.info('using local memcached')

            return Client(['localhost:11211'])
        except socket.error:
            logger.info('no local memcached')
            return None


    def crawl(self):
        year = 2014
        genders = (1, 2)
        results = []
        
        for gender in genders:
            gender_results = list(
                itertools.chain(*self.results_generator(year, gender))
            )

            results.extend(gender_results)

        return results


    def results_generator(self, year, gender):
        offset = 0

        while True:
            params = {
                'year': year,
                'gender': gender,
                'offset': offset,
            }

            html = self.query(params)
            results = self.parse(html)

            if not results:
                return

            yield results

            offset += self.step


    def query(self, params):
        logger.info('querying for {}'.format(params))

        url = self.url(params['year'], params['gender'])
        data={
            'start': params['offset'] + 1,
        }

        cache_key = md5(url + str(data)).hexdigest()

        if self.cache:
            html = self.cache.get(cache_key)

            if html is not None:
                logger.info('retrieved from cache')
                return html

        logger.info('querying server')
        html = self.query_server(url, data)

        if self.cache:
            self.cache.set(cache_key, html, time=0)

        return html


    def query_server(self, url, data):
        response = requests.post(
            url,
            data=data,
            headers={
                'Origin': 'http://registration.baa.org',
                'Content-Type': 'application/x-www-form-urlencoded',
            }
        )

        if 200 != response.status_code:
            raise RuntimeError((
                response.status_code, response.text
            ))

        return response.text


    def url(self, year, gender):
        base_url = (
            'http://registration.baa.org'
            '/{year}/cf/Public/iframe_ResultsSearch.cfm'
        ).format(year=year)

        qs = {
            'StoredProcParamsOn': 'yes',
            'VarAwardsDivID': 0,
            'VarBibNumber': '',
            'VarCity': '',
            'VarFirstName': '',
            'VarCountryOfCtzID': 0,
            'VarCountryOfResID': 0,
            'VarGenderId': gender,
            'VarLastName': '',
            'VarQualClassID': 0,
            'VarReportingSegID': 1,
            'VarStateID': 0,
            'VarTargetCount': 100000,
            'headerexists': 'Yes',
            'mode': 'results',
            'records': self.step,
            'queryname': 'SearchResults',
        }

        return base_url + '?' + urllib.urlencode(qs)


    def parse(self, html):
        soup = BeautifulSoup(html)

        tbody = soup.find('tbody')
        trs = tbody.findAll('tr', recursive=False)

        def pairwise(iterable):
            i = iter(iterable)
            return izip(i, i)

        return [
            self.parse_athlete(summary, splits) \
            for summary, splits in pairwise(trs[:-1])
        ]


    def parse_athlete(self, summary, splits):
        results = {}

        summary_tds = summary.findAll('td')
        summary_keys = [
            'bib', 'name', 'age', 'gender', 'city', 'state', 'country',
            'citizenship'
        ]

        results.update({
            summary_keys[i]: summary_tds[i].text.replace('&nbsp;', '') \
            for i in range(len(summary_keys))
        })

        splits_trs = splits.findAll('tr')
        splits_tds = list(itertools.chain(*[
            tr.findAll('td') for tr in (splits_trs[1], splits_trs[3])
        ]))
        splits_keys = [
            '5k', '10k', '15k', '20k', 'Half', '25k', '30k', '35k', '40k',
            'pace', 'projected time', 'official time', 'place_overall',
            'place_gender', 'place_division'
        ]

        results.update({
            splits_keys[i]: splits_tds[i].text.replace('&nbsp;', '') \
            for i in range(len(splits_keys))
        })

        return results
    


if '__main__' == __name__:
    results = Crawler().crawl()

    filename = 'crawl.csv'
    logger.info('writing {} results to {}'.format(
        len(results),
        filename
    ))

    with open(filename, 'wb') as f:
        writer = csv.DictWriter(f, results[0].keys())
        writer.writeheader()

        for result in results:
            writer.writerow(result)
