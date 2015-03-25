import logging
import requests
import urllib

from BeautifulSoup import BeautifulSoup
from itertools import izip


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Crawler(object):
    def crawl(self):
        year = 2014
        gender = 1
        offset = 0
        
        html = self.query(year, gender, offset)
        results = self.parse(html)


    def query(self, year, gender, offset):
        url = self.url(year, gender)

        response = requests.post(
            url,
            data={
                'start': offset + 1,
                'next': 'Next 25 Records',
            },
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
            'VarTargetCount': 1000,
            'headerexists': 'Yes',
            'mode': 'results',
            'records': 25,
            'queryname': 'SearchResults',
        }

        return base_url + '?' + urllib.urlencode(qs)


    def parse(self, html):
        soup = BeautifulSoup(html)

        tbody = soup.find('tbody')
        trs = tbody.findAll('tr')

        def pairwise(iterable):
            i = iter(iterable)
            return izip(i, i)

        return [
            self.parse_athlete(summary, splits) \
            for summary, splits in pairwise(trs[:-1])
        ]


    def parse_athlete(self, summary, splits):
        pass
    


if '__main__' == __name__:
    Crawler().crawl()
