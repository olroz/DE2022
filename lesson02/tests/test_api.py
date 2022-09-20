"""
Tests api.py
"""
import sys
from unittest import TestCase, mock


sys.path.append("/Users/olegr/work/_mine/DataEngineering/py/DE2022_my/lesson02/job1")
from job1 import api


class GetSalesTestCase(TestCase):
    """
    Test api.get_sales function.
    """
    def test_date_param(self):
        """
        Raise 501 HTTP code when 'date' param is wrong
        """
        resp = api.get_sales('200q-01-01')

        self.assertEqual(501, resp[1])
