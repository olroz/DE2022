"""
Tests for job1 main.py
"""
import sys
from unittest import TestCase, mock
sys.path.append("/Users/olegr/work/_mine/DataEngineering/py/DE2022_my/lesson02/job1")
from job1 import main


class Job1MainFunctionTestCase(TestCase):
    """
    Main tests for job1
    """
    print("******* job1 test main:")

    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()

    def test_return_code(self):
        """
        Raise 400 HTTP code when no 'date' and/or 'path' param
        """
        resp = self.client.post(
            '/',
            json={
                'raw_dir': '/foo/bar/',
                # no 'date' set
            },
        )
        resp2 = self.client.post(
            '/',
            json={
                'date': '2000-01-01',
                # no 'path' set
            },
        )
        resp3 = self.client.post(
            '/',
            json={
                'date': '3000-01-01',
                'raw_dir': '/foo/bar/'
            },
        )
        resp4 = self.client.post(
            '/',
            json={
                'date': '202a-01-01',
                'raw_dir': '/foo/bar/'
            },
        )
        self.assertEqual(400, resp.status_code)
        self.assertEqual(400, resp2.status_code)
        self.assertEqual(404, resp3.status_code)
        self.assertEqual(501, resp4.status_code)


    @mock.patch('main.storage.save_to_disk')
    @mock.patch('main.api.get_sales')
    def test_return_success_code(
            self,
            get_sales: mock.MagicMock,
            save_to_disk: mock.MagicMock,
    ):
        """
        Get 201 HTTP code when 'date' and 'path' are set
        """
        get_sales.return_value = {
                "data": '',
                }, 201
        save_to_disk.return_value = 'ok'
        resp = self.client.post(
            '/',
            json={
                'date': '2000-01-01',
                'raw_dir': '/foo/bar/'
            },
        )
        self.assertEqual(201, resp.status_code)


    @mock.patch('main.storage.save_to_disk')
    @mock.patch('main.api.get_sales')
    def test_api_get_sales_and_save_to_disk_called(
            self,
            get_sales: mock.MagicMock,
            save_to_disk: mock.MagicMock,
    ):
        """
        Test whether api.get_sales and storage.save_to_disk are called with proper params
        """
        fake_date = '2022-01-01'
        fake_path = '/foo/bar'
        self.client.post(
            '/',
            json={
                'date': fake_date,
                'raw_dir': fake_path,
            },
        )
        get_sales.assert_called_with(fake_date)
        save_to_disk.assert_called_with(get_sales()[0]['data'], fake_path+"/sales_"+fake_date)
