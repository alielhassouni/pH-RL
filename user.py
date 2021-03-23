import requests
import pandas as pd
from util import logging as log
from configs import therapist_config, filters


class User(object):

    logger = log.get_logger("User")
    authentication_response = None
    user_id = None

    def log_in(self):
        """
        Login a user with username/email and password.
        POST /users/login
        :return:
        """
        self.logger.info('Logging in therapist account.')
        base_url = "https://moodbuster-lite.science/api/monitoring/users/login"
        parameters = {"username": therapist_config.THERAPIST_USER_NAME,
                      "password": therapist_config.THERAPIST_PASSWORD}
        response = requests.post(base_url, parameters)
        self.authentication_response = response
        self.user_id = self.authentication_response.json()["userId"]

        print(self.authentication_response.json()["id"])
        return response

    def get_user_information(self, id):
        """
        GET /users/{id}
        Find a model instance by {{id}} from the data source.
        :return:
        """
        self.logger.info('Querying user model for therapist.')
        base_url = "https://moodbuster-lite.science/api/monitoring/users/"
        request_url = base_url + id + \
                      "?access_token=" + self.authentication_response.json()["id"]
        print(request_url)
        response = requests.get(request_url)
        return pd.json_normalize(response.json())

    def get_user_login_app_event(self, user_id, start_date=None, end_date=None):
        """
        Get user login app event
        :param user_id:
        :param start_date:
        :param end_date:
        :return:
        """
        self.logger.info('Retrieving user login app events.')
        if user_id is None:
            user_id = self.user_id

        base_url = "https://moodbuster-lite.science/api/monitoring/users/"
        request_url = base_url + user_id + \
                      "?" + filters.login_filter + "&access_token=" + self.authentication_response.json()["id"]
        print(request_url)
        response = requests.get(request_url)
        return response

    def get_user_logout_app_event(self, user_id, start_date=None, end_date=None):
        """
        Get user logout app event
        :param user_id:
        :param start_date:
        :param end_date:
        :return:
        """
        self.logger.info('Retrieving user logout app events.')
        if user_id is None:
            user_id = self.user_id

        base_url = "https://moodbuster-lite.science/api/monitoring/users/"
        request_url = base_url + user_id + \
                      "?" + filters.logout_filter + "&access_token=" + self.authentication_response.json()["id"]
        print(request_url)
        response = requests.get(request_url)
        return response

    def get_user_sync_app_event(self, user_id, start_date=None, end_date=None):
        """
        Get user sync app event
        :param user_id:
        :param start_date:
        :param end_date:
        :return:
        """
        self.logger.info('Retrieving user sync app events.')
        if user_id is None:
            user_id = self.user_id

        base_url = "https://moodbuster-lite.science/api/monitoring/users/"
        request_url = base_url + user_id + \
                      "?" + filters.sync_filter + "&access_token=" + self.authentication_response.json()["id"]
        print(request_url)
        response = requests.get(request_url)
        return response

    def get_user_language_app_event(self, user_id, start_date=None, end_date=None):
        """
        Get user language app event
        :param user_id:
        :param start_date:
        :param end_date:
        :return:
        """
        self.logger.info('Retrieving user language app events.')
        if user_id is None:
            user_id = self.user_id

        base_url = "https://moodbuster-lite.science/api/monitoring/users/"
        request_url = base_url + user_id + \
                      "?" + filters.language_filter + "&access_token=" + self.authentication_response.json()["id"]
        print(request_url)
        response = requests.get(request_url)
        return response

    def get_user_app_app_event(self, user_id, start_date=None, end_date=None):
        """
        Get user app app event
        :param user_id:
        :param start_date:
        :param end_date:
        :return:
        """
        self.logger.info('Retrieving user app app events.')
        if user_id is None:
            user_id = self.user_id

        base_url = "https://moodbuster-lite.science/api/monitoring/users/"
        request_url = base_url + user_id + \
                      "?" + filters.app_filter + "&access_token=" + self.authentication_response.json()["id"]
        print(request_url)
        response = requests.get(request_url)
        return response

    def get_user_view_app_event(self, user_id, start_date=None, end_date=None):
        """
        Get user view app event
        :param user_id:
        :param start_date:
        :param end_date:
        :return:
        """
        self.logger.info('Retrieving user view app events.')
        if user_id is None:
            user_id = self.user_id

        base_url = "https://moodbuster-lite.science/api/monitoring/users/"
        request_url = base_url + user_id + \
                      "?" + filters.view_filter + "&access_token=" + self.authentication_response.json()["id"]
        print(request_url)
        response = requests.get(request_url)
        return response

    def get_user_close_view_app_event(self, user_id, start_date=None, end_date=None):
        """
        Get user close view app event
        :param user_id:
        :param start_date:
        :param end_date:
        :return:
        """
        self.logger.info('Retrieving user close view app events.')
        if user_id is None:
            user_id = self.user_id

        base_url = "https://moodbuster-lite.science/api/monitoring/users/"
        request_url = base_url + user_id + \
                      "?" + filters.close_view_filter + "&access_token=" + self.authentication_response.json()["id"]
        print(request_url)
        response = requests.get(request_url)
        return response

    def get_user_record_app_event(self, user_id, start_date=None, end_date=None):
        """
        Get user record app event
        :param user_id:
        :param start_date:
        :param end_date:
        :return:
        """
        self.logger.info('Retrieving user record app events.')

        if user_id is None:
            user_id = self.user_id

        base_url = "https://moodbuster-lite.science/api/monitoring/users/"
        request_url = base_url + user_id + \
                      "?" + filters.record_filter + "&access_token=" + self.authentication_response.json()["id"]
        print(request_url)
        response = requests.get(request_url)
        return response


def main():
    user = User()
    user.log_in()
    user_id = "id"
    user_data_df = user.get_user_information(user_id)
    user_app_data_df = user.get_user_record_app_event(user_id)


if __name__ == "__main__":
    main()