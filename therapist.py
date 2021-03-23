import requests
import pandas as pd
from util import logging as log
from configs import therapist_config, filters
from datetime import datetime, timedelta
import statistics

pd.options.display.max_columns = None


class Therapist(object):

    logger = log.get_logger("Therapist")
    authentication_response = None
    therapist_id = None

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
        self.therapist_id = self.authentication_response.json()["userId"]
        print(response.json())
        return response

    def get_all_patients(self):
        """
        Queries patient of therapist.
        :return:
        """
        self.logger.info('Querying all patients for therapist.')
        base_url = "https://moodbuster-lite.science/api/monitoring/therapists/"
        request_url = base_url + self.authentication_response.json()["userId"] + \
                      "/patient?access_token=" + self.authentication_response.json()["id"]
        response = requests.get(request_url)
        return pd.json_normalize(response.json())["userId"].values.tolist()

    def get_all_patients_data(self):
        """
        Queries patient of therapist.
        :return:
        """
        self.logger.info('Querying all patients for therapist.')
        base_url = "https://moodbuster-lite.science/api/monitoring/therapists/"
        request_url = base_url + self.authentication_response.json()["userId"] + \
                      "/patient?access_token=" + self.authentication_response.json()["id"]
        response = requests.get(request_url)
        return pd.json_normalize(response.json())

    def get_all_messages(self, therapist_id=None):
        """
        queries messages of therapist per patient.
        :return:
        """
        if therapist_id is None:
            therapist_id = self.therapist_id

        self.logger.info('Querying all patients for therapist for their messages.')
        base_url = "https://moodbuster-lite.science/api/monitoring/therapists/"
        request_url = base_url + therapist_id + \
                      "/patient?" + filters.message_filter + "&access_token=" + self.authentication_response.json()["id"]

        print(request_url)
        response = requests.get(request_url)
        return response

    def get_all_patient_ratings(self, therapist_id=None):
        """
        queries ratings of patients.
        :return:
        """
        if therapist_id is None:
            therapist_id = self.therapist_id

        self.logger.info('Querying all patients of therapist for their ratings.')
        base_url = "https://moodbuster-lite.science/api/monitoring/therapists/"
        request_url = base_url + therapist_id + \
                      "/patient?" + filters.rating_filter + "&access_token=" + self.authentication_response.json()["id"]

        print(request_url)
        response = requests.get(request_url)
        return response

    def get_all_patient_notifications(self, therapist_id=None):
        """
        queries notifications of patients.
        :return:
        """
        if therapist_id is None:
            therapist_id = self.therapist_id

        self.logger.info('Querying all patients of therapist for their notifications.')
        base_url = "https://moodbuster-lite.science/api/monitoring/therapists/"
        request_url = base_url + therapist_id + \
                      "/patient?" + filters.notification_filter + "&access_token=" + self.authentication_response.json()["id"]

        print(request_url)
        response = requests.get(request_url)
        return response


def main():
    therapist = Therapist()
    therapist.log_in()
    patients = therapist.get_all_patients()
    all_patient_data = therapist.get_all_patients_data()

    all_notifications = therapist.get_all_patient_notifications()
    df = pd.json_normalize(all_notifications.json())

    all_ratings = therapist.get_all_patient_ratings()
    df = pd.json_normalize(all_ratings.json())

    all_messages = therapist.get_all_messages()
    df = pd.json_normalize(all_messages.json())


if __name__ == "__main__":
    main()
