import requests
import pandas as pd
from util import logging as log
from util import util
from configs import therapist_config, filters
from therapist import Therapist
from user import User
import time, os, json, argparse
from datetime import datetime, timedelta
import statistics
import random
from s3.s3 import S3Client
from io import StringIO # python3; python2: BytesIO


DATE_FORMAT = "%Y-%m-%d"
pd.options.display.max_columns = None


class PreprocessRLWorkflow(object):

    def __init__(self,):
        self.state_columns = ["serverTimestamp", "day_part", "user_id", "numberRating", "highestRating", "lowestRating",
                              "medianRating", "sdRating", "numberLowRating", "numberMediumRating", "numberHighRating",
                              "numberMessageReceived", "numberMessageRead", "readAllMessage"]

        self.reward_columns = ["serverTimestamp", "day_part", "user_id", "reward"]
        self.action_columns = ["serverTimestamp", "day_part", "user_id", "action"]

    logger = log.get_logger("PreprocessRLWorkflow")

    therapist = Therapist()
    therapist.log_in()

    all_ratings = pd.json_normalize(therapist.get_all_patient_ratings().json())
    all_messages = pd.json_normalize(therapist.get_all_messages().json())

    def run_workflow(self, start_date, end_date, user_id=None, write_s3=False):
        """
        Create a training dataset for the period at hand.
        :param start_date: The start date of the from which data will be queried.
        :param end_date: The end date of the from which data will be queried.
        :param user_id: The id of the user if available.
        :param write_s3: Write to S3 bucket.
        :return: A data frame with training data for RL.
        """
        user_ids = self.get_user_id(user_id)
        states, rewards, actions = self.run_rl_data_pre_processing_workflow(start_date, end_date, user_ids)
        # Write to S3 bucket/append
        # self.write to S3
        return states, rewards, actions

    def get_user_id(self, user_id):
        """
        Get all user ids to be considered.
        :param user_id: The id of the user if available.
        :return: A list of user id's to be considered.
        """
        if user_id is not None:
            user_ids = [user_id]
        else:
            user_ids = self.therapist.get_all_patients()

        return user_ids

    def run_rl_data_pre_processing_workflow(self, start_date, end_date, user_ids):
        """
        Run the preprocessing workflow that creates a dataframe consisting of experieinces.
        :param start_date: The start date of the from which data will be queried.
        :param end_date: The end date of the from which data will be queried.
        :param user_id: A list of ids users to be considered.
        :return: A data frame with training data for RL.
        """
        states = self.get_state(start_date, end_date, user_ids)
        #actions = self.generate_actions(start_date, end_date, user_ids)
        actions = self.get_action(start_date, end_date, user_ids)
        rewards = self.get_reward(start_date, end_date, user_ids)

        return states, rewards, actions

    def get_action(self, start_date=None, end_date=None, user_ids=None):
        """
        Gte all actions.
        :param start_date: The start date of the from which data will be queried.
        :param end_date: The end date of the from which data will be queried.
        :param user_id: A list of ids users to be considered.
        :return: A data frame with action data for RL.
        """
        client = S3Client("eu-central-1")
        object = client.get_object(bucket_name="aelhassouni-mob-phd-research", object_name="test/actions/actions.csv")
        csv = object.get()['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv))
        del df["Unnamed: 0"]

        return df

    def get_reward(self, start_date, end_date, user_ids):
        """
        Get reward for users for period.
        :param start_date: The start date of the from which data will be queried.
        :param end_date: The end date of the from which data will be queried.
        :param user_ids: A list of ids users to be considered.
        :return: A data frame with rewards training data for RL.
        """
        all_users_rewards = pd.DataFrame(columns=self.reward_columns)

        for user in user_ids:
            all_users_rewards = self.get_rewards_one_user_multiple_period(start_date, end_date, user, all_users_rewards)

        return all_users_rewards

    def get_rewards_one_user_multiple_period(self, start_date, end_date, user_id, all_users_rewards):
        """
        Get reward from user.
        :param start_date: The start date from which data will be queried.
        :param end_date: The end date of the from which data will be queried.
        :param user_id: A list of ids users to be considered.
        :return: A data frame with rewards training data for RL for user id.
        """
        dates = pd.date_range(start=start_date, end=end_date, closed=None, freq='D') # freq='9H'
        # user_rewards_all_dates = pd.DataFrame(columns = self.reward_columns)

        for date in dates:
            all_users_rewards = self.get_reward_one_user_one_period(date, user_id, all_users_rewards)
            #user_rewards_all_dates = user_rewards_all_dates.append(user_reward)

        return all_users_rewards

    def get_reward_one_user_one_period(self, date, user_id, all_users_rewards):
        """
        Get reward from user.
        :param start_date: The start date from which data will be queried.
        :param end_date: The end date of the from which data will be queried.
        :param user_id: A list of ids users to be considered.
        :return: A data frame with rewards training data for RL for user id.
        """
        day_parts = ["morning", "afternoon", "evening"]
        # user_rewards_all_day_parts = pd.DataFrame(columns = self.reward_columns)

        for day_part in day_parts:
            user_reward = self.get_reward_one_user_one_period_day_part(date, user_id, day_part)
            # user_rewards_all_day_parts = user_rewards_all_day_parts.append(user_reward)
            s = pd.Series(user_reward, index=self.reward_columns)
            all_users_rewards = all_users_rewards.append(s, ignore_index=True)

        return all_users_rewards

    def get_reward_one_user_one_period_day_part(self, date, user_id, day_part):
        """
        Get reward from user.
        :param start_date: The start date from which data will be queried.
        :param end_date: The end date of the from which data will be queried.
        :param user_id: A list of ids users to be considered.
        :return: A data frame with rewards training data for RL for user id.
        """
        message_reward = 0
        rating_reward = 0
        scalar_reward = 0

        rating = self.get_rating_feature(date, user_id, day_part)
        message = self.get_message_feature(date, user_id, day_part)

        if message[0] is not None and message[0] > 0:
            message_reward = message[1]/message[0]

        if rating[3] is not None:
            rating_reward = rating[3]

        scalar_reward = [rating[0], rating[1], rating[2], 0.5*message_reward + 0.5*rating_reward]
        return scalar_reward

    def get_state(self, start_date, end_date, user_ids):
        """
        Get state features for users for period.
        :param start_date: The start date of the from which data will be queried.
        :param end_date: The end date of the from which data will be queried.
        :param user_ids: A list of ids users to be considered.
        :return: A data frame with states training data for RL.
        """
        all_users_states = pd.DataFrame(columns=self.state_columns)

        for user in user_ids:
            all_users_states = self.get_state_one_user_multiple_period(start_date, end_date, user, all_users_states)

        return all_users_states

    def get_state_one_user_multiple_period(self, start_date, end_date, user_id, user_states_all_dates):
        """
        Get state from user.
        :param start_date: The start date from which data will be queried.
        :param end_date: The end date of the from which data will be queried.
        :param user_id: A list of ids users to be considered.
        :return: A data frame with states training data for RL for user id.
        """
        dates = pd.date_range(start=start_date, end=end_date, closed=None, freq='D') # freq='9H'

        for date in dates:
            user_states_all_dates = self.get_state_one_user_one_period(date, user_id, user_states_all_dates)

        return user_states_all_dates

    def get_state_one_user_one_period(self, date, user_id, user_states_all_day_parts):
        """
        Get state from user.
        :param start_date: The start date from which data will be queried.
        :param end_date: The end date of the from which data will be queried.
        :param user_id: A list of ids users to be considered.
        :return: A data frame with states training data for RL for user id.
        """
        day_parts = ["morning", "afternoon", "evening"]

        for day_part in day_parts:
            user_state = self.get_state_one_user_one_period_day_part(date, user_id, day_part)

            s = pd.Series(user_state, index=self.state_columns)
            user_states_all_day_parts = user_states_all_day_parts.append(s, ignore_index=True)

        return user_states_all_day_parts

    def get_state_one_user_one_period_day_part(self, date, user_id, day_part):
        """
        Get state from user.
        :param start_date: The start date from which data will be queried.
        :param end_date: The end date of the from which data will be queried.
        :param user_id: A list of ids users to be considered.
        :return: A data frame with states training data for RL for user id.
        """
        state = self.get_rating_feature(date, user_id, day_part) + self.get_message_feature(date, user_id, day_part)

        return state

    def get_day_part_value(self, day_part):
        if day_part == "morning":
            return 0
        elif day_part == "afternoon":
            return 1
        elif day_part == "evening":
            return 2

    def get_rating_feature(self, date, user_id, day_part):
        """
        Get rating feature for using on date.
        :param date: The date for which the features are calculated.
        :param user_id: The user id.
        :return: A list with the features.
        """
        current_user_ratings = self.all_ratings[self.all_ratings['userId'] == user_id]["rating"].tolist()[0]

        hours_range = util.get_time_ranges_for_day_part_string(day_part)
        date_mood_value = []

        number_rating = 0
        highest_rating = 0
        lowest_rating = 0
        median_rating = 0
        sd_rating = 0
        number_low_rating = 0
        number_medium_rating = 0
        number_high_rating = 0

        if current_user_ratings:
            for item in current_user_ratings:
                hour = datetime.strptime(item["serverTimestamp"], "%Y-%m-%dT%H:%M:%S.%fZ").hour
                dt = datetime.strptime(item["serverTimestamp"], "%Y-%m-%dT%H:%M:%S.%fZ").date()

                if dt == date and hour <= hours_range[1]:
                    date_mood_value.append(item["value"])

        if len(date_mood_value) > 0:
            number_rating = len(date_mood_value)
            highest_rating = max(date_mood_value)
            lowest_rating = min(date_mood_value)
            median_rating = statistics.median(date_mood_value)

            if len(date_mood_value) > 1:
                sd_rating = statistics.stdev(date_mood_value)
            else:
                sd_rating = 0

            number_low_rating = date_mood_value.count(1) + date_mood_value.count(2)
            number_medium_rating = date_mood_value.count(3) + date_mood_value.count(4) + date_mood_value.count(5)
            number_high_rating = date_mood_value.count(6) + date_mood_value.count(7)

        features = [date,
                    self.get_day_part_value(day_part),
                    user_id,
                    number_rating,
                    highest_rating,
                    lowest_rating,
                    median_rating,
                    sd_rating,
                    number_low_rating,
                    number_medium_rating,
                    number_high_rating]

        return features

    def get_message_feature(self, date, user_id, day_part):
        """
        Get message features for using on date.
        :param date: The date for which the features are calculated.
        :param user_id: The user id.
        :return: A list with the features.
        """
        hours_range = util.get_time_ranges_for_day_part_string(day_part)

        current_user_messages = self.all_messages[self.all_messages['userId'] == user_id]["user.message"].tolist()[0]

        number_message_received = 0
        number_message_read = 0
        read_all_message = 0

        if current_user_messages:
            for item in current_user_messages:
                hour = datetime.strptime(item["serverTimestamp"], "%Y-%m-%dT%H:%M:%S.%fZ").hour
                dt = datetime.strptime(item["serverTimestamp"], "%Y-%m-%dT%H:%M:%S.%fZ").date()
                if dt == date and hour <= hours_range[1]:
                    number_message_received = number_message_received + 1
                    if item['status'] == 'read':
                        number_message_read = number_message_read + 1
            if number_message_received>0:
                if number_message_received == number_message_read:
                    read_all_message = 1

        features = [number_message_received,
                    number_message_read,
                    read_all_message]

        return features

    def generate_actions(self,  start_date, end_date, user_ids):
        dates = pd.date_range(start=start_date, end=end_date, closed=None, freq='D')  # freq='9H'
        day_parts = ["morning", "afternoon", "evening"]
        user_actions_all_day_parts = pd.DataFrame(columns = self.action_columns)

        for user in user_ids:
            for date in dates:
                for day_part in day_parts:
                    action = [date, day_part, user, random.choice([0, 1, 2, 3])]

                    s = pd.Series(action, index=self.action_columns)
                    user_actions_all_day_parts = user_actions_all_day_parts.append(s, ignore_index=True)

        return user_actions_all_day_parts


def valid_date(arg):
    try:
        return time.strftime(DATE_FORMAT, time.strptime(arg, DATE_FORMAT))
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(arg)
        raise argparse.ArgumentTypeError(msg)


def main():
    pre_processing_workflow = PreprocessRLWorkflow()
    states, rewards, actions = pre_processing_workflow.run_workflow(pars.startDate,
                                                          pars.endDate,
                                                          pars.userId,
                                                          pars.writeToS3)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Preprocess data for RL training')
    processing_group = parser.add_mutually_exclusive_group()
    parser.add_argument('-sd',
                        dest='startDate',
                        type=valid_date,
                        required=True,
                        help="The start date of the period  - YYYY-MM-DD"
                        )
    parser.add_argument("-ed",
                        dest='endDate',
                        type=valid_date,
                        required=True,
                        default=datetime.today().strftime(DATE_FORMAT),
                        help="The end date of of the period x- YYYY-MM-DD"
                        )
    parser.add_argument("-ui",
                        dest='userId',
                        type=int,
                        required=False,
                        help="The id of the user. Overrides config file."
                        )
    parser.add_argument("-wr",
                        dest='writeToS3',
                        type=int,
                        required=False,
                        help="Write to S3 bucket."
                        )
    parser.add_argument("-scr",
                        dest='sanityCheckReport',
                        type=int,
                        required=False,
                        help="Perform a sanity check report."
                        )
    pars = parser.parse_args()
    main()
