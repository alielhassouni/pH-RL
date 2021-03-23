import requests
from util import logging as log
from configs import therapist_config
from datetime import datetime
import json
import random
from s3.s3 import S3Client
import io
import boto3
from botocore.client import ClientError
from configs import s3
import pandas as pd
from io import StringIO # python3; python2: BytesIO
from datetime import datetime
import pandas as pd
from therapist import Therapist

therapist = Therapist()
therapist.log_in()

class MoodBusterMessagePoster(object):

    logger = log.get_logger("MB message poster")

    def __init__(self, cod_user_receptor, title, body):
        self.cod_user_receptor = cod_user_receptor
        self.title = title
        self.body = body
        self.authentication_response = None
        self.user_id = None
        self.log_in()

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
        print(self.user_id)

        print(self.authentication_response.json()["id"])
        return response

    def post_message(self):
        """
        Post a message using the MoodBuster POST route.
        :param codUserReceptor:
        :param title: The title of the message.
        :param body: The body of the message.
        :return:
        """
        self.logger.info('Sending message with title: {} and body: {} to user {} at time {}.'.format(
            self.title,
            self.body,
            self.cod_user_receptor,
            datetime.now())
        )

        base_url = "https://moodbuster-lite.science/api/monitoring/users/"
        print(self.authentication_response.json()["id"])
        request_url = base_url + str(self.user_id) + \
                      "/message?access_token=" + str(self.authentication_response.json()["id"])
        print(request_url)
        data = {"title": self.title,
                "body": self.body,
                "codUserReceptor": self.cod_user_receptor}

        response = requests.post(request_url, data = data)
        self.authentication_response = response
        return response


class MoodBusterMessageSelector(object):

    logger = log.get_logger("MB message selector")

    def __init__(self, cod_user_receptor, action, mood):
        self.action = action
        self.mood = mood
        self.user_id = cod_user_receptor
        #'/home/ec2-user/moodbuster/messages/body.json'
        try:
            with open('/messages/body.json') as f:
                self.message_body = json.load(f)
        except (FileNotFoundError): # ImportError, ValueError
            print('MESSAGE BODY NOT FOUND')
            exit(1)

    def get_message_body(self, action: None, mood: None):
        """
        Return a message from the list of messages based on the selection action.
        If the message has been sent today already, return a different variation.
        Action: 0 -> encouraging
        Action: 1 -> informing
        Action: 2 -> affirming
        :param action: a numeric value indicating the action.
        :return: A string containing the message.
        """
        if action is not None:
            self.action = action

        if mood is not None:
            self.mood = mood

        messages = None

        if action == 1:
            if mood < 2:
                all_body_variant = self.message_body["encouraging"]["negative_neutral_mood"]
                messages = self.remove_duplicate_message_one_day(self.user_id, all_body_variant)

            elif mood >=2 and mood <=4:
                all_body_variant = self.message_body["encouraging"]["positive_neutral_mood"]
                messages = self.remove_duplicate_message_one_day(self.user_id, all_body_variant)

            elif mood == 9999:
                all_body_variant = self.message_body["encouraging"]["mood_unavailable"]
                messages = self.remove_duplicate_message_one_day(self.user_id, all_body_variant)

        elif action == 2:
            all_body_variant = self.message_body["informing"]["all"]
            messages = self.remove_duplicate_message_one_day(self.user_id, all_body_variant)

        elif action == 3:
            if mood < 2:
                all_body_variant = self.message_body["affirming"]["negative_neutral_mood"]
                messages = self.remove_duplicate_message_one_day(self.user_id, all_body_variant)

            elif mood >= 2 and mood <= 4:
                all_body_variant = self.message_body["affirming"]["positive_neutral_mood"]
                messages = self.remove_duplicate_message_one_day(self.user_id, all_body_variant)

            elif mood == 9999:
                all_body_variant = self.message_body["affirming"]["mood_unavailable"]
                messages = self.remove_duplicate_message_one_day(self.user_id, all_body_variant)

        return random.choice(messages)

    def get_random_message_body(self, action: None, mood: None):
        """
        Return a message from the list of messages based on the selection action.
        If the message has been sent today already, return a different variation.
        Action: 0 -> encouraging
        Action: 1 -> informing
        Action: 2 -> affirming
        :param action: a numeric value indicating the action.
        :return: A string containing the message.
        """
        if action is not None:
            self.action = action

        if mood is not None:
            self.mood = mood

        messages = None

        if action == 1:
            if mood < 4:
                messages = self.message_body["encouraging"]["negative_neutral_mood"]
                # messages = self.remove_duplicate_message_one_day(self.user_id, all_body_variant)

            elif mood >=4 and mood <=7:
                messages = self.message_body["encouraging"]["positive_neutral_mood"]
                # messages = self.remove_duplicate_message_one_day(self.user_id, all_body_variant)

            elif mood == 0:
                messages = self.message_body["encouraging"]["mood_unavailable"]
                # messages = self.remove_duplicate_message_one_day(self.user_id, all_body_variant)

        elif action == 2:
            messages = self.message_body["informing"]["all"]
            # messages = self.remove_duplicate_message_one_day(self.user_id, all_body_variant)

        elif action == 3:
            if mood < 4:
                messages = self.message_body["affirming"]["negative_neutral_mood"]
                # messages = self.remove_duplicate_message_one_day(self.user_id, all_body_variant)

            elif mood >= 4 and mood <= 7:
                messages = self.message_body["affirming"]["positive_neutral_mood"]
                # messages = self.remove_duplicate_message_one_day(self.user_id, all_body_variant)

            elif mood == 0:
                messages = self.message_body["affirming"]["mood_unavailable"]
                # messages = self.remove_duplicate_message_one_day(self.user_id, all_body_variant)

        return random.choice(messages)


    def remove_duplicate_message_one_day(self, user_id, messages):
        """
        Remove messages already send during current day to user.
        :param user_id: user id.
        :return: a list of messages excluding already sent.
        """
        # user_messages_today = self.get_messages_sent_to_user_today_s3(user_id)
        # print(user_messages_today)
        # if user_messages_today in messages: messages.remove(user_messages_today)

        return messages

    def get_messages_sent_to_user_today_s3(self, user_id):
        """
        Return all messages sent to user today. Get data from S3.
        :param user_id: user id.
        :return: A list of strings containing all messages sent to user today.
        """
        a = 2*user_id
        if 3 > 4:
            a = 3
        return [a]

def get_part_of_day(hour):
    return (
        "morning" if 5 <= hour <= 11
        else
        "afternoon" if 12 <= hour <= 17
        else
        "evening" if 18 <= hour <= 22
        else
        "night"
    )


def main():

    client = S3Client("eu-central-1")
    object = client.get_object(bucket_name="aelhassouni-mob-phd-research", object_name="test/actions/actions.csv")
    csv = object.get()['Body'].read().decode('utf-8')
    old_df = pd.read_csv(StringIO(csv))
    del old_df["Unnamed: 0"]

    column_names = ["timestamp", "day_part", "user_id", "action", "message"]
    new_df = pd.DataFrame(columns=column_names)

    users = ["id_s"]
    #users = therapist.get_all_patients()

    for user_id in users:
        message = "No message was sent!"
        action = random.choice([0, 1, 2, 3])
        mood = random.choice([0, 1, 2, 3, 4, 5, 6, 7])

        message_selector = MoodBusterMessageSelector(cod_user_receptor=user_id,
                                                     mood=mood,
                                                     action=action)

        if action == 0:
            pass
        else:
            message = message_selector.get_random_message_body(action=action, mood=mood)
            msp = MoodBusterMessagePoster(cod_user_receptor=user_id,
                                          title="Message from your therapist",
                                          body=message)
            result = msp.post_message().json()
            print(result)

        print(action, mood, message)

        data = pd.DataFrame(
            {"timestamp": datetime.now(tz=None), "day_part": get_part_of_day(datetime.now(tz=None).hour), "user_id": user_id, "action": action,
             "message": message}, index=[0])
        new_df = new_df.append(data)

    print(client.write_object(file=new_df, bucket_name="your_s3_bucket_name",
                              key="test/actions/actions-" + str(datetime.now(tz=None)) + ".csv",
                              prefix="/test/actions"))
    print(new_df)
    new_df = old_df.append(new_df)

    print(client.write_object(file=new_df, bucket_name="your_s3_bucket_name", key="test/actions/actions.csv",
                              prefix="/test/actions"))
    print(new_df)


if __name__ == "__main__":
    main()
