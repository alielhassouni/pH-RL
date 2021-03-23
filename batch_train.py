#!/usr/bin/env python3
# Created by Ali el Hassouni

from algorithm.lspi_python.lspi.sample import Sample
from algorithm.lspi_python.lspi.policy import Policy
from algorithm.lspi_python.lspi.basis_functions import OneDimensionalPolynomialBasis, ExactBasis
from algorithm.lspi_python.lspi.solvers import *
from algorithm.lspi_python.lspi.lspi import *
import numpy as np
from preprocess import PreprocessRLWorkflow
import pickle
import dill
from datetime import date
from datetime import timedelta
import pandas as pd
from s3.s3 import S3Client
from io import StringIO
from datetime import datetime

np.set_printoptions(1000)


class LSPI_policy():

    def __init__(self):
        self.LSPI_samples = []
        self.bins = None
        self.n_bins = 4
        self.data_dict = None
        self.n_features = None
        self.policy = None

    def get_day_part_value(self, day_part):
        if day_part == "morning":
            return 0
        elif day_part == "afternoon":
            return 1
        elif day_part == "evening":
            return 2
        elif day_part == 1:
            return 1

    def prepare_rl_data(self):
        bins = np.linspace(0, 4, self.n_bins)

        start_date = "2020-10-06"
        end_date = date.today()-timedelta(days=0)

        pre_processing_workflow = PreprocessRLWorkflow()
        states, rewards, actions = pre_processing_workflow.run_workflow(start_date,
                                                                        end_date)

        actions['serverTimestamp'] = pd.to_datetime(actions['timestamp'], format="%Y-%m-%d %H:%M:%S.%f").dt.date
        states['serverTimestamp'] = pd.to_datetime(states['serverTimestamp'], format="%Y-%m-%d").dt.date
        rewards['serverTimestamp'] = pd.to_datetime(rewards['serverTimestamp'], format="%Y-%m-%d").dt.date
        actions['day_part_numeric'] = actions['day_part'].apply(self.get_day_part_value)

        df1 = pd.merge(states, rewards, how='left', left_on=["serverTimestamp", "day_part", "user_id"],
                       right_on=["serverTimestamp", "day_part", "user_id"])
        merged_df = pd.merge(df1, actions, how='left', left_on=["serverTimestamp", "day_part", "user_id"],
                             right_on=["serverTimestamp", "day_part_numeric", "user_id"])

        self.n_features = len([1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13])

        number_samples = merged_df.shape[0]

        for i in range(number_samples-1):
            state = np.digitize(merged_df.iloc[i, [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]], bins)
            action = merged_df.iloc[i, 17]
            reward = merged_df.iloc[i, 14]
            next_state = np.digitize(merged_df.iloc[i+1, [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]], bins)

            sample = Sample(
                self.get_feature_approximation_state([self.n_bins] * len(state), np.array(state) - 1),
                action,
                np.float(reward),
                self.get_feature_approximation_state([self.n_bins] * len(state), np.array(next_state) - 1),
                absorb=False)

            self.LSPI_samples.append(sample)

    def get_feature_approximation_state(self, n_features, state):
        """
        Return basis function features for LSPI.
        :param n_features: the number of features in the state.
        :param state: The state values.
        :return:
        """
        result = np.zeros(sum(n_features), dtype=int)
        for i in range(0, len(n_features)):
            first_position = int(np.sum(n_features[0:i]))
            result[first_position + int(state[i])] = 1
        return result

    def learn(self):
        """
        Learn policy using all the available samples.
        :return: policy object containing the learned weights.
        """
        basis = ExactBasis(np.array([2]*self.n_bins*self.n_features), 4)
        policy = Policy(basis=basis,
                        discount=0.95,
                        explore=0.1,
                        tie_breaking_strategy=Policy.TieBreakingStrategy.FirstWins)
        solver = LSTDQSolver()
        p = learn(self.LSPI_samples,
                  policy,
                  solver,
                  max_iterations=25,
                  epsilon=0.00001)

        self.policy = p

        return p

    def run(self):
        self.prepare_rl_data()
        return self.learn()


def main():
    LSPI  = LSPI_policy()
    policy = LSPI.run()

    path = '/home/ec2-user/moodbuster/policy.pkl'
    with open(path, 'wb') as output:
        dill.dump(LSPI, output, pickle.HIGHEST_PROTOCOL)

    with open('/home/ec2-user/moodbuster/policy_' + str(datetime.now(tz=None)) + '.pkl', 'wb') as output:
        dill.dump(LSPI, output, pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    main()