import numpy as np


def get_part_of_day(hour):
    return (
        "morning" if 0 <= hour <= 11
        else
        "afternoon" if 12 <= hour <= 17
        else
        "evening" if 18 <= hour <= 22
        else
        "night"
    )


def get_time_ranges_for_day_part_string(day_part):
    return (
        [0, 12] if day_part == "morning"
        else
        [13, 18] if day_part == "afternoon"
        else
        [19, 23] #if day_part == "evening"
    )


def get_day_part_value(day_part):
    if day_part == "morning":
        return 0
    elif day_part == "afternoon":
        return 1
    elif day_part == "evening":
        return 2


def get_feature_approximation_state(n_features, state):
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