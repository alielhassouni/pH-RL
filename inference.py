import numpy as np
import dill


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


with open('LSPI.pkl', 'rb') as input:
    LSPI = dill.load(input)

state = 12 * [1]
s = LSPI.get_feature_approximation_state([LSPI.n_bins] * len(state), np.array(state) - 1)
state2 = 12 * [0]
s2 = LSPI.get_feature_approximation_state([LSPI.n_bins] * len(state2), np.array(state2) - 1)
state3 = 12 * [2]
s3 = LSPI.get_feature_approximation_state([LSPI.n_bins] * len(state3), np.array(state3) - 1)

print(LSPI.policy.select_action(s))
print(LSPI.policy.select_action(s2))
print(LSPI.policy.select_action(s3))

