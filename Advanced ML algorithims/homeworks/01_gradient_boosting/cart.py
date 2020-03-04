import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from sklearn.datasets import load_wine
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import KFold, train_test_split, GridSearchCV, RandomizedSearchCV
from sklearn.tree import DecisionTreeClassifier

class CART:
    NON_LEAF_TYPE = 0
    LEAF_TYPE = 1

    def __init__(self, min_leaf_size=1, min_samples_split=2, max_depth=None, max_features=None, eps=1e-6):
        self.tree = dict()
        self.min_leaf_size = min_leaf_size
        self.min_samples_split = min_samples_split
        self.max_depth = max_depth
        self.eps = eps

        if max_features == 'sqrt':
            self.get_feature_ids = self.__get_feature_ids_sqrt
        elif max_features == 'log2':
            self.get_feature_ids = self.__get_feature_ids_log2
        elif max_features == None:
            self.get_feature_ids = self.__get_feature_ids_N
        else:
            print('invalid max_features name')
            raise

    def __get_feature_ids_sqrt(self, n_feature):
        feature_ids = range(n_feature)
        np.random.shuffle(feature_ids)
        return feature_ids[:round(np.sqrt(n_feature))]

    def __get_feature_ids_log2(self, n_feature):
        feature_ids = range(n_feature)
        np.random.shuffle(feature_ids)
        return feature_ids[:round(np.log2(n_feature))]

    def __get_feature_ids_N(self, n_feature):
        return range(n_feature)

    def __sort_samples(self, x, y):
        sorted_idx = x.argsort()
        return x[sorted_idx], y[sorted_idx]

    def __div_samples(self, x, y, feature_id, threshold):
        left_mask = x[:, feature_id] < threshold
        right_mask = ~left_mask
        return x[left_mask], x[right_mask], y[left_mask], y[right_mask]

    def count_vars(self, y):
        Ns = 1 / np.arange(1, y.shape[0])
        y_2 = y ** 2
        sums = np.cumsum(y)[:-1]
        sums_2 = np.cumsum(y_2)[:-1]
        return Ns * sums_2 - (Ns ** 2) * (sums ** 2)

    def __find_threshold(self, x, y):
        sorted_x, sorted_y = self.__sort_samples(x, y)
        vars_left = self.count_vars(sorted_y)
        vars_right = self.count_vars(sorted_y[::-1])[::-1]
        N = y.shape[0]
        N_l = np.arange(1, y.shape[0])
        N_r = N_l[::-1]
        gains = (N_l / N) * vars_left + (N_r/ N) * vars_right
        diff = np.where(np.abs(sorted_x[1:] - sorted_x[:-1]) > self.eps)[0]
        if len(diff) == 0:
            return np.inf, 0
        gains = gains[diff]
        best_gain = np.argmin(gains)
        return gains[best_gain], (sorted_x[diff[best_gain]] + sorted_x[diff[best_gain]+1]) / 2.0

    def __fit_node(self, x, y, node_id, depth):
        if (self.max_depth is not None) and (depth >= self.max_depth) or \
                y.shape[0] < self.min_samples_split:
            self.tree[node_id] = (self.__class__.LEAF_TYPE, y.mean())
            return

        features = self.get_feature_ids(x.shape[1])
        thrs = np.array([self.__find_threshold(x[:, feature], y) for feature in features])
        best_feature = thrs[:, 0].argmin()
        if thrs[best_feature, 0] == np.inf:
            self.tree[node_id] = (self.__class__.LEAF_TYPE, y.mean())
            return
        best_thr = thrs[best_feature, 1]

        l_x, r_x, l_y, r_y = self.__div_samples(x, y, features[best_feature], best_thr)

        if l_x.shape[0] == 0 or r_x.shape[0] == 0:
            self.tree[node_id] = (self.__class__.LEAF_TYPE, y.mean())
            return

        self.tree[node_id] = (self.__class__.NON_LEAF_TYPE, best_feature, best_thr)

        self.__fit_node(l_x, l_y, node_id * 2 + 1, depth + 1)
        self.__fit_node(r_x, r_y, node_id * 2 + 2, depth + 1)

    def fit(self, x, y):
        self.__fit_node(x, y, 0, 0)

    def __predict(self, x, node_id):
        node = self.tree[node_id]
        if node[0] == self.__class__.NON_LEAF_TYPE:
            _, feature_id, threshold = node
            if x[feature_id] < threshold:
                return self.__predict(x, 2 * node_id + 1)
            else:
                return self.__predict(x, 2 * node_id + 2)
        else:
            return node[1]

    def predict(self, X):
        return np.array([self.__predict(x, 0) for x in X])

    def fit_predict(self, x_train, y_train, predicted_x):
        self.fit(x_train, y_train)
        return self.predict(predicted_x)