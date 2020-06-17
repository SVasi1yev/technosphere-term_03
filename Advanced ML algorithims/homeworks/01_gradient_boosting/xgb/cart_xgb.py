import numpy as np
from _collections import deque

class CART_xgb:
    NON_LEAF_TYPE = 0
    LEAF_TYPE = 1

    def __init__(self, min_leaf_size=1, min_samples_split=2, max_depth=None, max_features=None, eps=1e-6,
                 gamma=0, lamb=1):
        self.tree = dict()
        self.min_leaf_size = min_leaf_size
        self.min_samples_split = min_samples_split
        self.max_depth = max_depth
        self.eps = eps
        self.max_features = max_features
        self.gamma = gamma
        self.lamb = lamb
        self.leafs = []

    def get_feature_ids(self, num_features):
        features_ids = np.arange(self.data_shape[1])
        if not (num_features is None or num_features >= self.data_shape[1]):
            features_ids = features_ids[:num_features]
            np.random.shuffle(features_ids)
        return features_ids

    def __sort_samples(self, x, grad, hess):
        sorted_idx = x.argsort()
        return x[sorted_idx], grad[sorted_idx], hess[sorted_idx]

    def __div_samples(self, x, grad, hess, feature_id, threshold):
        left_mask = x[:, feature_id] < threshold
        right_mask = ~left_mask
        return x[left_mask], x[right_mask], \
               grad[left_mask], grad[right_mask],\
               hess[left_mask], hess[right_mask]

    def count_gains(self, grad, hess):
        G = np.sum(grad)
        Gl = np.cumsum(grad[:-1])
        Gr = G - Gl
        S = np.sum(hess)
        Sl = np.cumsum(hess[:-1])
        Sr = S - Sl
        return (Gl ** 2) / (Sl + self.lamb) + (Gr ** 2) / (Sr + self.lamb) - (G ** 2) / (S + self.lamb) - self.gamma

    def __find_threshold(self, x, grad, hess):
        sorted_x, sorted_grad, sorted_hess = self.__sort_samples(x, grad, hess)
        gains = self.count_gains(sorted_grad, sorted_hess)
        if self.min_leaf_size > 1:
            gains[:self.min_leaf_size-1] = np.array([-np.inf] * (self.min_leaf_size - 1))
            gains[-(self.min_leaf_size-1):] = np.array([-np.inf] * (self.min_leaf_size - 1))
        diff = np.where(np.abs(sorted_x[1:] - sorted_x[:-1]) > self.eps)[0]
        if len(diff) == 0:
            return -np.inf, 0
        gains = gains[diff]
        best_gain = np.argmax(gains)
        return gains[best_gain], (sorted_x[diff[best_gain]] + sorted_x[diff[best_gain]+1]) / 2.0

    def __fit_node(self, x, grad, hess, node_id, depth):
        if (self.max_depth is not None) and (depth >= self.max_depth) or \
                x.shape[0] < self.min_samples_split:
            self.tree[node_id] = (self.__class__.LEAF_TYPE, -np.sum(grad) / (np.sum(hess) + self.lamb))
            self.leafs.append(node_id)
            return

        features = self.get_feature_ids(self.max_features)
        thrs = np.array([self.__find_threshold(x[:, feature], grad, hess) for feature in features])
        best_feature = thrs[:, 0].argmax()
        if thrs[best_feature, 0] == -np.inf:
            self.tree[node_id] = (self.__class__.LEAF_TYPE, -np.sum(grad) / (np.sum(hess) + self.lamb))
            self.leafs.append(node_id)
            return
        best_thr = thrs[best_feature, 1]

        l_x, r_x, l_grad, r_grad, l_hess, r_hess = self.__div_samples(x, grad, hess, features[best_feature], best_thr)

        if l_x.shape[0] == 0 or r_x.shape[0] == 0:
            self.tree[node_id] = (self.__class__.LEAF_TYPE, -np.sum(grad) / (np.sum(hess) + self.lamb))
            self.leafs.append(node_id)
            return

        self.tree[node_id] = (self.__class__.NON_LEAF_TYPE, best_feature, best_thr, thrs[best_feature, 0], -np.sum(grad) / (np.sum(hess) + self.lamb))

        self.__fit_node(l_x, l_grad, l_hess, node_id * 2 + 1, depth + 1)
        self.__fit_node(r_x, r_grad, r_hess, node_id * 2 + 2, depth + 1)

    def pruning(self):
        while len(self.leafs) > 0:
            leaf_id = self.leafs.pop()
            parent_id = int((leaf_id - 1) / 2)
            if self.tree[parent_id][0] == self.__class__.NON_LEAF_TYPE and self.tree[parent_id][3] < 0:
                self.tree[parent_id] = (self.__class__.LEAF_TYPE, self.tree[parent_id][4])
                self.leafs.append(parent_id)

    def fit(self, x, grad, hess):
        x = np.array(x)
        grad = np.array(grad)
        hess = np.array(hess)
        self.data_shape = x.shape
        self.__fit_node(x, grad, hess, 0, 0)
        self.pruning()

    def __predict(self, x, node_id):
        node = self.tree[node_id]
        if node[0] == self.__class__.NON_LEAF_TYPE:
            _, feature_id, threshold, _, _ = node
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