from sklearn.dummy import DummyRegressor
from sklearn.tree import DecisionTreeRegressor
from tqdm import tqdm
import numpy as np
import time

def mse(pred, true):
    pred = np.array(pred)
    true = np.array(true)
    return np.mean((pred - true) ** 2)

class GradientBoosting:
    def __init__(self, n_estimators=100, learning_rate=0.1, subsamples=1.0, max_features=None, min_split_size=1,
                 max_depth=3, min_leaf_size=1, init=None):
        self.n_estimators = n_estimators
        self.learning_rate = learning_rate
        self.subsamples = subsamples
        self.max_features = max_features
        self.max_depth = max_depth
        self.min_leaf_size = min_leaf_size
        self.min_split_size = min_split_size
        if init is None:
            self.init_model = DummyRegressor()
        else:
            self.init_model = init
        self.estimators = []
        self.weights = []
        self.loss_by_iter = []

    def fit(self, data):
        if self.init_model == 'zeros':
            pred = np.zeros(data.scores.shape)
        else:
            self.init_model.fit(data.feature_matrix, data.scores)
            pred = self.init_model.predict(data.feature_matrix)
        res = pred.copy()
        # self.loss_by_iter.append(mse(res, y))

        for i in tqdm(range(self.n_estimators)):
            st = time.time()
            lambdas = data.get_lambdas(pred)
            print("get_lamdas: " + str(time.time() - st))

            cur_estimator = DecisionTreeRegressor(
                max_depth=self.max_depth, min_samples_leaf=self.min_leaf_size,
                min_samples_split=self.min_split_size
            )
            # if self.subsamples < 1.0:
            #     sample_ids = np.arange(y.shape[0])
            #     np.random.shuffle(sample_ids)
            #     sample_ids = sample_ids[:int(y.shape[0] * self.subsamples)]
            #     cur_estimator.fit(X[sample_ids], grad[sample_ids])
            # else:
            st = time.time()
            cur_estimator.fit(data.feature_matrix, lambdas)
            print("fit_tree: " + str(time.time() - st))
            self.estimators.append(cur_estimator)
            pred = cur_estimator.predict(data.feature_matrix)
            b = 1
            self.weights.append(b)
            res += self.learning_rate * b * pred
            # self.loss_by_iter.append(mse(res, y))

    def predict(self, X):
        pred = self.init_model.predict(X)
        for i in range(self.n_estimators):
            pred += self.learning_rate * self.weights[i] * self.estimators[i].predict(X)
        return pred

