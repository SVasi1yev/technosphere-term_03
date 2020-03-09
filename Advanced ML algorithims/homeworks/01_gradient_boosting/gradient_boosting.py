from cart import CART
from sklearn.dummy import DummyRegressor
from sklearn.tree import DecisionTreeRegressor
from tqdm import tqdm
import numpy as np

def mse(pred, true):
    pred = np.array(pred)
    true = np.array(true)
    return np.mean((pred - true) ** 2)

def golden_section(res, pred, y, a=0, b=1000, eps=0.0001):
    F = 1.618
    while (b - a) > eps:
        x_1 = b - (b - a) / F
        x_2 = a + (b - a) / F
        y_1 = mse(res + pred * x_1, y)
        y_2 = mse(res + pred * x_2, y)
        if y_1 >= y_2:
            a = x_1
        else:
            b = x_2
    res = (a + b) / 2
    return res

class GradientBoosting:
    def __init__(self, n_estimators=100, learning_rate=0.1, subsamples=1.0, max_features=None,
                 max_depth=3, min_leaf_size=1, init=None):
        self.n_estimators = n_estimators
        self.learning_rate = learning_rate
        self.subsamples = subsamples
        self.max_features = max_features
        self.max_depth = max_depth
        self.min_leaf_size = min_leaf_size
        if init is None:
            self.init_model = DummyRegressor()
        else:
            self.init_model = init
        self.estimators = []
        self.weights = []
        self.loss_by_iter = []

    def fit(self, X, y):
        if self.init_model == 'zeros':
            pred = np.zeros(y.shape)
        else:
            self.init_model.fit(X, y)
            pred = self.init_model.predict(X)
        res = pred.copy()
        self.loss_by_iter.append(mse(res, y))

        for i in tqdm(range(self.n_estimators)):
            grad = (y - res)

            cur_estimator = CART(max_depth=self.max_depth, min_leaf_size=self.min_leaf_size, max_features=self.max_features)
            if self.subsamples < 1.0:
                sample_ids = np.arange(y.shape[0])
                np.random.shuffle(sample_ids)
                sample_ids = sample_ids[:int(y.shape[0] * self.subsamples)]
                cur_estimator.fit(X[sample_ids], grad[sample_ids])
            else:
                cur_estimator.fit(X, grad)
            self.estimators.append(cur_estimator)
            pred = cur_estimator.predict(X)
            # b = golden_section(res, pred, y)\
            b = 1
            self.weights.append(b)
            res += self.learning_rate * b * pred
            self.loss_by_iter.append(mse(res, y))

    def predict(self, X):
        pred = self.init_model.predict(X)
        for i in range(self.n_estimators):
            pred += self.learning_rate * self.weights[i] * self.estimators[i].predict(X)
        return pred

