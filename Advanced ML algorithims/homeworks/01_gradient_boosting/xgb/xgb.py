from cart_xgb import CART_xgb
from sklearn.dummy import DummyClassifier
from sklearn.tree import DecisionTreeRegressor
from tqdm import tqdm
import numpy as np
from sklearn.metrics import log_loss

# def log_loss(true, pred):
#     pred = np.array(pred)
#     true = np.array(true)
#     return -np.mean(true * np.log(pred) + (1 - true) * np.log(1 - pred))

def log_loss_grad(true, pred):
    pred = np.array(pred)
    true = np.array(true)
    return -(true - pred) / (pred * (1 - pred))

def log_loss_hess(true, pred):
    pred = np.array(pred)
    true = np.array(true)
    return -(true * (2 * pred - 1) - pred ** 2) / ((pred - 1) ** 2 * pred ** 2)

class XGradientBoosting:
    def __init__(self, n_estimators=100, learning_rate=0.1, subsamples=1.0, max_features=None,
                 max_depth=3, min_leaf_size=1, init=None, gamma=0, lamb=1):
        self.n_estimators = n_estimators
        self.learning_rate = learning_rate
        self.subsamples = subsamples
        self.max_features = max_features
        self.max_depth = max_depth
        self.min_leaf_size = min_leaf_size
        self.gamma = gamma
        self.lamb = lamb
        if init is None:
            self.init_model = DummyClassifier()
        else:
            self.init_model = init
        self.estimators = []
        self.loss_by_iter = []

    def fit(self, X, y):
        if self.init_model == 'zeros':
            pred = np.zeros(y.shape)
        else:
            self.init_model.fit(X, y)
            pred = self.init_model.predict(X).astype(float)
        res = pred.copy()
        self.loss_by_iter.append(log_loss(y, res))

        for i in tqdm(range(self.n_estimators)):
            grad = log_loss_grad(y, res)
            hess = log_loss_hess(y, res)
            cur_estimator = CART_xgb(max_depth=self.max_depth, min_leaf_size=self.min_leaf_size,
                                     max_features=self.max_features, gamma=self.gamma, lamb=self.lamb)
            if self.subsamples < 1.0:
                sample_ids = np.arange(y.shape[0])
                np.random.shuffle(sample_ids)
                sample_ids = sample_ids[:int(y.shape[0] * self.subsamples)]
                cur_estimator.fit(X[sample_ids], grad[sample_ids], hess[sample_ids])
            else:
                cur_estimator.fit(X, grad, hess)
            self.estimators.append(cur_estimator)
            pred = cur_estimator.predict(X)
            res += self.learning_rate * pred
            self.loss_by_iter.append(log_loss(y, res))

    def predict(self, X):
        pred = self.init_model.predict(X).astype(float)
        for i in range(self.n_estimators):
            pred += self.learning_rate * self.estimators[i].predict(X)
        return pred

