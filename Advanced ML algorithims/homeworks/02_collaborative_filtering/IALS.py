import numpy as np

np.random.seed(0)

class IALS:
    def __init__(self, embedding_size=5, lambda_=5, alpha=40, eps=0.13):
        self.embedding_size = embedding_size
        self.lambda_ = lambda_
        self.alpha = alpha
        self.eps = eps

    def get_init_vectors(self, train, test):
        not_zero_mask = (train != 0)
        mean = train[not_zero_mask].mean()
        train_center = (train - mean) * not_zero_mask

        user_num = not_zero_mask.sum(axis=1).reshape(-1, 1)
        user_num[user_num == 0] = 1
        user_biases = train_center.sum(axis=1).reshape(-1, 1) / user_num

        item_num = not_zero_mask.sum(axis=0).reshape(1, -1)
        item_num[item_num == 0] = 1
        items_biases = ((train_center - user_biases) * not_zero_mask).sum(axis=0).reshape(1, -1) / item_num

        p = train + ~not_zero_mask * ((test == 0) * (user_biases + items_biases + mean) * 0.85 + (test == 1) * (user_biases + items_biases + mean))

        return p, user_biases, items_biases

    def get_confidence(self, train, test, alpha, eps):
        return np.ones(train.shape) + alpha * np.log(np.ones(train.shape) + (train + test) / eps)

    def eval(self, pred, train, valid):
        train_error = np.sqrt(np.sum((pred * (train > 0) - train) ** 2) / (train > 0).sum())
        if valid is not None:
            valid_error = np.sqrt(np.sum((pred * (valid > 0) - valid) ** 2) / (valid > 0).sum())
        else:
            valid_error = None
        return train_error, valid_error

    def fit(self, train, valid, test0, epochs_num):
        if valid is not None:
            test = test0 + (valid > 0)
        else:
            test = test0.copy()
        p, user_biases, item_biases = self.get_init_vectors(train, test)
        c = self.get_confidence(train, test, self.alpha, self.eps)
        c_1 = c - 1
        lambda_I = np.eye(self.embedding_size + 1, self.embedding_size + 1) * self.lambda_

        X = np.hstack([np.ones((train.shape[0], 1)), np.random.randn(train.shape[0], self.embedding_size)])
        Y = np.hstack([np.ones((train.shape[1], 1)), np.random.randn(train.shape[1], self.embedding_size)])

        n_user = (train + test > 0).sum(axis=1)
        n_item = (train + test > 0).sum(axis=0)

        for epoch in range(epochs_num):
            YtY = np.matmul(Y.T, Y)
            pg = p - item_biases
            cp = c * pg
            for i in range(X.shape[0]):
                inv_mat = np.linalg.inv(YtY + lambda_I * (n_user[i]) + np.matmul(Y.T * c_1[i, :], Y))
                t = np.matmul(inv_mat, Y.T)
                X[i, :] = np.matmul(t, cp[i, :].reshape(-1, 1)).reshape(-1)
            user_biases = X[:, 0].copy().reshape(-1, 1)
            X[:, 0] = 1

            XtX = np.matmul(X.T, X)
            pb = p - user_biases
            cp = c * pb
            for j in range(Y.shape[0]):
                inv_mat = np.linalg.inv(XtX + lambda_I * (n_item[j]) + np.matmul(X.T * c_1[:, j], X))
                t = np.matmul(inv_mat, X.T)
                Y[j, :] = np.matmul(t, cp[:, j].reshape(-1, 1)).reshape(-1)
            item_biases = Y[:, 0].copy().reshape(1, -1)
            Y[:, 0] = 1

            result = np.matmul(X[:, 1:], Y[:, 1:].T) + user_biases + item_biases
            result[result > 5] = 5
            result[result < 1] = 1
            train_error, valid_error = self.eval(result, train, valid)
            print(f"Epoch {epoch} --- TRAIN_ER: {train_error} --- VALID_ER: {valid_error}")

        return result