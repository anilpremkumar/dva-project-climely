import numpy as np
import pandas as pd
import pickle
import time
import gc
from sklearn.model_selection import cross_val_score, GridSearchCV, cross_validate, train_test_split
from sklearn.metrics import accuracy_score, classification_report, explained_variance_score, r2_score
from sklearn.svm import SVR
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.neural_network import MLPClassifier, MLPRegressor
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.preprocessing import StandardScaler, normalize
from sklearn.decomposition import PCA
from sklearn.model_selection import RandomizedSearchCV


class EdaAnalysis:

    def read_data(self):
        counties = ['Los Angeles', 'Miami-Dade', 'Berkshire', 'Harris', 'Coconino', 'Maricopa', 'Aiken',
                    'Butler', 'Duval', 'Edgefield', 'Edmonson', 'Platte', 'Chester', 'Bullitt', 'Berks',
                    'Yuma', 'Tarrant', 'Hanover', 'Harford', 'Davidson', 'Westmoreland', 'Hinds', 'Hardin']
        full_data = np.array(pd.read_csv('./X_Y_count_weka.csv'))
        data = None
        for county in counties:
            county_data = full_data[full_data[:, 0] == county.upper()]
            if data is not None:
                data = np.append(data, county_data, axis=0)
            else:
                data = county_data
        self.x_data = data[:, 3:-1]
        self.y_data = data[:, -1]
        # scaler = StandardScaler()
        # scaler.fit(self.x_data)
        # self.x_data = scaler.transform(self.x_data)
        self.x_train, self.x_test, self.y_train, self.y_test = train_test_split(self.x_data, self.y_data, test_size=0.3,
                                                                                random_state=614,
                                                                                shuffle=True)

    def read_simulated_values(self):

        self.data_sim = np.array(pd.read_csv('./simulated_X.csv'))

        self.x_data_sim = self.data_sim[:, 3:]

        # scaler = StandardScaler()
        # scaler.fit(self.x_data)
        # self.x_data = scaler.transform(self.x_data)

    def pca(self):
        pca = PCA(n_components=8, svd_solver='full')
        pca.fit(self.x_data)
        self.x_data = pca.transform(self.x_data)
        print(pca.explained_variance_ratio_)

    def linear_reg_train(self):
        self.linear_reg = LinearRegression()
        self.linear_reg.fit(self.x_train, self.y_train)
        y_train_pred = self.linear_reg.predict(self.x_train)
        print(r2_score(self.y_train, y_train_pred))
        print(explained_variance_score(self.y_train, y_train_pred))

    def linear_reg_test(self):
        y_pred = self.linear_reg.predict(self.x_test)
        print(explained_variance_score(self.y_test, y_pred))

    def neural_networks_grid_search(self):
        parameters = {'solver': ['lbfgs'], 'max_iter': [1000, 1500, 2000], 'alpha': 10.0 ** -np.arange(1, 7),
                      'hidden_layer_sizes': [(5, 5), (10, 10), (20, 20), (25, 25)], 'random_state': [0, 1, 2, 3]}
        clf_grid = GridSearchCV(MLPRegressor(), parameters, n_jobs=-1)
        clf_grid.fit(self.x_train, self.y_train)
        print(clf_grid.best_params_)

    def neural_networks_train(self):
        # {'alpha': 0.1, 'hidden_layer_sizes': (5, 5), 'max_iter': 1000, 'random_state': 1, 'solver': 'lbfgs'}
        mlp = MLPRegressor(solver='lbfgs',
                           alpha=0.01,
                           hidden_layer_sizes=(5, 5),
                           random_state=1,
                           max_iter=5000)
        mlp.fit(self.x_train, self.y_train)
        y_train_pred = mlp.predict(self.x_train)
        rs_train = self.cal_r_square(self.y_train, y_train_pred)
        print("MLPRegression train: {}".format(r2_score(self.y_train, y_train_pred)))
        y_pred = mlp.predict(self.x_test)
        print("MLPRegression test: {}".format(r2_score(self.y_test, y_pred)))
        # print(rs_train)

    def svc_train(self):
        svr = SVR(kernel='rbf', epsilon=1.0)
        svr.fit(self.x_train, self.y_train)
        y_train_pred = svr.predict(self.x_train)
        rs_train = r2_score(self.y_train, y_train_pred)
        print(rs_train)

    def cal_r_square(self, real, pred):
        real_mean = np.mean(real)
        res = np.sum(np.square(real - pred))
        tot = np.sum(np.square(real - real_mean))

        return 1.0 - (res / tot) if tot != 0 else 0.0

    def random_forest_search(self):
        # Number of trees in random forest
        n_estimators = [int(x) for x in np.linspace(start=200, stop=2000, num=10)]
        # Number of features to consider at every split
        max_features = ['auto', 'sqrt']
        # Maximum number of levels in tree
        max_depth = [int(x) for x in np.linspace(10, 110, num=11)]
        max_depth.append(None)
        # Minimum number of samples required to split a node
        min_samples_split = [2, 3, 4, 5, 10]
        # Minimum number of samples required at each leaf node
        min_samples_leaf = [1, 2, 3, 4]
        # Method of selecting samples for training each tree
        bootstrap = [True, False]
        # Create the random grid
        random_grid = {'n_estimators': n_estimators,
                       'max_features': max_features,
                       'max_depth': max_depth,
                       'min_samples_split': min_samples_split,
                       'min_samples_leaf': min_samples_leaf,
                       'bootstrap': bootstrap}
        # Use the random grid to search for best hyperparameters
        # First create the base model to tune
        rf = RandomForestRegressor()
        # Random search of parameters, using 3 fold cross validation,
        # search across 100 different combinations, and use all available cores
        rf_random = RandomizedSearchCV(estimator=rf, param_distributions=random_grid, n_iter=100, verbose=2,
                                       random_state=42, n_jobs=-1)
        # Fit the random search model
        rf_random.fit(self.x_data, self.y_data)
        print(rf_random.best_params_)

    def random_forest_regressor(self):
        # params = {'n_estimators': 800, 'min_samples_split': 10, 'min_samples_leaf': 4, 'max_features': 'sqrt', 'max_depth': 50,
        #  'bootstrap': True}
        # params = {'n_estimators': 800, 'min_samples_split': 2, 'min_samples_leaf': 4, 'max_features': 'sqrt',
        #           'max_depth': 20, 'bootstrap': False}
        # # params = {'n_estimators': 400, 'min_samples_split': 2, 'min_samples_leaf': 4, 'max_features': 'sqrt',
        # #           'max_depth': 10, 'bootstrap': True}
        params = {'n_estimators': 800, 'min_samples_split': 10, 'min_samples_leaf': 2, 'max_features': 'sqrt',
                  'max_depth': 20, 'bootstrap': False}
        # #test
        # # params = {'n_estimators': 600, 'min_samples_split': 2, 'min_samples_leaf': 2, 'max_features': 'sqrt', 'max_depth': 110,
        # #  'bootstrap': False}
        # # params = {'n_estimators': 1309, 'min_samples_split': 2, 'min_samples_leaf': 2, 'max_features': 'sqrt', 'max_depth': None,
        # #  'bootstrap': False}
        # params = {'n_estimators': 400, 'min_samples_split': 5, 'min_samples_leaf': 4, 'max_features': 'sqrt',
        #           'max_depth': 50, 'bootstrap': True}
        estimator = RandomForestRegressor(**params)
        for k, v in params.items():
            setattr(estimator, k, v)

        estimator.fit(self.x_data, self.y_data)
        # y_train_pred = estimator.predict(self.x_train)
        # rs_train = r2_score(self.y_train, y_train_pred)
        # print("RandomForestRegressor train: {}".format(rs_train))
        # y_pred = estimator.predict(self.x_test)
        # print("RandomForestRegressor test: {}".format(r2_score(self.y_test, y_pred)))

        y_pred = np.round(estimator.predict(self.x_data_sim))
        # x_sim=self.data_sim[:,:]
        final_XY = np.column_stack((self.data_sim, y_pred))

        np.savetxt("Simulated_X_Y.csv", final_XY, delimiter=",", fmt='%s')
        print(final_XY)
        dbfile = open('model.pkl', 'ab')

        # source, destination
        pickle.dump(estimator, dbfile)
        dbfile.close()

        # RandomForestRegressor train: 0.550706791698345
        # RandomForestRegressor test: 0.19600475004039053


if __name__ == "__main__":
    edaAnalysis = EdaAnalysis()
    edaAnalysis.read_data()
    edaAnalysis.read_simulated_values()
    # edaAnalysis.pca()
    # edaAnalysis.linear_reg_train()
    # edaAnalysis.linear_reg_test()
    # #edaAnalysis.neural_networks_grid_search()
    # edaAnalysis.neural_networks_train()
    # edaAnalysis.random_forest_search()
    edaAnalysis.random_forest_regressor()
# edaAnalysis.svc_train()
