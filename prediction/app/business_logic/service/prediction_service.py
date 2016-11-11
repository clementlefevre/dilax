from sklearn.cross_validation import train_test_split
from sklearn.grid_search import GridSearchCV


def cv_optimize(clf, parameters, X, y, n_jobs=1, n_folds=5, score_func=None):
    if score_func:
        gs = GridSearchCV(clf, param_grid=parameters,
                          cv=n_folds, n_jobs=n_jobs, scoring=score_func)
    else:
        gs = GridSearchCV(clf, param_grid=parameters,
                          n_jobs=n_jobs, cv=n_folds)
    gs.fit(X, y)
    print "BEST", gs.best_params_, gs.best_score_, gs.grid_scores_
    best = gs.best_estimator_
    return best


def do_classify(clf, parameters, X, y,
                mask=None, score_func=None, n_folds=5, n_jobs=1):
    # remove index and idbldsite

    Xtrain, Xtest, ytrain, ytest = train_test_split(X, y,
                                                    test_size=0.3)
    clf = cv_optimize(clf, parameters, Xtrain, ytrain,
                      n_jobs=n_jobs, n_folds=n_folds,
                      score_func=score_func)
    clf = clf.fit(Xtrain, ytrain)
    training_accuracy = clf.score(Xtrain, ytrain)
    test_accuracy = clf.score(Xtest, ytest)

    print "############# based on standard predict ################"
    print "Accuracy on training data: %0.2f" % (training_accuracy)
    print "Accuracy on test data:     %0.2f" % (test_accuracy)
    print "########################################################"
    print "Features importance : ", clf.feature_importances_
    try:
        print pd.DataFrame(zip(clf.feature_importances_, fields))
    except Exception as e:
        print "no feature_importance"
    print "########################################################"
    return clf, Xtrain, ytrain, Xtest, ytest
