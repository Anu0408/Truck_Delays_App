import configparser
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
import mlflow
from mlflow import log_metric, log_param

class ModelTrainer:
    def __init__(self):
        pass

    def evaluate_model(self, model, X_test, y_test):
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, average="weighted")
        recall = recall_score(y_test, y_pred, average="weighted")
        f1 = f1_score(y_test, y_pred, average="weighted")

        # Log metrics to MLflow
        log_metric("accuracy", accuracy)
        log_metric("precision", precision)
        log_metric("recall", recall)
        log_metric("f1_score", f1)

        return accuracy, precision, recall, f1

    def train_and_evaluate(self, model_name, X_train, X_test, y_train, y_test):
        try:
            mlflow.start_run()  # Start a new MLflow run

            # Define models and parameter grids
            models = {
                "Logistic Regression": (LogisticRegression(solver='liblinear', max_iter=1000), {
                    'penalty': ['l1', 'l2'],
                    'C': [0.1, 1.0, 10],
                    'solver': ['liblinear']
                }),
                "Random Forest": (RandomForestClassifier(random_state=42), {
                    'n_estimators': [100, 200, 500],
                    'max_depth': [5, 10, 15],
                    'min_samples_split': [2, 5, 10]
                }),
                "XGBoost": (XGBClassifier(use_label_encoder=False, eval_metric='logloss'), {
                    'n_estimators': [100, 200, 500],
                    'learning_rate': [0.01, 0.1, 0.2],
                    'max_depth': [3, 5, 7]
                })
            }

            model, param_grid = models[model_name]

            # Perform Grid Search for hyperparameter tuning
            grid_search = GridSearchCV(model, param_grid, cv=5, scoring='accuracy', error_score='raise')
            grid_search.fit(X_train, y_train)

            best_model = grid_search.best_estimator_
            log_param("best_params", grid_search.best_params_)

            # Evaluate the best model
            accuracy, precision, recall, f1 = self.evaluate_model(best_model, X_test, y_test)
            
            # Log model performance metrics
            log_metric("accuracy", accuracy)
            log_metric("precision", precision)
            log_metric("recall", recall)
            log_metric("f1_score", f1)

            # Log the trained model to MLflow
            mlflow.sklearn.log_model(best_model, model_name.lower().replace(" ", "_") + "_model", 
                                      input_example=X_test.iloc[:1])  # Example input for logging

        except Exception as e:
            print(f"Error occurred during model training: {e}")
            mlflow.log_param("error", str(e))
            raise
        finally:
            mlflow.end_run()  # End the MLflow run
