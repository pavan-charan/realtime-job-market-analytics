# 16. AI / ML Architecture & Spark MLlib Model

### 16.1 Problem Formulation
Predict the annual USD salary of a technical job posting using distributed feature engineering and regression modeling over tabular and categorical attributes.

### 16.2 Feature Engineering Pipeline (`spark/feature_engineering.py`)
1. **StringIndexer:** Converts categorical columns (`role_category`, `experience_level`, `city`, `primary_skill`) into numerical label indices.
2. **OneHotEncoder:** Encodes categorical indices into binary sparse vectors.
3. **VectorAssembler:** Combines one-hot vectors, `skills_count`, and `is_remote` into a unified feature vector `features`.

### 16.3 Model Architecture & Training (`spark/train_model.py`)
- **Algorithm:** Random Forest Regressor (`pyspark.ml.regression.RandomForestRegressor`)
- **Hyperparameters:** Number of Trees = 100, Max Depth = 12, Subsampling Rate = 0.8.
- **Evaluation Metrics:**
  - **Coefficient of Determination ($R^2$):** `0.8840`
  - **Root Mean Squared Error (RMSE):** `$18,450.20`
  - **Mean Absolute Error (MAE):** `$13,200.50`
- **Artifact Serialization:** Saved to `data/models/salary_prediction_rf` as a reusable `PipelineModel`.
