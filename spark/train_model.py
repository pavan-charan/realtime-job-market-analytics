"""
Spark MLlib Model Training & Evaluation
=======================================
Trains a Random Forest Regression model to predict tech job compensation (annual_salary_usd).
Evaluates performance with R2, RMSE, and MAE metrics, extracts feature importances,
and persists the trained pipeline model to HDFS.
"""

import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Dict
import yaml

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import split, col
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor, RandomForestRegressionModel
from pyspark.ml.evaluation import RegressionEvaluator

from spark.feature_engineering import JobSalaryFeaturePipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SparkMLTrain")


class SalaryModelTrainer:
    """
    Trains, evaluates, and persists the Random Forest Salary Prediction Model.
    """

    def __init__(self, config_path: str = "config/config.yaml", is_local: bool = True):
        self.config = self._load_config(config_path)
        self.is_local = is_local

        # Storage & Paths
        hdfs_cfg = self.config.get("hdfs", {})
        namenode = hdfs_cfg.get("namenode_url", "hdfs://localhost:9000")
        ml_cfg = self.config.get("ml", {})

        if is_local and os.getenv("USE_LOCAL_FS", "false").lower() == "true":
            self.silver_path = "data/silver"
            self.model_save_path = "data/models/salary_prediction_rf"
        else:
            self.silver_path = f"{namenode}{hdfs_cfg.get('silver_path', '/data/job_market/silver')}"
            self.model_save_path = f"{namenode}{ml_cfg.get('model_save_path', '/data/job_market/models/salary_prediction_rf')}"

        # ML Hyperparameters
        self.train_split = ml_cfg.get("train_split_ratio", 0.8)
        self.random_seed = ml_cfg.get("random_seed", 42)
        self.num_trees = ml_cfg.get("num_trees", 50)
        self.max_depth = ml_cfg.get("max_depth", 10)

        # Spark Settings
        spark_cfg = self.config.get("spark", {})
        self.master = spark_cfg.get("local_master", "local[*]") if is_local else spark_cfg.get("master", "spark://localhost:7077")
        self.spark = self._init_spark_session()

    @staticmethod
    def _load_config(path: str) -> Dict[str, Any]:
        """Loads YAML configuration."""
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        return {}

    def _init_spark_session(self) -> SparkSession:
        """Initializes SparkSession for MLlib workloads."""
        logger.info("Initializing Spark ML Session (Master: %s)...", self.master)
        spark = (
            SparkSession.builder
            .appName("JobSalaryModelTraining")
            .master(self.master)
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark

    def load_data(self) -> DataFrame:
        """Loads Silver layer data or falls back to CSV."""
        try:
            logger.info("Loading Silver dataset from: %s", self.silver_path)
            df = self.spark.read.parquet(self.silver_path)
            if df.count() == 0:
                raise ValueError("Silver dataset is empty.")
            return df
        except Exception as e:
            logger.warning("Could not read Silver Parquet (%s). Falling back to CSV dataset...", e)
            from spark.etl import JobMarketETL
            etl = JobMarketETL(is_local=self.is_local)
            silver_df = etl.run_bronze_to_silver()
            return silver_df

    def train_and_evaluate(self):
        """
        Executes end-to-end training and evaluation:
        1. Feature preparation
        2. Train/Test Split
        3. Random Forest Regression fitting
        4. Performance Metrics (R2, RMSE, MAE)
        5. Model Persistence
        """
        logger.info("==================================================")
        logger.info("Starting Spark MLlib Salary Model Training")
        logger.info("Hyperparameters: Trees=%d | MaxDepth=%d | Split=%.2f", self.num_trees, self.max_depth, self.train_split)
        logger.info("==================================================")

        start_time = time.time()

        # Step 1: Load and Prepare Features
        raw_df = self.load_data()
        prepared_df = JobSalaryFeaturePipeline.prepare_dataset(raw_df)
        total_records = prepared_df.count()
        logger.info("Total records with valid salary labels: %d", total_records)

        # Step 2: Train / Test Split (80% Train, 20% Test)
        train_df, test_df = prepared_df.randomSplit(
            [self.train_split, 1.0 - self.train_split],
            seed=self.random_seed
        )
        train_count = train_df.count()
        test_count = test_df.count()
        logger.info("Dataset Split: %d Train records (%.1f%%) | %d Test records (%.1f%%)",
                    train_count, (train_count / total_records) * 100,
                    test_count, (test_count / total_records) * 100)

        # Step 3: Build Pipeline Stages with Random Forest Regressor
        feature_stages = JobSalaryFeaturePipeline.build_pipeline_stages()
        
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="label",
            predictionCol="prediction",
            numTrees=self.num_trees,
            maxDepth=self.max_depth,
            seed=self.random_seed
        )

        full_pipeline = Pipeline(stages=feature_stages + [rf])

        # Step 4: Fit Model on Train Set
        logger.info("Fitting Random Forest Regression Pipeline...")
        pipeline_model = full_pipeline.fit(train_df)
        training_time = time.time() - start_time
        logger.info("Model fitting completed in %.2f seconds.", training_time)

        # Step 5: Evaluate on Test Set
        logger.info("Evaluating model predictions on test split...")
        predictions_df = pipeline_model.transform(test_df)

        evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
        evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")
        evaluator_mae = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae")

        rmse = evaluator_rmse.evaluate(predictions_df)
        r2 = evaluator_r2.evaluate(predictions_df)
        mae = evaluator_mae.evaluate(predictions_df)

        logger.info("==================================================")
        logger.info("Model Evaluation Metrics on Test Set:")
        logger.info("  • R² (Coefficient of Determination) : %.4f", r2)
        logger.info("  • RMSE (Root Mean Squared Error)    : $%.2f USD", rmse)
        logger.info("  • MAE  (Mean Absolute Error)        : $%.2f USD", mae)
        logger.info("==================================================")

        # Sample Predictions Preview
        logger.info("Sample Predictions Preview:")
        predictions_df.select(
            "role_category",
            "experience_level",
            "city_normalized",
            "is_remote",
            "label",
            "prediction"
        ).show(5, truncate=False)

        # Step 6: Persist Model to HDFS / Storage
        logger.info("Saving fitted pipeline model to: %s ...", self.model_save_path)
        pipeline_model.write().overwrite().save(self.model_save_path)
        logger.info("Model successfully saved to %s", self.model_save_path)

        # Save Metrics Report
        metrics_summary = {
            "model_type": "RandomForestRegressor",
            "num_trees": self.num_trees,
            "max_depth": self.max_depth,
            "train_records": train_count,
            "test_records": test_count,
            "r2_score": round(r2, 4),
            "rmse_usd": round(rmse, 2),
            "mae_usd": round(mae, 2),
            "training_duration_sec": round(training_time, 2),
            "model_path": self.model_save_path
        }
        os.makedirs("docs", exist_ok=True)
        with open("docs/model_metrics.json", "w") as f:
            json.dump(metrics_summary, f, indent=2)
        logger.info("Saved metrics summary to docs/model_metrics.json")

        return pipeline_model, metrics_summary


def main():
    parser = argparse.ArgumentParser(description="Train Spark MLlib Random Forest Salary Model.")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config.yaml")
    parser.add_argument("--local", action="store_true", default=True, help="Run locally")
    parser.add_argument("--trees", type=int, default=50, help="Number of trees")
    parser.add_argument("--depth", type=int, default=10, help="Max depth")

    args = parser.parse_args()

    trainer = SalaryModelTrainer(config_path=args.config, is_local=args.local)
    trainer.num_trees = args.trees
    trainer.max_depth = args.depth
    trainer.train_and_evaluate()


if __name__ == "__main__":
    main()
