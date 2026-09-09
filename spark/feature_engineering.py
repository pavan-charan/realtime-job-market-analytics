"""
Spark MLlib Feature Engineering Pipeline
========================================
Defines feature extraction, string indexing, one-hot encoding,
and vector assembly stages for predicting tech job compensation.

Features processed:
- Categorical: role_category, experience_level, city_normalized, employment_type, primary_skill
- Numerical: is_remote (0/1), skills_count
- Target: annual_salary_usd
"""

from typing import List, Tuple
from pyspark.ml import Pipeline, PipelineStage
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
    StandardScaler
)
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    when,
    coalesce,
    lower,
    trim,
    size,
    element_at,
    lit
)


class JobSalaryFeaturePipeline:
    """
    Constructs and applies Spark ML feature transformation pipelines.
    """

    CATEGORICAL_COLS = [
        "role_category",
        "experience_level",
        "city_normalized",
        "employment_type",
        "primary_skill"
    ]

    NUMERICAL_COLS = [
        "is_remote_double",
        "skills_count_double"
    ]

    @classmethod
    def prepare_dataset(cls, df: DataFrame) -> DataFrame:
        """
        Prepares raw/silver dataset by extracting primary skill, casting numerics,
        and filtering out records with null target salary.
        """
        prepared_df = (
            df.filter(col("annual_salary_usd").isNotNull() & (col("annual_salary_usd") > 0))
            .withColumn("role_category", coalesce(col("role_category"), lit("Other Tech Roles")))
            .withColumn("experience_level", coalesce(col("experience_level"), lit("Mid-Level")))
            .withColumn("city_normalized", coalesce(col("city_normalized"), col("city"), lit("Remote")))
            .withColumn("employment_type", coalesce(lower(trim(col("employment_type"))), lit("full_time")))
            # Extract first skill from array as primary skill representation
            .withColumn(
                "primary_skill",
                when(size(col("skills")) > 0, lower(trim(element_at(col("skills"), 1))))
                .otherwise(lit("general"))
            )
            .withColumn("is_remote_double", when(col("is_remote") == True, 1.0).otherwise(0.0))
            .withColumn("skills_count_double", coalesce(col("skills_count"), lit(0)).cast("double"))
            .withColumn("label", col("annual_salary_usd").cast("double"))
        )
        return prepared_df

    @classmethod
    def build_pipeline_stages(cls) -> List[PipelineStage]:
        """
        Assembles StringIndexers, OneHotEncoders, and VectorAssembler.
        Uses handleInvalid='keep' to ensure inference robustness on unseen categories.
        """
        stages: List[PipelineStage] = []
        indexed_cols = [f"{c}_idx" for c in cls.CATEGORICAL_COLS]
        encoded_cols = [f"{c}_vec" for c in cls.CATEGORICAL_COLS]

        # 1. String Indexers
        indexers = StringIndexer(
            inputCols=cls.CATEGORICAL_COLS,
            outputCols=indexed_cols,
            handleInvalid="keep"
        )
        stages.append(indexers)

        # 2. One-Hot Encoders
        encoders = OneHotEncoder(
            inputCols=indexed_cols,
            outputCols=encoded_cols,
            handleInvalid="keep"
        )
        stages.append(encoders)

        # 3. Vector Assembler combining all features
        assembler_inputs = encoded_cols + cls.NUMERICAL_COLS
        assembler = VectorAssembler(
            inputCols=assembler_inputs,
            outputCol="features",
            handleInvalid="keep"
        )
        stages.append(assembler)

        return stages

    @classmethod
    def get_feature_pipeline(cls) -> Pipeline:
        """Returns the un-fitted feature preparation Pipeline."""
        stages = cls.build_pipeline_stages()
        return Pipeline(stages=stages)
