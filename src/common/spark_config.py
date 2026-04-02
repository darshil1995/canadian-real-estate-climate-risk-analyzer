from pyspark.sql import SparkSession
import mlflow
from src.common.config import MLFLOW_TRACKING_URI, EXPERIMENT_NAME

import os
import platform


def patch_spark_env():
    """
    Configures JAVA_HOME and HADOOP_HOME based on the OS.
    Prevents hardcoded Windows paths from breaking the Docker (Linux) container.
    """
    if platform.system() == "Windows":
        # --- YOUR LOCAL WINDOWS PATHS ---
        # Update these to match your actual D:\ or C:\ drive paths
        os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
        os.environ["HADOOP_HOME"] = r"C:\hadoop"

        # Add winutils to PATH for Windows Spark
        os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], 'bin')
        print("🔧 [ENV] Windows Spark Patch Applied.")
    else:
        # Inside Docker/Linux, the Dockerfile already sets these.
        print("[ENV] Linux/Docker Environment Detected. Using System JAVA_HOME.")


def get_spark_session(app_name=EXPERIMENT_NAME):
    """
    Initializes a Spark Session optimized for local execution.
    Integrates with MLflow for experiment tracking.
    """

    # Point Spark/MLflow to our centralized tracking server
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    spark = (SparkSession.builder
             .appName(app_name)
             .master("local[*]")
             .config("spark.driver.memory", "4g")
             # Optimization for Parquet (Silver/Gold layers)
             .config("spark.sql.parquet.compression.codec", "snappy")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .getOrCreate())

    # Auto-log Spark ML algorithms (Linear Regression, GBT, etc.)
    try:
        mlflow.pyspark.ml.autolog()
    except Exception as e:
        print(f"MLflow Autologging notice: {e}")

    return spark


if __name__ == "__main__":
    session = get_spark_session()
    print(f"[SUCCESS] Spark {session.version} linked to MLflow at {MLFLOW_TRACKING_URI}")
    session.stop()