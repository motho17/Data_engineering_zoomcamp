# TensorFlow Model Deployment Guide: BigQuery ML to TensorFlow Serving

## Table of Contents
1.  [Overview](#overview)
2.  [Prerequisites](#prerequisites)
3.  [Deployment Steps](#deployment-steps)
    *   [Step 1: Export Model from BigQuery ML to GCS](#step-1-export-model-from-bigquery-ml-to-gcs)
    *   [Step 2: Prepare Local Environment and Download Model Artifacts](#step-2-prepare-local-environment-and-download-model-artifacts)
    *   [Step 3: Structure Model for TensorFlow Serving](#step-3-structure-model-for-tensorflow-serving)
    *   [Step 4: Run TensorFlow Serving with Docker](#step-4-run-tensorflow-serving-with-docker)
    *   [Step 5: Test Model Prediction](#step-5-test-model-prediction)
4.  [Troubleshooting Tips](#troubleshooting-tips)

---

## Overview

This guide details the process of deploying a machine learning model, specifically the `tip_model` trained in BigQuery ML, using TensorFlow Serving within a Docker container. This setup allows for real-time inference requests to your model via a REST API.

The deployment process involves:
1.  Exporting the trained `tip_model` from BigQuery ML to a Google Cloud Storage (GCS) bucket.
2.  Downloading the model artifacts from GCS to a local environment (e.g., a Codespace, local machine, or VM).
3.  Structuring the downloaded model files in the specific format expected by TensorFlow Serving.
4.  Running TensorFlow Serving in a Docker container to host the model.
5.  Testing the deployed model with a sample prediction request to confirm functionality.

## Prerequisites

Before you begin, ensure you have the following:
*   A Google Cloud Project with BigQuery ML enabled.
*   A trained `tip_model` in BigQuery ML.
*   `gsutil` configured and authenticated to access your GCS bucket.
*   Docker installed and running on your local environment (Codespace, local machine, VM, etc.).
*   `curl` installed for making API requests.

## Deployment Steps

### Step 1: Export Model from BigQuery ML to GCS

First, your `tip_model` must be exported from BigQuery ML to a Google Cloud Storage (GCS) bucket. This is typically a one-time operation after your model has been trained.

```bash
# Example `bq extract` command:
# Replace `your_project_id`, `your_dataset_id`, `your_model_name`, and `your-gcs-bucket`
# with your actual values.
#
# bq extract --destination_format=TF_SAVED_MODEL \
#   --project_id=your_project_id \
#   your_dataset_id.your_model_name \
#   gs://your-gcs-bucket/ml_models/your_model_name/

### Step 2: Prepare Local Environment and Download Model Artifacts
Next, we will set up a temporary local directory and download the model files from GCS into it.

1.  **Clean and Create Temporary Directory:**
    It's good practice to ensure your temporary directory is clean before downloading.

    ```bash
    # Remove any existing temporary directory to ensure a fresh start
    rm -rf /tmp/model_from_gcs
    # Create the temporary directory where we want the model files to land
    mkdir -p /tmp/model_from_gcs
    ```

2.  **Download Model from GCS:**
    This command copies the *contents* of your GCS model directory directly into your local temporary directory. The `*` at the end of the GCS path is crucial; it tells `gsutil` to copy the files *inside* the `tip_model/` directory, rather than creating an additional `tip_model/` subdirectory locally.

    ```bash
    gsutil cp -r gs://joyce-gitau-taxi-data/ml_models/tip_model/* /tmp/model_from_gcs/
    ```

3.  **Verify Downloaded Files:**
    Confirm that the model files (e.g., `saved_model.pb`, `assets/`, `variables/`) are present directly within `/tmp/model_from_gcs/`.

    ```bash
    echo "Verifying contents of /tmp/model_from_gcs after gsutil cp:"
    ls -R /tmp/model_from_gcs
    ```
    **Expected Output Example:**
    ```
    Verifying contents of /tmp/model_from_gcs after gsutil cp:
    /tmp/model_from_gcs:
    assets
    explanation_metadata.json
    fingerprint.pb
    saved_model.pb
    variables

    /tmp/model_from_gcs/assets:

    /tmp/model_from_gcs/variables:
    variables.data-00000-of-00001
    variables.index
    ```

### Step 3: Structure Model for TensorFlow Serving
TensorFlow Serving expects model files to be organized in a specific directory structure: `<base_path>/<model_name>/<version_number>`. We will use `serving_dir` as our base path, `tip_model` as the model name, and `1` as the initial version number.

1.  **Clean and Create Serving Directory Structure:**
    ```bash
    # Clean up any old serving_dir
    rm -rf serving_dir
    # Create the serving directory structure: serving_dir/tip_model/1/
    mkdir -p serving_dir/tip_model/1
    ```

2.  **Copy Downloaded Model Files to Serving Directory:**
    Move the model artifacts from the temporary download location (`/tmp/model_from_gcs/`) into the TensorFlow Serving-compliant structure (`serving_dir/tip_model/1/`).

    ```bash
    cp -r /tmp/model_from_gcs/* serving_dir/tip_model/1
    ```

3.  **Verify Serving Directory Structure:**
    Confirm that the model files are correctly placed within `serving_dir/tip_model/1`.

    ```bash
    echo "Verifying contents of serving_dir/tip_model/1:"
    ls -R serving_dir/tip_model/1
    ```
    **Expected Output Example:**
    ```
    Verifying contents of serving_dir/tip_model/1:
    /path/to/your/codespace/serving_dir/tip_model/1:
    assets
    explanation_metadata.json
    fingerprint.pb
    saved_model.pb
    variables

    /path/to/your/codespace/serving_dir/tip_model/1/assets:

    /path/to/your/codespace/serving_dir/tip_model/1/variables:
    variables.data-00000-of-00001
    variables.index
    ```

### Step 4: Run TensorFlow Serving with Docker
Now, we'll pull the TensorFlow Serving Docker image and run it, mounting our prepared model directory.

1.  **Pull TensorFlow Serving Docker Image:**
    This command downloads the necessary TensorFlow Serving software into a Docker image.

    ```bash
    docker pull tensorflow/serving
    ```

2.  **Run TensorFlow Serving Container:**
    This command starts the serving container in the background, maps network ports, and instructs TensorFlow Serving where to find your model using a volume mount.

    ```bash
    docker run -d -p 8501:8501 \
      --mount type=bind,source="$(pwd)"/serving_dir/tip_model,target=/models/tip_model \
      -e MODEL_NAME=tip_model \
      tensorflow/serving
    ```
    *   `-d`: Runs the container in detached mode (in the background).
    *   `-p 8501:8501`: Maps port 8501 on your host machine (e.g., Codespace) to port 8501 inside the Docker container. This is the port where TensorFlow Serving listens for requests.
    *   `--mount type=bind,source="$(pwd)"/serving_dir/tip_model,target=/models/tip_model`: This is a critical part. It creates a *bind mount* that links your local `serving_dir/tip_model` directory (which contains your model version `1`) to the `/models/tip_model` path inside the Docker container. TensorFlow Serving is configured to look for models in `/models/<model_name>/<version_number>`.
    *   `-e MODEL_NAME=tip_model`: Sets an environment variable `MODEL_NAME` inside the container, explicitly telling TensorFlow Serving the name of the model it should serve.

3.  **Wait for Container to Start:**
    It takes a few moments for the Docker container to initialize and load the model. Adding a `sleep` command helps prevent "connection refused" errors if you try to make a prediction too quickly.

    ```bash
    echo "TensorFlow Serving container is starting up. Waiting 5 seconds..."
    sleep 5
    echo "Continuing..."
    ```

### Step 5: Test Model Prediction
Finally, send a sample prediction request to your running model server to confirm it's working correctly and returning predictions.

1.  **Send Prediction Request:**
    This `curl` command sends a JSON payload representing a single taxi trip instance to your deployed model's prediction endpoint.

    ```bash
    echo "Sending a test prediction request..."
    curl -d '{"instances": [{"passenger_count":1, "trip_distance":12.2, "PULocationID":"193", "DOLocationID":"264", "payment_type":"2","fare_amount":20.4,"tolls_amount":0.0}]}' \
      -X POST http://localhost:8501/v1/models/tip_model:predict
    ```
    **Expected Output:** A JSON response containing the model's prediction for the given input. The exact format will depend on your model's output signature, but it will typically include a `predictions` array.

2.  **Check Model Status (Optional):**
    You can also query the server to check the status of your deployed model. This is useful for verifying that the model loaded successfully and is `AVAILABLE`.

    ```bash
    echo "Checking model status..."
    curl http://localhost:8501/v1/models/tip_model
    ```
    **Expected Output:** A JSON response indicating the model's state (e.g., `AVAILABLE`) and its version.