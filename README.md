# mito_end_to_end_forecasting

End to End Pipeline for Apple Tree Mito site.
The repository comprises of:
1. Fetching temperature data from open-meteo.com.
2. Fetching training data comprising of NetLoad from influx db.
3. Training an XGBoost Model for load forecasting.
4. Proper versioning of data used for training.
5. Preparing dvc pipelines for deployment of model on edge.
6. Inclusion of DAG schedulers using Airflow for proper scheduling and performance monitoring.
7. Deployment of forecasting container on remote edge device via SSH.
8. Testing of deployment via CI/CD pipeline on remote edge device using Github Actions and Portainer.
9. Re-training logic based on availability of new data, model performance driftings or scheduler intervals.

