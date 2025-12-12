provider "google" {
  project = "gcp-airflow-project-480711"
  region  = "us-central1"
}

resource "google_storage_bucket" "data_lake" {
  name          = "gcp-airflow-project-480711-datalake"
  location      = "US"
  storage_class = "STANDARD"
  force_destroy = true
  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                 = "ecommerce"
  location                   = "US"
  delete_contents_on_destroy = true
}