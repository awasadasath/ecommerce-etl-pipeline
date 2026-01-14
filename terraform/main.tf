provider "google" {
  project = var.project_id 
  region  = var.region
}

resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name 
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true
  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                 = "ecommerce"
  project                    = var.project_id
  location                   = var.region
  delete_contents_on_destroy = true
}