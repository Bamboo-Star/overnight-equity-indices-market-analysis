terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.18.1"
    }
  }
}


provider "google" {
  credentials = file(var.GOOGLE_CREDENTIALS)
  project     = var.GCP_PROJECT_ID
  region      = var.GCP_INFRA_REGION
}

resource "google_storage_bucket" "storage-bucket" {
  name          = var.GCP_GCS_BUCKET_NAME
  location      = var.GCP_INFRA_LOCATION
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id                 = var.GCP_BIGQUERY_DATASET
  location                   = var.GCP_INFRA_LOCATION
  delete_contents_on_destroy = true
}