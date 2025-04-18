variable "GOOGLE_CREDENTIALS" {
  description = "Google Cloud Credentials"
  type        = string
}

variable "GCP_PROJECT_NAME" {
  description = "Project Name"
  type        = string
}

variable "GCP_PROJECT_ID" {
  description = "Project ID"
  type        = string
}

variable "GCP_INFRA_REGION" {
  description = "Region"
  type        = string
}

variable "GCP_INFRA_LOCATION" {
  description = "Project Location"
  type        = string
}

variable "GCP_GCS_BUCKET_NAME" {
  description = "Google Storage Bucket Name"
  type        = string
}

variable "GCP_STORAGE_CLASS" {
  description = "Bucket Storage Class"
  type        = string
}

variable "GCP_BIGQUERY_DATASET" {
  description = "Google BigQuery Dataset Name"
  type        = string
}

variable "GCP_DATAPROC_CLUSTER" {
  description = "Google Dataproc Cluster Name"
  type        = string
}