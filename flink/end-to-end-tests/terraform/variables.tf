variable "region" {
  description = "The default region to manage resources in."
  type        = string
  default     = "us-west-2"
}

variable "availability_zone1" {
  description = "The default availability zone to manage resources in."
  type        = string
  default     = "us-west-2a"
}

variable "availability_zone2" {
  description = "The default availability zone to manage resources in."
  type        = string
  default     = "us-west-2b"
}

variable "test_data_bucket_name" {
  description = "The name of the AWS S3 bucket that will be used to store test data."
  type        = string
}

variable "tags" {
  description = "Common tags assigned to each resource."
  type        = map(string)
  default     = {}
}
