resource "aws_s3_bucket" "test_data" {
  bucket = var.test_data_bucket_name
}
