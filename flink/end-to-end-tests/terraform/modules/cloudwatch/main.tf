resource "aws_cloudwatch_log_group" "flink_session_cluster" {
  name              = var.cloudwatch_group_name
  retention_in_days = 1
}
