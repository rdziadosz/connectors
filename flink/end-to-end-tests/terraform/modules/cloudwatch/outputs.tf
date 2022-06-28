output "cloudwatch_group_id" {
  value = aws_cloudwatch_log_group.flink_session_cluster.id
}
