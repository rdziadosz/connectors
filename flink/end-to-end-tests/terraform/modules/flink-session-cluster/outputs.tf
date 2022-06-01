output "jobmanager_hostname" {
  value = aws_alb.public.dns_name
}

output "jobmanager_port" {
  value = local.jobmanager_rest_port
}

output "jobmanager_address" {
  value = "http://${aws_alb.public.dns_name}:${local.jobmanager_rest_port}"
}
