output "master_node_address" {
  value = aws_emr_cluster.benchmarks.master_public_dns
}

output "load_balancer_ip" {
  value = aws_alb.application_load_balancer.dns_name
}
