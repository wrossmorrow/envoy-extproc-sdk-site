output "urls" {
  description = "HTTPS URLs for all configured domains."
  value       = [for d in var.domains : "https://${d.fqdn}/"]
}

output "nlb_dns_name" {
  description = "DNS name of the Network Load Balancer."
  value       = aws_lb.this.dns_name
}

output "instance_id" {
  description = "EC2 instance ID (for SSM Session Manager)."
  value       = aws_instance.this.id
}

output "ssm_connect_command" {
  description = "Command to open a shell on the EC2 instance via SSM."
  value       = "aws ssm start-session --region ${var.aws_region} --target ${aws_instance.this.id}"
}
