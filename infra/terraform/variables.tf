variable "aws_region" {
  description = "AWS region for the EC2 instance, NLB, and ACM certificate."
  type        = string
  default     = "us-west-2"
}

variable "name_prefix" {
  description = "Prefix applied to AWS resources."
  type        = string
  default     = "envoy-extproc-demo"
}

variable "domains" {
  description = "List of registered domains/subdomains to attach to the ACM cert and Route 53."
  type = list(object({
    fqdn      = string
    zone_name = string
  }))

  validation {
    condition     = length(var.domains) > 0
    error_message = "Provide at least one domain in var.domains."
  }
}

variable "instance_type" {
  description = "EC2 instance type running the Docker Compose stack."
  type        = string
  default     = "t3.medium"
}

variable "ecr_registry" {
  description = "ECR registry host (e.g. 123456789012.dkr.ecr.us-west-2.amazonaws.com)."
  type        = string
}

variable "ecr_repo_prefix" {
  description = "ECR repository name prefix (expects <prefix>-{controller,web,extproc,extproc-rust,extproc-java})."
  type        = string
  default     = "envoy-extproc-sdk-site"
}

variable "image_tag" {
  description = "Docker image tag to pull from ECR."
  type        = string
  default     = "latest"
}

variable "git_repo_url" {
  description = "Git repository URL containing compose.yaml, compose.prod.yaml, and envoy.yaml."
  type        = string
  default     = "https://github.com/wrossmorrow/envoy-extproc-sdk-site.git"
}

variable "git_ref" {
  description = "Git branch or tag to check out on the EC2 instance."
  type        = string
  default     = "main"
}
