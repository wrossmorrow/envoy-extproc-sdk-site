locals {
  domain_map   = { for d in var.domains : d.fqdn => d }
  zone_names   = toset([for d in var.domains : d.zone_name])
  primary_fqdn = var.domains[0].fqdn
  san_fqdns    = slice([for d in var.domains : d.fqdn], 1, length(var.domains))
}

# ------------------------------------------------------------------------------
# Networking (default VPC + subnets)
# ------------------------------------------------------------------------------

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "nlb" {
  name        = "${var.name_prefix}-nlb"
  description = "Allow public HTTPS to the NLB"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "TLS from anywhere"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.name_prefix}-nlb"
  }
}

resource "aws_security_group" "ec2" {
  name        = "${var.name_prefix}-ec2"
  description = "Allow Envoy traffic from the NLB only (SSM used for shell access)"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description     = "Envoy HTTP listener from NLB"
    from_port       = 8000
    to_port         = 8000
    protocol        = "tcp"
    security_groups = [aws_security_group.nlb.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.name_prefix}-ec2"
  }
}

# ------------------------------------------------------------------------------
# Route 53 + ACM (single multi-SAN certificate for all configured domains)
# ------------------------------------------------------------------------------

data "aws_route53_zone" "domains" {
  for_each     = local.zone_names
  name         = each.value
  private_zone = false
}

resource "aws_acm_certificate" "this" {
  domain_name               = local.primary_fqdn
  subject_alternative_names = local.san_fqdns
  validation_method         = "DNS"

  lifecycle {
    create_before_destroy = true
  }

  tags = {
    Name = var.name_prefix
  }
}

resource "aws_route53_record" "cert_validation" {
  for_each = {
    for dvo in aws_acm_certificate.this.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
      zone   = data.aws_route53_zone.domains[local.domain_map[dvo.domain_name].zone_name].zone_id
    }
  }

  allow_overwrite = true
  name            = each.value.name
  records         = [each.value.record]
  ttl             = 60
  type            = each.value.type
  zone_id         = each.value.zone
}

resource "aws_acm_certificate_validation" "this" {
  certificate_arn         = aws_acm_certificate.this.arn
  validation_record_fqdns = [for r in aws_route53_record.cert_validation : r.fqdn]
}

resource "aws_route53_record" "alias" {
  for_each = local.domain_map

  zone_id = data.aws_route53_zone.domains[each.value.zone_name].zone_id
  name    = each.value.fqdn
  type    = "A"

  alias {
    name                   = aws_lb.this.dns_name
    zone_id                = aws_lb.this.zone_id
    evaluate_target_health = true
  }
}

# ------------------------------------------------------------------------------
# Network Load Balancer (TLS:443 -> TCP:8000 on EC2)
# ------------------------------------------------------------------------------

resource "aws_lb" "this" {
  name               = var.name_prefix
  internal           = false
  load_balancer_type = "network"
  subnets            = data.aws_subnets.default.ids
  security_groups    = [aws_security_group.nlb.id]

  tags = {
    Name = var.name_prefix
  }
}

resource "aws_lb_target_group" "envoy" {
  name               = "${var.name_prefix}-envoy"
  port               = 8000
  protocol           = "TCP"
  vpc_id             = data.aws_vpc.default.id
  preserve_client_ip = false

  health_check {
    protocol            = "TCP"
    port                = "8000"
    healthy_threshold   = 2
    unhealthy_threshold = 2
    interval            = 10
  }

  tags = {
    Name = "${var.name_prefix}-envoy"
  }
}

resource "aws_lb_listener" "tls" {
  load_balancer_arn = aws_lb.this.arn
  port              = 443
  protocol          = "TLS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = aws_acm_certificate_validation.this.certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.envoy.arn
  }
}

resource "aws_lb_target_group_attachment" "ec2" {
  target_group_arn = aws_lb_target_group.envoy.arn
  target_id        = aws_instance.this.id
  port             = 8000
}

# ------------------------------------------------------------------------------
# IAM + EC2 (Amazon Linux 2023 + Docker Compose)
# ------------------------------------------------------------------------------

resource "aws_iam_role" "ec2" {
  name = "${var.name_prefix}-ec2"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "ec2.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecr_read" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ec2" {
  name = "${var.name_prefix}-ec2"
  role = aws_iam_role.ec2.name
}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-kernel-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_instance" "this" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  subnet_id                   = data.aws_subnets.default.ids[0]
  vpc_security_group_ids      = [aws_security_group.ec2.id]
  iam_instance_profile        = aws_iam_instance_profile.ec2.name
  associate_public_ip_address = true

  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    aws_region      = var.aws_region
    ecr_registry    = var.ecr_registry
    ecr_repo_prefix = var.ecr_repo_prefix
    image_tag       = var.image_tag
    git_repo_url    = var.git_repo_url
    git_ref         = var.git_ref
  })

  metadata_options {
    http_tokens = "required"
  }

  tags = {
    Name = var.name_prefix
  }
}
