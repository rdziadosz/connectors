locals {
  flink_session_cluster_name   = "flink-session-cluster"
  jobmanager_container_name    = "jobmanager"
  jobmanager_rest_port         = 8081
  flink_version                = "1.13.0"
  flink_jobmanager_properties  = <<EOT
    jobmanager.rpc.address: jobmanager
    jobmanager.memory.process.size: 3g
    jobmanager.memory.jvm-metaspace.size: 1g
    fs.s3a.aws.credentials.provider: com.amazonaws.auth.ContainerCredentialsProvider
    classloader.check-leaked-classloader: false
    EOT
  flink_taskmanager_properties = <<EOT
    jobmanager.rpc.address: jobmanager
    taskmanager.numberOfTaskSlots: 4
    taskmanager.memory.process.size: 10g
    taskmanager.memory.jvm-metaspace.size: 1g
    fs.s3a.aws.credentials.provider: com.amazonaws.auth.ContainerCredentialsProvider
    classloader.check-leaked-classloader: false
    EOT
}

/* ========== EC2 ========== */

data "aws_ami" "default" {
  filter {
    name   = "name"
    values = ["amzn2-ami-ecs-hvm-2.0.202*-x86_64-ebs"]
  }
  most_recent = true
  owners      = ["amazon"]
}

/* ========== ELB ========== */

resource "aws_alb" "public" {
  name            = "e2e-flink-cluster-alb"
  security_groups = [aws_security_group.flink_session_cluster_lb.id]
  subnets         = [var.subnet1_id, var.subnet2_id]
}

resource "aws_alb_target_group" "public_rest" {
  name     = "e2e-flink-cluster-rest-alb-tg"
  vpc_id   = var.vpc_id
  protocol = "HTTP"
  port     = local.jobmanager_rest_port
  health_check {
    path = "/"
    port = local.jobmanager_rest_port
  }
}

resource "aws_alb_listener" "public_rest" {
  load_balancer_arn = aws_alb.public.arn
  port              = local.jobmanager_rest_port
  protocol          = "HTTP"
  default_action {
    target_group_arn = aws_alb_target_group.public_rest.arn
    type             = "forward"
  }
}

resource "aws_launch_configuration" "flink_session_cluster" {
  iam_instance_profile        = aws_iam_instance_profile.ecs_instance.name
  image_id                    = data.aws_ami.default.id
  instance_type               = "c5.2xlarge"
  associate_public_ip_address = true
  security_groups             = [aws_security_group.flink_session_cluster_ec2.id]
  lifecycle {
    create_before_destroy = true
  }
  name_prefix = "e2e-flink-session-cluster-"
  root_block_device {
    volume_type = "gp2"
    volume_size = 30
  }
  user_data = "#!/bin/bash\necho ECS_CLUSTER=${local.flink_session_cluster_name} >> /etc/ecs/ecs.config"
}

/* ========== Autoscaling ========== */

resource "aws_autoscaling_group" "flink_session_cluster" {
  name                 = "e2e-flink-cluster-ag"
  launch_configuration = aws_launch_configuration.flink_session_cluster.name
  health_check_type    = "EC2"
  desired_capacity     = 1
  max_size             = 1
  min_size             = 1
  target_group_arns    = [aws_alb_target_group.public_rest.arn]
  termination_policies = ["OldestInstance"]
  vpc_zone_identifier  = [var.subnet1_id, var.subnet2_id]
  # AWS Auto Scaling Groups (ASGs) dynamically create and destroy EC2 instances as defined in the ASG's configuration.
  # Because these EC2 instances are created and destroyed by AWS, Terraform does not manage them, and is not directly
  # aware of them. As a result, the AWS provider cannot apply your default tags to the EC2 instances managed by
  # your ASG.
  dynamic "tag" {
    for_each = data.aws_default_tags.current.tags
    content {
      key                 = tag.key
      value               = tag.value
      propagate_at_launch = true
    }
  }
}

/* ========== ECS ========== */

resource "aws_ecs_capacity_provider" "this" {
  name = "e2e-flink-cluster-default-capacity-provider"
  auto_scaling_group_provider {
    auto_scaling_group_arn = aws_autoscaling_group.flink_session_cluster.arn
  }
}

resource "aws_ecs_cluster" "flink_session_cluster" {
  name = local.flink_session_cluster_name
  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_ecs_cluster_capacity_providers" "this" {
  cluster_name       = aws_ecs_cluster.flink_session_cluster.name
  capacity_providers = [aws_ecs_capacity_provider.this.name]
}

resource "aws_ecs_task_definition" "flink_session_cluster" {
  family                   = "e2e-flink-cluster"
  network_mode             = "bridge"
  requires_compatibilities = ["EC2"]
  execution_role_arn       = aws_iam_role.flink_session_cluster_execution.arn
  task_role_arn            = aws_iam_role.flink_session_cluster_task.arn
  container_definitions    = templatefile("./modules/flink-session-cluster/flink-containers.json", {
    flink_version          = local.flink_version
    region                 = var.region
    cloudwatch_log_group   = var.cloudwatch_group_id
    // join+split: Convert a multiline string to a regular string
    taskmanager_properties = join("\\n", split("\n", local.flink_taskmanager_properties))
    jobmanager_properties  = join("\\n", split("\n", local.flink_jobmanager_properties))
  })
}

data "aws_ecs_task_definition" "flink_session_cluster" {
  task_definition = aws_ecs_task_definition.flink_session_cluster.family
}

resource "aws_ecs_service" flink_session_cluster {
  name                    = "e2e-flink-cluster"
  task_definition         = "${aws_ecs_task_definition.flink_session_cluster.family}:${data.aws_ecs_task_definition.flink_session_cluster.revision}"
  cluster                 = aws_ecs_cluster.flink_session_cluster.id
  desired_count           = 1
  wait_for_steady_state   = true
  enable_ecs_managed_tags = true
  force_new_deployment    = true
  load_balancer {
    target_group_arn = aws_alb_target_group.public_rest.arn
    container_name   = local.jobmanager_container_name
    container_port   = local.jobmanager_rest_port
  }
  propagate_tags = "SERVICE"
  depends_on = [
    aws_iam_role_policy_attachment.ecs_instance,
    aws_autoscaling_group.flink_session_cluster
  ]
}

/* ========== IAM ========== */

data "aws_iam_policy_document" "ec2_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      identifiers = ["ec2.amazonaws.com"]
      type        = "Service"
    }
  }
}

resource "aws_iam_role" "ecs_instance" {
  name               = "e2eFlinkEcsInstanceRole"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role_policy.json
}

resource "aws_iam_role_policy_attachment" "ecs_instance" {
  role       = aws_iam_role.ecs_instance.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_instance_profile" "ecs_instance" {
  name = "e2eFlinkEcsInstanceProfile"
  role = aws_iam_role.ecs_instance.name
}

resource "aws_iam_role" "flink_session_cluster_execution" {
  name               = "e2eFlinkClusterTaskExecutionRole"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role_policy.json
}

data "aws_iam_policy_document" "ecs_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      identifiers = ["ecs-tasks.amazonaws.com"]
      type        = "Service"
    }
  }
}

resource "aws_iam_role_policy_attachment" "flink_session_cluster_execution" {
  role       = aws_iam_role.flink_session_cluster_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}


resource "aws_iam_role" "flink_session_cluster_task" {
  name               = "e2eFlinkClusterTaskRole"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume_role_policy.json
}

resource "aws_iam_role_policy" "flink_session_cluster_task" {
  name   = "e2eFlinkClusterTaskPolicy"
  role   = aws_iam_role.flink_session_cluster_task.id
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:AbortMultipartUpload",
                "s3:ListBucket",
                "s3:DeleteObject",
                "s3:GetObjectVersion",
                "s3:ListMultipartUploadParts",
                "s3:*"
            ],
            "Resource": [
                "arn:aws:s3:::${var.test_data_bucket_name}/*",
                "arn:aws:s3:::${var.test_data_bucket_name}"
            ]
        }
    ]
}
EOF
}

/* ========== VPC ========== */

// TODO: limit access to LB to specific addresses?
resource "aws_security_group" "flink_session_cluster_lb" {
  name   = "e2e-flink-cluster-lb-security-group"
  vpc_id = var.vpc_id
  ingress {
    cidr_blocks = ["0.0.0.0/0"]
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
  }
  egress {
    cidr_blocks = ["0.0.0.0/0"]
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
  }
}

resource "aws_security_group" "flink_session_cluster_ec2" {
  name   = "e2e-flink-cluster-ec2-security-group"
  vpc_id = var.vpc_id
  ingress {
    from_port       = 0
    to_port         = 0
    protocol        = "-1"
    security_groups = [aws_security_group.flink_session_cluster_lb.id]
  }
  egress {
    cidr_blocks = ["0.0.0.0/0"]
    protocol    = "-1"
    from_port   = 0
    to_port     = 0
  }
}
