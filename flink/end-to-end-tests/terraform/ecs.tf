resource "aws_ecs_cluster" "flink_end_to_end_tests" {
  name = "flink_end_to_end_tests_cluster"
}

resource "aws_ecs_task_definition" "flink_session_cluster" {
  family = "flink_session_cluster"

  container_definitions = jsonencode([
    {
      name         = "jobmanager"
      image        = "flink:1.13.0-scala_2.12-java8"
      cpu          = 2048
      memory       = 4096
      essential    = true
      portMappings = [
        {
          containerPort = 8081
          hostPort      = 8081
        }
      ]
    },
    {
      name      = "taskmanager"
      image     = "flink:1.13.0-scala_2.12-java8"
      cpu       = 2048
      memory    = 4096
      essential = true
    }
  ])

  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 4096
  memory                   = 8192
  execution_role_arn       = aws_iam_role.flink_session_cluster_execution_task_role.arn
  task_role_arn            = aws_iam_role.flink_session_cluster_execution_task_role.arn
}

resource "aws_iam_role" "flink_session_cluster_execution_task_role" {
  name               = "flink-session-cluster-execution-task-role"
  assume_role_policy = data.aws_iam_policy_document.assume_role_policy.json
}

data "aws_iam_policy_document" "assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy_attachment" "ecsTaskExecutionRole_policy" {
  role       = aws_iam_role.flink_session_cluster_execution_task_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_ecs_service" "flink_session_cluster" {
  name                 = "flink_session_cluster"
  cluster              = aws_ecs_cluster.flink_end_to_end_tests.id
  task_definition      = aws_ecs_task_definition.flink_session_cluster.id
  scheduling_strategy  = "REPLICA"
  desired_count        = 1
  force_new_deployment = true

  network_configuration {
    subnets          = [aws_subnet.benchmarks_subnet1.id, aws_subnet.benchmarks_subnet2.id]
    assign_public_ip = false
    security_groups  = [aws_security_group.allow_my_ip.id]
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.target_group.arn
    container_name   = "jobmanager"
    container_port   = 8081
  }

  depends_on = [aws_lb_listener.listener]
}

resource "aws_alb" "application_load_balancer" {
  name               = "flink-session-cluster-alb"
  internal           = false
  load_balancer_type = "application"
  subnets            = [aws_subnet.benchmarks_subnet1.id, aws_subnet.benchmarks_subnet2.id]
  security_groups    = [aws_security_group.allow_my_ip.id]
}

resource "aws_lb_target_group" "target_group" {
  name        = "flink-session-cluster-tg"
  port        = 8081
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = aws_vpc.this.id

  health_check {
    healthy_threshold   = "3"
    interval            = "300"
    protocol            = "HTTP"
    matcher             = "200"
    timeout             = "3"
    path                = "/config/"
    unhealthy_threshold = "2"
  }
}

resource "aws_lb_listener" "listener" {
  load_balancer_arn = aws_alb.application_load_balancer.id
  port              = "8081"
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.target_group.id
  }
}
