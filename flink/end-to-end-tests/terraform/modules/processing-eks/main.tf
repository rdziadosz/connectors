/* ========== EKS ========== */

resource "aws_eks_cluster" "flink-tests" {
  name     = "flink-tests-eks-cluster-${var.run_id}"
  role_arn = aws_iam_role.flink-tests.arn

  vpc_config {
    subnet_ids = [var.subnet1_id, var.subnet2_id]
  }

  # Ensure that IAM Role permissions are created before and deleted after EKS Cluster handling.
  # Otherwise, EKS will not be able to properly delete EKS managed EC2 infrastructure such as Security Groups.
  depends_on = [
    aws_iam_role.flink-tests,
    aws_iam_role_policy_attachment.flink-tests-AmazonEKSClusterPolicy,
    aws_iam_role_policy_attachment.flink-tests-AmazonEKSVPCResourceController,
  ]
}

resource "aws_eks_node_group" "flink-tests" {
  cluster_name    = aws_eks_cluster.flink-tests.name
  node_group_name = "flink-tests-node-group-${var.run_id}"
  subnet_ids      = [var.subnet1_id]
  instance_types  = []
  node_role_arn   = aws_iam_role.flink-tests_node_group.arn
  labels          = {
    "spark/component" = "executor"
    "role" = "spark"
  }

  launch_template {
    name    = aws_launch_template.flink-tests.name
    version = "$Latest"
  }

  scaling_config {
    desired_size = var.eks_workers
    max_size     = var.eks_workers
    min_size     = var.eks_workers
  }

  # Ensure that IAM Role permissions are created before and deleted after EKS Node Group handling.
  # Otherwise, EKS will not be able to properly delete EC2 Instances and Elastic Network Interfaces.
  depends_on = [
    aws_iam_role_policy_attachment.flink-tests-AmazonEKSWorkerNodePolicy,
    aws_iam_role_policy_attachment.flink-tests-AmazonEKS_CNI_Policy,
    aws_iam_role_policy_attachment.flink-tests-AmazonEC2ContainerRegistryReadOnly,
  ]
}

resource "aws_launch_template" "flink-tests" {
  instance_type = "i3.2xlarge"
  name          = "flink-tests-lt-${var.run_id}"
  lifecycle {
    create_before_destroy = true
  }
  tag_specifications {
    resource_type = "instance"
    tags          = var.tags
  }
}

resource "aws_eks_node_group" "egde_node" {
  cluster_name    = aws_eks_cluster.flink-tests.name
  node_group_name = "flink-tests-edge-node-node-group-${var.run_id}"
  subnet_ids      = [var.subnet1_id]
  instance_types  = []
  node_role_arn   = aws_iam_role.flink-tests_node_group.arn
  labels          = {
    "spark/component" = "driver"
    "role" = "edge-node"
  }

  launch_template {
    name    = aws_launch_template.egde_node.name
    version = "$Latest"
  }

  scaling_config {
    desired_size = 1
    max_size     = 1
    min_size     = 1
  }
  # Ensure that IAM Role permissions are created before and deleted after EKS Node Group handling.
  # Otherwise, EKS will not be able to properly delete EC2 Instances and Elastic Network Interfaces.
  depends_on = [
    aws_iam_role_policy_attachment.flink-tests-AmazonEKSWorkerNodePolicy,
    aws_iam_role_policy_attachment.flink-tests-AmazonEKS_CNI_Policy,
    aws_iam_role_policy_attachment.flink-tests-AmazonEC2ContainerRegistryReadOnly,
  ]
}

resource "aws_launch_template" "egde_node" {
  instance_type = "m5.xlarge"
  name          = "flink-tests-edge-node-lt-${var.run_id}"
  lifecycle {
    create_before_destroy = true
  }
  tag_specifications {
    resource_type = "instance"
    tags          = var.tags
  }
}

/* ========== IAM ========== */

resource "aws_iam_access_key" "user" {
  user    = aws_iam_user.user.name
}

resource "aws_iam_user" "user" {
  name = "flink-tests-user"
  path = "/"
}

resource "aws_iam_user_policy" "user_policy" {
  name = "flink-tests-user-policy"
  user = aws_iam_user.user.name

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::${var.test_data_bucket_name}"
            ],
            "Action": [
                "s3:ListBucket"
            ]
        },
        {
            "Effect": "Allow",
            "Resource": "arn:aws:s3:::${var.test_data_bucket_name}/*",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ]
        }
    ]
}
EOF
}


resource "aws_iam_role" "flink-tests" {
  name = "flink-tests-eks-cluster-role-${var.run_id}"

  assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "eks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy_attachment" "flink-tests-AmazonEKSClusterPolicy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
  role       = aws_iam_role.flink-tests.name
}

resource "aws_iam_role_policy_attachment" "flink-tests-AmazonEKSVPCResourceController" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSVPCResourceController"
  role       = aws_iam_role.flink-tests.name
}

resource "aws_iam_role" "flink-tests_node_group" {
  name = "flink-tests-eks-node-group-role-${var.run_id}"

  assume_role_policy = jsonencode({
    Statement = [
      {
        Action    = "sts:AssumeRole"
        Effect    = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
    Version = "2012-10-17"
  })
}

resource "aws_iam_role_policy_attachment" "flink-tests-AmazonEKSWorkerNodePolicy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.flink-tests_node_group.name
}

resource "aws_iam_role_policy_attachment" "flink-tests-AmazonEC2ContainerRegistryReadOnly" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.flink-tests_node_group.name
}

resource "aws_iam_role_policy_attachment" "flink-tests-AmazonEKS_CNI_Policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role       = aws_iam_role.flink-tests_node_group.name
}

data "tls_certificate" "flink-tests" {
  url = aws_eks_cluster.flink-tests.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "flink-tests" {
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.flink-tests.certificates[0].sha1_fingerprint]
  url             = aws_eks_cluster.flink-tests.identity[0].oidc[0].issuer
}

data "aws_iam_policy_document" "flink-tests_assume_role_policy" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      identifiers = [aws_iam_openid_connect_provider.flink-tests.arn]
      type        = "Federated"
    }
    condition {
      test     = "StringEquals"
      variable = "${replace(aws_iam_openid_connect_provider.flink-tests.url, "https://", "")}:sub"
      values   = ["system:serviceaccount:${local.namespace}:${local.service_account}"]
    }
    condition {
      test     = "StringEquals"
      variable = "${replace(aws_iam_openid_connect_provider.flink-tests.url, "https://", "")}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }

  depends_on = [aws_iam_openid_connect_provider.flink-tests]
}

resource "aws_iam_role" "container_role" {
  assume_role_policy = data.aws_iam_policy_document.flink-tests_assume_role_policy.json
  name               = "flink-tests-container-role-${var.run_id}"
}

resource "aws_iam_role_policy" "flink-tests_container_role_policy" {
  name   = "flink-tests_container_role_policy_${var.run_id}"
  role   = aws_iam_role.container_role.id
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::${var.test_data_bucket_name}"
            ],
            "Action": [
                "s3:ListBucket"
            ]
        },
        {
            "Effect": "Allow",
            "Resource": "arn:aws:s3:::${var.test_data_bucket_name}/*",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ]
        }
    ]
}
EOF
}
