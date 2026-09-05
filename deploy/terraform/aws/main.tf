# EdgeMesh demonstration cluster on AWS EKS.
#
# Established community modules are used for the VPC and the cluster: the
# portfolio-defining engineering in this repository is EdgeMesh itself, and
# hand-written VPC and IAM boilerplate would add risk without adding signal.
#
# Nothing here applies automatically. See README.md for the cost warning.

provider "aws" {
  region = var.region

  default_tags {
    tags = merge(
      {
        Project     = "edgemesh"
        ManagedBy   = "terraform"
        Environment = "demo"
        # A demonstration cluster that outlives its demonstration is the most
        # common way this configuration costs money unexpectedly.
        Warning = "billable-demo-cluster-destroy-when-done"
      },
      var.tags,
    )
  }
}

data "aws_availability_zones" "available" {
  state = "available"

  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, var.availability_zone_count)

  # /20 subnets give ~4,000 addresses each, which is ample for a demo and
  # leaves room in the VPC for future subnets.
  public_subnets  = [for i in range(var.availability_zone_count) : cidrsubnet(var.vpc_cidr, 4, i)]
  private_subnets = [for i in range(var.availability_zone_count) : cidrsubnet(var.vpc_cidr, 4, i + 8)]

  # Without NAT there is no egress from private subnets, so nodes go in the
  # public ones. This is an explicit demonstration trade-off, not an oversight.
  node_subnets = var.enable_nat_gateway ? module.vpc.private_subnets : module.vpc.public_subnets
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.17"

  name = "${var.cluster_name}-vpc"
  cidr = var.vpc_cidr

  azs             = local.azs
  public_subnets  = local.public_subnets
  private_subnets = local.private_subnets

  enable_nat_gateway = var.enable_nat_gateway
  # One shared gateway rather than one per AZ: a demo does not need
  # zone-independent egress, and three gateways triple the cost.
  single_nat_gateway = var.enable_nat_gateway

  enable_dns_hostnames = true
  enable_dns_support   = true

  # Tags the AWS load balancer controller uses to pick subnets.
  public_subnet_tags = {
    "kubernetes.io/role/elb"                    = 1
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb"           = 1
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.31"

  cluster_name    = var.cluster_name
  cluster_version = var.kubernetes_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = local.node_subnets

  cluster_endpoint_public_access       = true
  cluster_endpoint_public_access_cidrs = var.cluster_endpoint_public_access_cidrs
  cluster_endpoint_private_access      = true

  # Audit and authenticator logs are what make an incident reconstructable.
  # Retention is bounded so the logs do not become the largest line on the bill.
  cluster_enabled_log_types              = ["api", "audit", "authenticator"]
  cloudwatch_log_group_retention_in_days = var.log_retention_days

  # The creating principal gets admin access, so `aws eks update-kubeconfig`
  # works immediately after apply without a second authorization step.
  enable_cluster_creator_admin_permissions = true
  authentication_mode                      = "API_AND_CONFIG_MAP"

  cluster_addons = {
    coredns    = { most_recent = true }
    kube-proxy = { most_recent = true }
    vpc-cni    = { most_recent = true }
    # EdgeMesh's control plane uses PersistentVolumeClaims for Raft state, so
    # the EBS CSI driver is required rather than optional.
    aws-ebs-csi-driver = {
      most_recent              = true
      service_account_role_arn = module.ebs_csi_irsa.iam_role_arn
    }
  }

  eks_managed_node_group_defaults = {
    ami_type       = "AL2023_x86_64_STANDARD"
    disk_size      = var.node_disk_size
    instance_types = var.node_instance_types
  }

  eks_managed_node_groups = {
    default = {
      name = "${var.cluster_name}-ng"

      min_size     = var.node_min_size
      max_size     = var.node_max_size
      desired_size = var.node_desired_size

      instance_types = var.node_instance_types
      capacity_type  = "ON_DEMAND"

      # Without NAT the nodes need public addresses to pull images and reach
      # the EKS endpoint.
      subnet_ids                  = local.node_subnets
      associate_public_ip_address = !var.enable_nat_gateway

      labels = {
        "edgemesh.io/role" = "worker"
      }

      tags = {
        Name = "${var.cluster_name}-node"
      }
    }
  }

  # EdgeMesh's peer-cache and Raft traffic stays inside the cluster, so the node
  # security group only needs the intra-cluster rules the module provides plus
  # explicit node-to-node access on EdgeMesh's ports.
  node_security_group_additional_rules = {
    edgemesh_peer_cache = {
      description = "EdgeMesh peer-cache RPC between edge nodes"
      protocol    = "tcp"
      from_port   = 7200
      to_port     = 7200
      type        = "ingress"
      self        = true
    }
    edgemesh_raft = {
      description = "EdgeMesh Raft consensus between control nodes"
      protocol    = "tcp"
      from_port   = 7000
      to_port     = 7000
      type        = "ingress"
      self        = true
    }
    edgemesh_control = {
      description = "EdgeMesh edge-to-control configuration stream"
      protocol    = "tcp"
      from_port   = 7300
      to_port     = 7300
      type        = "ingress"
      self        = true
    }
  }
}

# IAM role for the EBS CSI driver, which the control plane's PersistentVolumes
# depend on.
module "ebs_csi_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.48"

  role_name             = "${var.cluster_name}-ebs-csi"
  attach_ebs_csi_policy = true

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:ebs-csi-controller-sa"]
    }
  }
}
