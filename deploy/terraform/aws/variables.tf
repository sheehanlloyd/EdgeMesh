variable "region" {
  description = "AWS region for the cluster."
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "Name of the EKS cluster."
  type        = string
  default     = "edgemesh-demo"

  validation {
    condition     = can(regex("^[a-zA-Z][a-zA-Z0-9-]{0,99}$", var.cluster_name))
    error_message = "cluster_name must start with a letter and contain only letters, digits, and hyphens."
  }
}

variable "kubernetes_version" {
  description = "EKS control-plane version."
  type        = string
  default     = "1.31"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.42.0.0/16"

  validation {
    condition     = can(cidrhost(var.vpc_cidr, 0))
    error_message = "vpc_cidr must be a valid CIDR block."
  }
}

variable "availability_zone_count" {
  description = "Number of availability zones to spread subnets across."
  type        = number
  default     = 2

  validation {
    # Two AZs is the minimum EKS accepts; three is the useful maximum for a
    # demonstration before cost outweighs the added resilience.
    condition     = var.availability_zone_count >= 2 && var.availability_zone_count <= 3
    error_message = "availability_zone_count must be 2 or 3."
  }
}

variable "node_instance_types" {
  description = "Instance types for the managed node group."
  type        = list(string)
  default     = ["t3.medium"]
}

variable "node_desired_size" {
  description = "Desired node count."
  type        = number
  default     = 2
}

variable "node_min_size" {
  description = "Minimum node count."
  type        = number
  default     = 2
}

variable "node_max_size" {
  description = "Maximum node count."
  type        = number
  default     = 4
}

variable "node_disk_size" {
  description = "EBS volume size per node, in GiB."
  type        = number
  default     = 20
}

variable "enable_nat_gateway" {
  description = <<-EOT
    Place worker nodes in private subnets behind a NAT gateway.

    A NAT gateway costs roughly $32/month plus data processing charges, which is
    why this defaults to false: a short-lived demonstration does not need it,
    and an accidental one is the most common surprise on an AWS bill.
  EOT
  type        = bool
  default     = false
}

variable "cluster_endpoint_public_access_cidrs" {
  description = <<-EOT
    CIDR blocks permitted to reach the EKS public API endpoint.

    The default is open, which is convenient for a demonstration and wrong for
    anything else. Restrict it to your own address.
  EOT
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "log_retention_days" {
  description = "CloudWatch retention for EKS control-plane logs."
  type        = number
  default     = 7
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default     = {}
}
