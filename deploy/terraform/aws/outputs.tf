output "cluster_name" {
  description = "EKS cluster name."
  value       = module.eks.cluster_name
}

output "region" {
  description = "AWS region the cluster runs in."
  value       = var.region
}

output "cluster_endpoint" {
  description = "EKS API server endpoint."
  value       = module.eks.cluster_endpoint
}

output "cluster_version" {
  description = "Kubernetes version of the control plane."
  value       = module.eks.cluster_version
}

output "vpc_id" {
  description = "VPC the cluster runs in."
  value       = module.vpc.vpc_id
}

output "node_group_subnets" {
  description = "Subnets the worker nodes were placed in."
  value       = local.node_subnets
}

output "nat_gateway_enabled" {
  description = "Whether a NAT gateway was created (a recurring cost)."
  value       = var.enable_nat_gateway
}

output "configure_kubectl" {
  description = "Command to configure kubectl for this cluster."
  value       = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.region}"
}

output "destroy_reminder" {
  description = "How to tear the cluster down when the demonstration is over."
  value       = <<-EOT
    This cluster bills continuously whether or not it is used.

      helm uninstall edgemesh --namespace edgemesh
      kubectl delete namespace edgemesh
      terraform destroy -var 'cluster_name=${var.cluster_name}' -var 'region=${var.region}'

    Uninstall the Helm release first so any load balancers it created are
    removed; otherwise they are orphaned and keep billing.
  EOT
}
