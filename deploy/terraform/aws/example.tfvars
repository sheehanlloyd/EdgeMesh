# Example variables for an EdgeMesh demonstration cluster.
#
# Copy to terraform.tfvars and edit. Never commit a file containing secrets;
# terraform.tfvars is git-ignored for that reason.

region       = "us-east-1"
cluster_name = "edgemesh-demo"

# Two t3.medium nodes are enough for 3 control pods, 3 edge pods, and 2 origins.
node_instance_types = ["t3.medium"]
node_desired_size   = 2
node_min_size       = 2
node_max_size       = 4

# Leave NAT disabled for a short-lived demonstration: it adds roughly $32/month
# plus data processing charges.
enable_nat_gateway = false

# Restrict this to your own address. The default is open, which is convenient
# for a demonstration and wrong for anything else.
# cluster_endpoint_public_access_cidrs = ["203.0.113.4/32"]

tags = {
  Owner   = "your-name"
  Purpose = "edgemesh-portfolio-demo"
}
