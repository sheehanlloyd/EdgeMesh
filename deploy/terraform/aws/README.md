# EdgeMesh on AWS EKS

Terraform for a small demonstration EKS cluster.

## Cost warning

**Applying this configuration creates billable AWS resources.** At the default
sizing that is roughly:

| Resource | Default | Approximate cost |
|---|---|---|
| EKS control plane | 1 cluster | ~$0.10/hour (~$73/month) |
| Managed node group | 2 × `t3.medium` | ~$0.08/hour (~$60/month) |
| EBS volumes | 2 × 20 GiB gp3 | ~$3/month |
| NAT gateway | **disabled by default** | ~$32/month + data if enabled |

Roughly **$0.20/hour**, or about **$140/month** if left running.

Rates vary by region and change over time. Check the AWS pricing pages before
relying on these figures.

### Guardrails

- No CI workflow runs `terraform apply`. CI runs `fmt -check`, `init -backend=false`,
  and `validate` only.
- No script in this repository runs `terraform apply`.
- `enable_nat_gateway` defaults to `false`. Nodes run in public subnets with
  public IPs, which is appropriate for a short-lived demonstration and is
  documented as such below.
- `single_nat_gateway` is `true` when NAT is enabled, so a demo never creates
  one gateway per availability zone.

## Usage

```bash
cd deploy/terraform/aws

terraform init
terraform plan -var 'cluster_name=edgemesh-demo' -var 'region=us-east-1'

# Applying costs money. This is deliberately not wrapped in a make target.
terraform apply -var 'cluster_name=edgemesh-demo' -var 'region=us-east-1'

aws eks update-kubeconfig --name edgemesh-demo --region us-east-1

helm upgrade --install edgemesh ../../helm/edgemesh \
  --namespace edgemesh --create-namespace \
  --set image.registry=<your-registry> \
  --set mode=production \
  --set security.mtls.enabled=true \
  --set adminAuth.enabled=true \
  --set adminAuth.existingSecret=edgemesh-admin \
  --set 'edge.originSecurity.allowedCIDRs={10.0.0.0/16}'
```

## Destroy

**Destroy the cluster when you are finished.** An idle EKS cluster costs the
same as a busy one.

```bash
# Remove the Helm release first so any cloud load balancers it created are
# deleted; otherwise they are orphaned and keep billing after the cluster is
# gone.
helm uninstall edgemesh --namespace edgemesh
kubectl delete namespace edgemesh

terraform destroy -var 'cluster_name=edgemesh-demo' -var 'region=us-east-1'
```

Verify afterwards in the console that no load balancers, EBS volumes, or
Elastic IPs survive.

## Security posture

The defaults are for a demonstration, not production:

- The EKS API endpoint is public. Restrict it with
  `cluster_endpoint_public_access_cidrs`.
- Without NAT, nodes sit in public subnets. Set `enable_nat_gateway = true` for
  private nodes, and accept the added cost.
- Control-plane logging is enabled for `api`, `audit`, and `authenticator`,
  which is what makes an incident reconstructable. CloudWatch retention is 7
  days by default to bound cost.

## What is not here

- No production networking (Transit Gateway, VPC peering, PrivateLink).
- No cluster autoscaler or Karpenter.
- No external-dns, cert-manager, or ingress controller.
- No multi-region topology.

EdgeMesh is the portfolio-defining work in this repository; the infrastructure
around it deliberately uses well-tested community modules rather than
hand-written VPC and IAM boilerplate.
