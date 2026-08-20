#!/usr/bin/env bash
#
# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Create the EC2 instance the soak runs on. bootstrap.sh (one-time host setup:
# toolchain, jemalloc, the OTel Collector, then build.sh) and run.sh (the soak
# supervisor) both assume the instance already exists; nothing in this
# directory creates one until now.
#
# Defaults are the exact subnet, security group, AMI, instance type and
# cflt_* governance tags already in use for this project's soak host — not
# generic guesses. Override anything via flags if a second host, a different
# region, or a different account is ever needed.
#
# Usage:
#   ./create-ec2.sh                       # create with defaults, prompting for --key-name if new
#   ./create-ec2.sh --dry-run             # validate permissions/parameters, create nothing
#   ./create-ec2.sh --label njc-rust-soak-tests-2 --key-name MY-KEY
#   ./create-ec2.sh --terminate i-0123456789abcdef0
#
# After creation, this prints the exact next steps: the SSH command, the
# `git archive | scp` delivery bootstrap.sh requires (the repo cannot be
# cloned on the box — see bootstrap.sh), and the bootstrap.sh invocation.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  create-ec2.sh [options]              Create the soak instance
  create-ec2.sh --dry-run [options]    Validate only; creates nothing
  create-ec2.sh --terminate <id>       Terminate a previously created instance

Options (all default to this project's existing soak host configuration):
  --label <name>          Name / cflt_service tag value. Default: njc-rust-soak-tests.
                          Vary this to run a second, independent host.
  --region <region>       Default: us-west-2.
  --ami-id <id>           Default: ami-02167eae61967e403.
  --instance-type <type>  Default: c6a.2xlarge (8 vCPU / 16 GiB).
  --key-name <name>       EC2 key pair name. Default: NJC-KEY. Created
                          automatically (aws ec2 create-key-pair) if it does
                          not already exist in the target region; the private
                          key is written to --key-out and chmod 400.
  --key-out <path>        Where to write a newly created private key.
                          Default: ~/.ssh/<key-name>.pem
  --subnet-id <id>        Default: subnet-7d015e27.
  --security-group-id <id>
                          Default: sg-09baff84e9f7d8782. This is an existing,
                          presumably SecOps-approved group -- this script
                          never creates or modifies a security group.
  --iam-instance-profile <name>
                          Default: njc-rust-soak-tests-role.
  --volume-size-gb <n>    Root EBS volume size. Default: 100.
  --dry-run               Pass --dry-run to `aws ec2 run-instances` (an IAM
                          permission check; creates nothing) and print the
                          resolved parameters instead of launching.
  --terminate <id>        Terminate the given instance id and exit. Provided
                          because a forgotten running instance is a standing
                          AWS bill; there is no other cleanup path here.
  -h, --help              This message.

Requires the AWS CLI, configured with credentials that can run-instances /
create-key-pair / describe-key-pairs / terminate-instances in the target
account. This script never invokes AWS except in direct response to the flags
above -- it does not run automatically, and --dry-run touches nothing.
EOF
}

# --- defaults: this project's existing, working soak-host configuration ----
LABEL="njc-rust-soak-tests"
REGION="us-west-2"
AMI_ID="ami-02167eae61967e403"
INSTANCE_TYPE="c6a.2xlarge"
KEY_NAME="NJC-KEY"
KEY_OUT=""
SUBNET_ID="subnet-7d015e27"
SECURITY_GROUP_ID="sg-09baff84e9f7d8782"
IAM_PROFILE="njc-rust-soak-tests-role"
VOLUME_SIZE_GB=100
DRY_RUN=false
TERMINATE_ID=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --label)               LABEL="$2"; shift 2 ;;
        --region)               REGION="$2"; shift 2 ;;
        --ami-id)               AMI_ID="$2"; shift 2 ;;
        --instance-type)        INSTANCE_TYPE="$2"; shift 2 ;;
        --key-name)             KEY_NAME="$2"; shift 2 ;;
        --key-out)              KEY_OUT="$2"; shift 2 ;;
        --subnet-id)            SUBNET_ID="$2"; shift 2 ;;
        --security-group-id)    SECURITY_GROUP_ID="$2"; shift 2 ;;
        --iam-instance-profile) IAM_PROFILE="$2"; shift 2 ;;
        --volume-size-gb)       VOLUME_SIZE_GB="$2"; shift 2 ;;
        --dry-run)              DRY_RUN=true; shift ;;
        --terminate)            TERMINATE_ID="$2"; shift 2 ;;
        -h|--help)              usage; exit 0 ;;
        *) echo "ERROR: unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if ! command -v aws >/dev/null 2>&1; then
    echo "ERROR: aws CLI not found. Install it: https://aws.amazon.com/cli/" >&2
    exit 2
fi
if ! aws sts get-caller-identity --region "$REGION" >/dev/null 2>&1; then
    echo "ERROR: aws CLI has no working credentials for region $REGION." \
         "Run 'aws configure' or set AWS_PROFILE first." >&2
    exit 2
fi

# --- terminate mode ----------------------------------------------------------
if [[ -n "$TERMINATE_ID" ]]; then
    echo ">>> Terminating $TERMINATE_ID in $REGION"
    aws ec2 terminate-instances --region "$REGION" --instance-ids "$TERMINATE_ID"
    exit 0
fi

[[ -n "$KEY_OUT" ]] || KEY_OUT="$HOME/.ssh/${KEY_NAME}.pem"

# --- key pair: create iff it does not already exist in this region ---------
if aws ec2 describe-key-pairs --region "$REGION" --key-names "$KEY_NAME" >/dev/null 2>&1; then
    echo ">>> Key pair $KEY_NAME already exists in $REGION; reusing it."
    echo "    (If you don't have its private key, this script cannot recover it --"
    echo "     AWS never returns key material after creation. Use a different"
    echo "     --key-name, or ask whoever created $KEY_NAME for the .pem.)"
else
    if [[ "$DRY_RUN" == true ]]; then
        echo ">>> [dry-run] Would create key pair $KEY_NAME in $REGION, writing to $KEY_OUT"
    else
        echo ">>> Creating key pair $KEY_NAME in $REGION -> $KEY_OUT"
        mkdir -p "$(dirname "$KEY_OUT")"
        aws ec2 create-key-pair --region "$REGION" --key-name "$KEY_NAME" \
            --key-type rsa --query 'KeyMaterial' --output text > "$KEY_OUT"
        chmod 400 "$KEY_OUT"
    fi
fi

TAGS_INSTANCE="ResourceType=instance,Tags=[{Key=Name,Value=$LABEL},{Key=cflt_service,Value=$LABEL},{Key=cflt_managed_by,Value=user},{Key=cflt_managed_id,Value=confluentinc/example-confluent-kafka-rust},{Key=cflt_partition,Value=operational-tools},{Key=cflt_environment,Value=devel}]"
TAGS_VOLUME="ResourceType=volume,Tags=[{Key=Name,Value=$LABEL},{Key=cflt_service,Value=$LABEL},{Key=cflt_managed_by,Value=user},{Key=cflt_managed_id,Value=confluentinc/example-confluent-kafka-rust},{Key=cflt_partition,Value=operational-tools},{Key=cflt_environment,Value=devel}]"
TAGS_ENI="ResourceType=network-interface,Tags=[{Key=cflt_service,Value=$LABEL},{Key=cflt_managed_by,Value=user},{Key=cflt_managed_id,Value=confluentinc/example-confluent-kafka-rust},{Key=cflt_partition,Value=operational-tools},{Key=cflt_environment,Value=devel}]"
BLOCK_DEVICES="[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":$VOLUME_SIZE_GB,\"VolumeType\":\"gp3\",\"Iops\":3000,\"Throughput\":125,\"Encrypted\":true,\"DeleteOnTermination\":true}}]"

RUN_ARGS=(
    ec2 run-instances
    --region "$REGION"
    --image-id "$AMI_ID"
    --instance-type "$INSTANCE_TYPE"
    --key-name "$KEY_NAME"
    --subnet-id "$SUBNET_ID"
    --security-group-ids "$SECURITY_GROUP_ID"
    --associate-public-ip-address
    --iam-instance-profile "Name=$IAM_PROFILE"
    --block-device-mappings "$BLOCK_DEVICES"
    --tag-specifications "$TAGS_INSTANCE" "$TAGS_VOLUME" "$TAGS_ENI"
)

if [[ "$DRY_RUN" == true ]]; then
    echo ">>> [dry-run] aws ${RUN_ARGS[*]} --dry-run"
    aws "${RUN_ARGS[@]}" --dry-run
    echo ">>> Dry-run permission check above. Nothing was created."
    exit 0
fi

echo ">>> aws ${RUN_ARGS[*]}"
INSTANCE_ID="$(aws "${RUN_ARGS[@]}" --query 'Instances[0].InstanceId' --output text)"
echo ">>> Launched $INSTANCE_ID; waiting for it to enter 'running'..."
aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE_ID"

PUBLIC_IP="$(aws ec2 describe-instances --region "$REGION" --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)"

cat <<EOF

>>> Instance running: $INSTANCE_ID at $PUBLIC_IP

    To terminate it later:
      $0 --terminate $INSTANCE_ID --region $REGION

    Next steps (the repo cannot be cloned on the box -- see bootstrap.sh):

      ssh -i $KEY_OUT ubuntu@$PUBLIC_IP

      # from your machine, in the repo root:
      SHA=\$(git rev-parse HEAD)
      git archive "\$SHA" | ssh -i $KEY_OUT ubuntu@$PUBLIC_IP \\
          'mkdir -p ~/confluent-kafka-rust && tar -x -C ~/confluent-kafka-rust'

      # on the instance, before running bootstrap.sh, fill in the three
      # FILL_IN_* values in bindings/python/soak/otel-config.yaml, then:
      cd ~/confluent-kafka-rust/bindings/python/soak
      ./bootstrap.sh \$SHA "$LABEL"
EOF
