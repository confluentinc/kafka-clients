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

"""Tests for create-ec2.sh: the required-value gate and --label validation.

This project's soak host lives in a real AWS account, and its subnet /
security group / IAM instance profile / AMI must never be committed as
defaults in this public repo (see create-ec2.sh's own comment block). These
tests drive the real script with a stub `aws` on PATH -- no real AWS account,
no credentials -- and assert:

  * the four account-identifying values are required (flag or SOAK_EC2_* env)
    for create/--dry-run, and the script fails clearly, before any `aws` call,
    when one is missing;
  * --terminate does not require them;
  * a `create-ec2.env` next to the script is sourced automatically and can
    supply them;
  * --label is validated against [A-Za-z0-9_-]+ before any `aws` call.

They need only bash, and run on macOS and Linux alike -- same as
test_run_sh.py.
"""

import os
import shutil
import stat
import subprocess

import pytest

SOAK_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREATE_EC2_SH = os.path.join(SOAK_DIR, "create-ec2.sh")

pytestmark = pytest.mark.skipif(shutil.which("bash") is None,
                                 reason="create-ec2.sh needs bash")

REQUIRED_FLAGS = {
    "--subnet-id": "subnet-fake123",
    "--security-group-id": "sg-fake123",
    "--iam-instance-profile": "fake-role",
    "--ami-id": "ami-fake123",
}


def _write_fake_aws(tmp_path):
    """A stub `aws` CLI: records every invocation, answers just enough to let
    create-ec2.sh's dry-run path complete."""
    calls_file = tmp_path / "aws-calls.log"
    fake_aws = tmp_path / "bin" / "aws"
    fake_aws.parent.mkdir(parents=True, exist_ok=True)
    fake_aws.write_text(
        "#!/usr/bin/env bash\n"
        'echo "$@" >> ' + str(calls_file) + "\n"
        'case "$*" in\n'
        '  sts\\ get-caller-identity*) exit 0 ;;\n'
        '  ec2\\ describe-key-pairs*) exit 1 ;;\n'  # key pair not found yet
        '  ec2\\ create-key-pair*) echo FAKEKEY; exit 0 ;;\n'
        '  ec2\\ run-instances*--dry-run*) exit 0 ;;\n'
        '  ec2\\ run-instances*) echo \'{"Instances":[{"InstanceId":"i-fake"}]}\''
        ' ; exit 0 ;;\n'
        '  ec2\\ wait\\ instance-running*) exit 0 ;;\n'
        '  ec2\\ describe-instances*) echo 203.0.113.5; exit 0 ;;\n'
        '  ec2\\ terminate-instances*) exit 0 ;;\n'
        "esac\n"
        "exit 0\n")
    fake_aws.chmod(fake_aws.stat().st_mode | stat.S_IEXEC)
    return fake_aws.parent, calls_file


def run_create_ec2(tmp_path, args, env=None):
    fake_bin, calls_file = _write_fake_aws(tmp_path)
    full_env = dict(os.environ)
    for key in list(full_env):
        if key.startswith("SOAK_EC2_"):
            del full_env[key]
    full_env["PATH"] = str(fake_bin) + os.pathsep + full_env.get("PATH", "")
    full_env["HOME"] = str(tmp_path)
    full_env.update(env or {})

    result = subprocess.run(
        ["bash", CREATE_EC2_SH] + args, cwd=str(tmp_path), env=full_env,
        capture_output=True, text=True, timeout=60)
    calls = calls_file.read_text().splitlines() if calls_file.exists() else []
    return result, calls


# ---------------------------------------------------------------------------
# Required account-identifying values
# ---------------------------------------------------------------------------
def test_dry_run_fails_clearly_when_a_required_value_is_missing(tmp_path):
    # Omit --ami-id: every other required flag is present.
    args = ["--dry-run", "--label", "t1"]
    for flag, value in REQUIRED_FLAGS.items():
        if flag != "--ami-id":
            args += [flag, value]

    result, calls = run_create_ec2(tmp_path, args)

    assert result.returncode != 0
    assert "--ami-id" in result.stderr
    assert "SOAK_EC2_AMI_ID" in result.stderr
    # The whole point: no AWS account/credentials should be touched at all.
    assert calls == []


def test_dry_run_fails_clearly_when_all_required_values_are_missing(tmp_path):
    result, calls = run_create_ec2(tmp_path, ["--dry-run", "--label", "t1"])

    assert result.returncode != 0
    for flag in REQUIRED_FLAGS:
        assert flag in result.stderr
    assert calls == []


def test_dry_run_succeeds_when_all_required_values_are_supplied_via_flags(
        tmp_path):
    args = ["--dry-run", "--label", "t1"]
    for flag, value in REQUIRED_FLAGS.items():
        args += [flag, value]

    result, calls = run_create_ec2(tmp_path, args)

    assert result.returncode == 0, result.stderr
    run_instances_calls = [c for c in calls if c.startswith("ec2 run-instances")]
    assert len(run_instances_calls) == 1
    assert "--image-id ami-fake123" in run_instances_calls[0]
    assert "--subnet-id subnet-fake123" in run_instances_calls[0]
    assert "--security-group-ids sg-fake123" in run_instances_calls[0]
    assert "--iam-instance-profile Name=fake-role" in run_instances_calls[0]


def test_required_values_can_come_from_environment_variables(tmp_path):
    env = {
        "SOAK_EC2_SUBNET_ID": "subnet-fromenv",
        "SOAK_EC2_SECURITY_GROUP_ID": "sg-fromenv",
        "SOAK_EC2_IAM_PROFILE": "role-fromenv",
        "SOAK_EC2_AMI_ID": "ami-fromenv",
    }
    result, calls = run_create_ec2(tmp_path, ["--dry-run", "--label", "t1"], env)

    assert result.returncode == 0, result.stderr
    run_instances_calls = [c for c in calls if c.startswith("ec2 run-instances")]
    assert len(run_instances_calls) == 1
    assert "--image-id ami-fromenv" in run_instances_calls[0]
    assert "--subnet-id subnet-fromenv" in run_instances_calls[0]


def test_required_values_can_come_from_a_local_env_file(tmp_path):
    """create-ec2.env next to the script is sourced automatically, mirroring
    ccloud.config's "copy the .example, fill in real values" pattern."""
    env_file = os.path.join(SOAK_DIR, "create-ec2.env")
    assert not os.path.exists(env_file), (
        "a real create-ec2.env exists in the working tree; refusing to "
        "clobber it for a test")
    with open(env_file, "w") as fh:
        fh.write(
            'export SOAK_EC2_SUBNET_ID="subnet-fromfile"\n'
            'export SOAK_EC2_SECURITY_GROUP_ID="sg-fromfile"\n'
            'export SOAK_EC2_IAM_PROFILE="role-fromfile"\n'
            'export SOAK_EC2_AMI_ID="ami-fromfile"\n')
    try:
        result, calls = run_create_ec2(tmp_path, ["--dry-run", "--label", "t1"])
        assert result.returncode == 0, result.stderr
        run_instances_calls = [c for c in calls
                                if c.startswith("ec2 run-instances")]
        assert len(run_instances_calls) == 1
        assert "--image-id ami-fromfile" in run_instances_calls[0]
    finally:
        os.remove(env_file)


def test_terminate_does_not_require_the_account_identifying_values(tmp_path):
    result, calls = run_create_ec2(tmp_path, ["--terminate", "i-0123456789abcdef0"])

    assert result.returncode == 0, result.stderr
    assert any(c.startswith("ec2 terminate-instances") for c in calls)


# ---------------------------------------------------------------------------
# --label validation
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("label", ["bad,label", "bad label", "bad;label",
                                    "bad=label", "bad{label}"])
def test_invalid_label_is_rejected_before_any_aws_call(tmp_path, label):
    args = ["--dry-run", "--label", label]
    for flag, value in REQUIRED_FLAGS.items():
        args += [flag, value]

    result, calls = run_create_ec2(tmp_path, args)

    assert result.returncode != 0
    assert "--label" in result.stderr
    assert calls == []


@pytest.mark.parametrize("label", ["njc-rust-soak-tests", "test_2", "Abc123"])
def test_valid_labels_are_accepted(tmp_path, label):
    args = ["--dry-run", "--label", label]
    for flag, value in REQUIRED_FLAGS.items():
        args += [flag, value]

    result, _calls = run_create_ec2(tmp_path, args)

    assert result.returncode == 0, result.stderr
