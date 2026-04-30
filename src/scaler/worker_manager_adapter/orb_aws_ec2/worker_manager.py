import asyncio
import logging
import math
import os
from typing import Any, List, Optional

try:
    import boto3
    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError('execute "pip install opengris-scaler[orb]" to use ORB AWS EC2 worker Manager') from exc

from scaler.config.section.orb_aws_ec2_worker_adapter import ORBAWSEC2WorkerAdapterConfig
from scaler.protocol.capnp import WorkerManagerCommand, WorkerManagerCommandResponse
from scaler.utility.event_loop import register_event_loop, run_task_forever
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.common import extract_desired_count, format_capabilities
from scaler.worker_manager_adapter.mixins import DeclarativeWorkerProvisioner
from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner

Status = WorkerManagerCommandResponse.Status

ORB_AWS_EC2_POLLING_INTERVAL_SECONDS = 5
ORB_AWS_EC2_MAX_POLLING_ATTEMPTS = 60


class ORBWorkerProvisioner(DeclarativeWorkerProvisioner):
    def __init__(
        self,
        config: ORBAWSEC2WorkerAdapterConfig,
        max_instances: int,
        sdk: Any,
        template_id: str,
        workers_per_instance: int,
    ) -> None:
        self._config = config
        self._max_instances = max_instances
        self._sdk = sdk
        self._template_id = template_id
        self._workers_per_instance = workers_per_instance
        self._units: List[str] = []  # EC2 instance IDs of active units
        self._desired_count: int = 0
        self._reconcile_lock: asyncio.Lock = asyncio.Lock()

        # keeps a strong reference to the running task
        self._active_reconcile_task: Optional[asyncio.Task] = None
        self._pending_reconcile_task: Optional[asyncio.Task] = None

    async def set_desired_task_concurrency(
        self, requests: List[WorkerManagerCommand.DesiredTaskConcurrencyRequest]
    ) -> None:
        own_capabilities = self._config.worker_config.per_worker_capabilities.capabilities
        task_concurrency = extract_desired_count(requests, own_capabilities)
        new_desired = math.ceil(task_concurrency / self._workers_per_instance)
        if new_desired != self._desired_count:
            logging.info(
                f"Desired instance count changed: {self._desired_count} → {new_desired} "
                f"(task_concurrency={task_concurrency}, workers_per_instance={self._workers_per_instance})"
            )
        self._desired_count = new_desired

        # reconciling the ec2 instances can take some time
        # so we launch a task to handle it in the background
        # `_reconcile_lock` ensures only one runs at a time
        if self._pending_reconcile_task is None:
            self._pending_reconcile_task = asyncio.create_task(self._reconcile())

    async def _reconcile(self) -> None:
        async with self._reconcile_lock:
            self._active_reconcile_task = asyncio.current_task()
            self._pending_reconcile_task = None
            try:
                current = len(self._units)
                delta = self._desired_count - current
                if self._max_instances != -1:
                    delta = min(delta, self._max_instances - current)
                capped = self._max_instances != -1 and delta != self._desired_count - current
                msg = f"Reconcile: desired={self._desired_count}, current={current}, delta={delta:+d}" + (
                    f" (capped by max_instances={self._max_instances})" if capped else ""
                )
                if delta != 0:
                    logging.info(msg)
                else:
                    logging.debug(msg)
                if delta > 0:
                    await self.start_units(delta)
                elif delta < 0:
                    await self.stop_units(abs(delta))
            except Exception as exc:
                logging.exception(f"Reconcile failed: {exc}")
            finally:
                self._active_reconcile_task = None

    async def start_units(self, count: int) -> None:
        logging.info(f"Submitting ORB batch machine request for template {self._template_id} (count={count})...")
        create_response = await self._sdk.create_request(template_id=self._template_id, count=count)

        request_id = create_response.get("created_request_id") if isinstance(create_response, dict) else None
        if not request_id:
            raise RuntimeError(f"ORB create_request returned no request ID. Response: {create_response}")

        logging.info(f"ORB request {request_id} submitted, polling for {count} instance ID(s)...")
        timeout_seconds = ORB_AWS_EC2_MAX_POLLING_ATTEMPTS * ORB_AWS_EC2_POLLING_INTERVAL_SECONDS
        elapsed = 0

        while elapsed < timeout_seconds:
            await asyncio.sleep(ORB_AWS_EC2_POLLING_INTERVAL_SECONDS)
            elapsed += ORB_AWS_EC2_POLLING_INTERVAL_SECONDS

            status_response = await self._sdk.get_request_status(request_ids=[request_id])

            requests = status_response.get("requests", []) if isinstance(status_response, dict) else []
            if not requests:
                continue

            req = requests[0] if isinstance(requests[0], dict) else {}
            status = req.get("status", "")
            machine_ids = req.get("machine_ids", [])

            if len(machine_ids) >= count:
                for instance_id in machine_ids:
                    logging.info(f"ORB request {request_id}: instance {instance_id} ready")
                self._units.extend(machine_ids)
                return

            if status.lower() in {"failed", "error", "cancelled", "canceled"}:
                raise RuntimeError(
                    f"ORB request {request_id} reached terminal status '{status}' "
                    f"with {len(machine_ids)}/{count} instances fulfilled."
                )

        raise TimeoutError(
            f"ORB request {request_id} timed out after {timeout_seconds:.0f}s " f"with 0/{count} instances fulfilled."
        )

    async def stop_units(self, count: int) -> None:
        unit_ids = self._units[:count]
        if len(unit_ids) < count:
            logging.warning(f"Requested to stop {count} unit(s) but only {len(unit_ids)} available.")
        if not unit_ids:
            return
        logging.info(f"Stopping {len(unit_ids)} unit(s): instances {unit_ids}")
        await self._sdk.create_return_request(machine_ids=unit_ids)
        del self._units[:count]
        logging.info(f"Successfully stopped {count} unit(s): instances {unit_ids}")

    async def terminate_all_workers(self) -> None:
        if not self._units:
            return
        logging.info(f"Terminating {len(self._units)} unit(s)...")
        try:
            await self._sdk.create_return_request(machine_ids=self._units)
            logging.info(f"Successfully requested termination of instances: {self._units}")
        except Exception as e:
            logging.warning(f"Failed to terminate instances during cleanup: {e}")
        self._units.clear()


class ORBAWSEC2WorkerAdapter:
    def __init__(self, config: ORBAWSEC2WorkerAdapterConfig) -> None:
        self._config = config
        self._worker_scheduler_address = config.worker_manager_config.effective_worker_scheduler_address
        self._event_loop = config.worker_config.event_loop
        self._logging_paths = config.logging_config.paths
        self._logging_level = config.logging_config.level
        self._logging_config_file = config.logging_config.config_file

        self._orb_pool: Optional[ORBWorkerProvisioner] = None
        self._runner: Optional[WorkerManagerRunner] = None

        self._ec2: Optional[Any] = None
        self._created_security_group_id: Optional[str] = None
        self._created_key_name: Optional[str] = None
        self._cleaned_up = False
        self._subnet_id: Optional[str] = None

        if config.image_id is None:
            requirements_content = self._load_requirements_content(config.requirements_txt)
            self._validate_requirements(requirements_content)

    def _build_app_config(self) -> dict:
        region = self._config.aws_region
        return {
            "provider": {
                "selection_policy": "FIRST_AVAILABLE",
                "providers": [
                    {"name": "aws-default", "type": "aws", "enabled": True, "priority": 1, "config": {"region": region}}
                ],
                # ORB skips loading strategy defaults (aws_defaults.json) when config_dict is
                # provided, so provider_defaults must be included explicitly here. Without it,
                # get_effective_handlers() returns {} and RunInstances is not in supported_apis.
                # This may be fixed in a more recent version of the ORB SDK.
                "provider_defaults": {
                    "aws": {
                        "handlers": {
                            "RunInstances": {
                                "handler_class": "RunInstancesHandler",
                                "supports_spot": False,
                                "supports_ondemand": True,
                            }
                        }
                    }
                },
            },
            "storage": {"type": "json"},
        }

    async def _setup(self, sdk: Any) -> None:
        region = self._config.aws_region
        self._ec2 = boto3.client("ec2", region_name=region)
        self._subnet_id = self._config.subnet_id or self._discover_default_subnet()

        workers_per_instance = self._discover_vcpu_count(self._config.instance_type)
        mtc = self._config.worker_manager_config.max_task_concurrency
        max_instances = math.ceil(mtc / workers_per_instance) if mtc != -1 else -1
        logging.info(
            f"ORB instance type {self._config.instance_type!r}: {workers_per_instance} vCPUs/instance, "
            f"max_task_concurrency={mtc} → max_instances={max_instances}"
        )

        template_id = os.urandom(8).hex()

        security_group_ids = self._config.security_group_ids
        if not security_group_ids:
            self._create_security_group(template_id)
            security_group_ids = [self._created_security_group_id]

        key_name = self._config.key_name
        if not key_name:
            self._create_key_pair(template_id)
            key_name = self._created_key_name

        user_data = self._create_user_data()
        image_id = self._config.image_id or self._discover_latest_al2023_ami()

        self._orb_pool = ORBWorkerProvisioner(
            config=self._config,
            max_instances=max_instances,
            sdk=sdk,
            template_id=template_id,
            workers_per_instance=workers_per_instance,
        )
        self._runner = WorkerManagerRunner(
            address=self._config.worker_manager_config.scheduler_address,
            name="worker_manager_orb_aws_ec2",
            heartbeat_interval_seconds=self._config.worker_config.heartbeat_interval_seconds,
            capabilities=self._config.worker_config.per_worker_capabilities.capabilities,
            max_provisioner_units=max_instances,
            worker_manager_id=self._config.worker_manager_config.worker_manager_id.encode(),
            worker_provisioner=self._orb_pool,
            io_threads=self._config.worker_config.io_threads,
            workers_per_provisioner_unit=workers_per_instance,
        )

        create_result = await sdk.create_template(
            template_id=template_id,
            name=f"opengris-orb-{template_id}",
            image_id=image_id,
            provider_api="RunInstances",
            instance_type=self._config.instance_type,
            max_instances=max_instances,
            provider_name="aws-default",
            machine_types={self._config.instance_type: 1},
            subnet_ids=[self._subnet_id],
            security_group_ids=security_group_ids,
            key_name=key_name,
            user_data=user_data,
        )
        logging.info(f"create_template result: {create_result}")

        validate_result = await sdk.validate_template(template_id=template_id)
        logging.info(f"validate_template result: {validate_result}")

    def run(self) -> None:
        self._loop = asyncio.new_event_loop()
        run_task_forever(self._loop, self._run(), cleanup_callback=self._cleanup)

    async def _run(self) -> None:
        register_event_loop(self._event_loop)

        try:
            from orb import ORBClient as orb
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                'execute "pip install opengris-scaler[orb]" to use ORB AWS EC2 worker Manager'
            ) from exc

        async with orb(app_config=self._build_app_config()) as sdk:
            # setup_logger is called after the ORB context is entered because ORB reconfigures
            # the root logger during __aenter__, which would otherwise suppress scaler log output.
            setup_logger(self._logging_paths, self._logging_config_file, self._logging_level)
            await self._setup(sdk)
            try:
                await self._runner.run_in_loop(self._loop)
            except asyncio.CancelledError:
                pass
            finally:
                await self._orb_pool.terminate_all_workers()

    def _cleanup(self) -> None:
        if self._cleaned_up:
            return
        self._cleaned_up = True

        if self._runner is not None:
            self._runner.cleanup()

        logging.info("Starting cleanup of AWS resources...")

        if self._created_security_group_id is not None:
            try:
                logging.info(f"Deleting AWS security group: {self._created_security_group_id}")
                self._ec2.delete_security_group(GroupId=self._created_security_group_id)
            except Exception as e:
                logging.warning(f"Failed to delete security group {self._created_security_group_id}: {e}")

        if self._created_key_name is not None:
            try:
                logging.info(f"Deleting AWS key pair: {self._created_key_name}")
                self._ec2.delete_key_pair(KeyName=self._created_key_name)
            except Exception as e:
                logging.warning(f"Failed to delete key pair {self._created_key_name}: {e}")

        logging.info("Cleanup completed.")

    def __del__(self) -> None:
        self._cleanup()

    def _create_user_data(self) -> str:
        worker_config = self._config.worker_config
        adapter_config = self._config.worker_manager_config

        script = "#!/bin/bash\n"

        if self._config.image_id is None:
            python_version = self._config.python_version
            requirements_txt = self._config.requirements_txt

            requirements_content = self._load_requirements_content(requirements_txt)

            # User data runs as root so no sudo is needed.
            # set -e ensures any install failure aborts the script rather than launching a broken worker.
            script += f"""set -e
dnf update -y
dnf install -y python{python_version} python{python_version}-pip
python{python_version} -m venv /opt/opengris-scaler
/opt/opengris-scaler/bin/python -m pip install --upgrade pip
cat > /tmp/requirements.txt << 'REQUIREMENTS_EOF'
{requirements_content}
REQUIREMENTS_EOF
/opt/opengris-scaler/bin/pip install -r /tmp/requirements.txt
ln -sf /opt/opengris-scaler/bin/scaler_* /usr/local/bin/
set +e

"""

        # --max-task-concurrency is not passed: scaler_worker_manager defaults to cpu_count - 1 workers,
        # where cpu_count is determined by the machine type the user configured in the ORB template.
        script += f"""INSTANCE_ID=$(ec2-metadata --instance-id --quiet)
nohup scaler_worker_manager baremetal_native {self._worker_scheduler_address!r} \\
    --mode fixed \\
    --worker-type ORB \\
    --worker-manager-id "${{INSTANCE_ID}}" \\
    --per-worker-task-queue-size {worker_config.per_worker_task_queue_size} \\
    --heartbeat-interval-seconds {worker_config.heartbeat_interval_seconds} \\
    --task-timeout-seconds {worker_config.task_timeout_seconds} \\
    --garbage-collect-interval-seconds {worker_config.garbage_collect_interval_seconds} \\
    --death-timeout-seconds {worker_config.death_timeout_seconds} \\
    --trim-memory-threshold-bytes {worker_config.trim_memory_threshold_bytes} \\
    --event-loop {self._config.worker_config.event_loop} \\
    --io-threads {self._config.worker_config.io_threads}"""

        if worker_config.hard_processor_suspend:
            script += " \\\n    --hard-processor-suspend"

        if adapter_config.object_storage_address:
            script += f" \\\n    --object-storage-address {adapter_config.object_storage_address!r}"

        capabilities = worker_config.per_worker_capabilities.capabilities
        if capabilities:
            cap_str = format_capabilities(capabilities)
            if cap_str.strip():
                script += f" \\\n    --per-worker-capabilities {cap_str}"

        script += " > /var/log/opengris-scaler.log 2>&1 &\n"

        return script

    def _discover_vcpu_count(self, instance_type: str) -> int:
        response = self._ec2.describe_instance_types(InstanceTypes=[instance_type])
        instance_types = response.get("InstanceTypes", [])
        if not instance_types:
            raise RuntimeError(f"Could not retrieve instance type info for {instance_type!r}.")
        return instance_types[0]["VCpuInfo"]["DefaultVCpus"]

    def _discover_latest_al2023_ami(self) -> str:
        response = self._ec2.describe_images(
            Filters=[
                {"Name": "name", "Values": ["al2023-ami-2023.*-kernel-*-x86_64"]},
                {"Name": "root-device-type", "Values": ["ebs"]},
                {"Name": "virtualization-type", "Values": ["hvm"]},
            ],
            Owners=["amazon"],
        )
        images = response.get("Images", [])
        if not images:
            raise RuntimeError("No AL2023 AMI found in the current region.")
        images.sort(key=lambda img: img["CreationDate"], reverse=True)
        ami_id = images[0]["ImageId"]
        logging.info(f"Auto-discovered latest AL2023 AMI: {ami_id}")
        return ami_id

    def _discover_default_subnet(self) -> str:
        vpcs = self._ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
        if not vpcs["Vpcs"]:
            raise RuntimeError("No default VPC found, and no subnet_id provided.")
        default_vpc_id = vpcs["Vpcs"][0]["VpcId"]

        subnets = self._ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [default_vpc_id]}])
        if not subnets["Subnets"]:
            raise RuntimeError(f"No subnets found in default VPC {default_vpc_id}.")

        subnet_id = subnets["Subnets"][0]["SubnetId"]
        logging.info(f"Auto-discovered subnet_id: {subnet_id}")
        return subnet_id

    def _create_security_group(self, template_id: str) -> None:
        subnet_response = self._ec2.describe_subnets(SubnetIds=[self._subnet_id])
        vpc_id = subnet_response["Subnets"][0]["VpcId"]

        group_name = f"opengris-orb-sg-{template_id}"
        sg_response = self._ec2.create_security_group(
            Description="Temporary security group created for OpenGRIS ORB worker adapter",
            GroupName=group_name,
            VpcId=vpc_id,
        )
        self._created_security_group_id = sg_response["GroupId"]
        logging.info(f"Created security group with ID: {self._created_security_group_id}")

    def _create_key_pair(self, template_id: str) -> None:
        key_name = f"opengris-orb-key-{template_id}"
        self._ec2.create_key_pair(KeyName=key_name)
        self._created_key_name = key_name
        logging.info(f"Created key pair: {key_name}")

    @staticmethod
    def _load_requirements_content(requirements_txt: str) -> str:
        if os.path.isfile(requirements_txt):
            with open(requirements_txt) as f:
                return f.read()
        return requirements_txt

    @staticmethod
    def _validate_requirements(requirements_content: str) -> None:
        found_scaler = False
        for line in requirements_content.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("-"):
                continue
            try:
                req = Requirement(line)
                if canonicalize_name(req.name) == "opengris-scaler":
                    found_scaler = True
            except Exception:
                if "://" not in line:
                    raise ValueError(f"Invalid requirement line that would cause pip to fail: {line!r}")

        if not found_scaler:
            raise ValueError(
                "The requirements file must include the 'opengris-scaler' package. "
                "Workers will fail to start without it."
            )
