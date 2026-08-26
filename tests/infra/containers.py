from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path

from docker.errors import DockerException
from testcontainers.core.container import DockerContainer
from testcontainers.core.exceptions import ContainerStartException
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import HttpWaitStrategy

SERVICEBUS_IMAGE = "mcr.microsoft.com/azure-messaging/servicebus-emulator@sha256:5a96d893b245031740f7d46e0fe5ff282d24b78c4b7d761dd57590f3f010a9b3"
SQL_EDGE_IMAGE = "mcr.microsoft.com/azure-sql-edge@sha256:902628a8be89e35dfb7895ca31d602974c7bafde4d583a0d0873844feb1c42cf"
SQL_PASSWORD = "Emulator!Passw0rd"  # pragma: allowlist secret


class ServiceBusEmulator:
    """Own the containers required by the Service Bus emulator."""

    def __init__(self) -> None:
        self._stack = ExitStack()
        self._sql: DockerContainer | None = None
        self._servicebus: DockerContainer | None = None
        self.connection_string: str | None = None

    def start(self) -> ServiceBusEmulator:
        try:
            network = self._stack.enter_context(Network())

            self._sql = DockerContainer(SQL_EDGE_IMAGE).with_envs(
                ACCEPT_EULA="Y",
                MSSQL_SA_PASSWORD=SQL_PASSWORD,
            )
            self._sql.with_network(network).with_network_aliases("sqledge")
            self._sql.with_kwargs(security_opt=["no-new-privileges:true"])
            self._stack.enter_context(self._sql)

            config_path = Path(__file__).with_name("servicebus-config.json")
            self._servicebus = DockerContainer(SERVICEBUS_IMAGE).with_envs(
                SQL_SERVER="sqledge",
                MSSQL_SA_PASSWORD=SQL_PASSWORD,
                ACCEPT_EULA="Y",
                SQL_WAIT_INTERVAL="10",
                EMULATOR_HTTP_PORT="5300",
            )
            self._servicebus.with_network(network)
            self._servicebus.with_volume_mapping(
                config_path,
                "/ServiceBus_Emulator/ConfigFiles/Config.json",
                mode="ro",
            )
            self._servicebus.with_exposed_ports(5672, 5300)
            self._servicebus.with_kwargs(security_opt=["no-new-privileges:true"])
            self._servicebus.waiting_for(
                HttpWaitStrategy(5300, "/health")
                .for_status_code(200)
                .with_startup_timeout(180),
            )
            self._stack.enter_context(self._servicebus)
        except BaseException as exc:
            logs = self.logs()
            if logs and hasattr(exc, "add_note"):
                exc.add_note(logs)
            self.stop()
            raise

        host = self._servicebus.get_container_host_ip()
        port = self._servicebus.get_exposed_port(5672)
        self.connection_string = (
            f"Endpoint=sb://{host}:{port};"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=SAS_KEY_VALUE;"  # pragma: allowlist secret
            "UseDevelopmentEmulator=true;"
        )
        return self

    def stop(self) -> None:
        self._stack.close()

    def logs(self) -> str:
        sections = []
        for name, container in (
            ("Service Bus emulator", self._servicebus),
            ("SQL Edge", self._sql),
        ):
            if container is None:
                continue
            try:
                stdout, stderr = container.get_logs()
            except (ContainerStartException, DockerException):
                continue
            output = (stdout + stderr).decode(errors="replace").strip()
            if output:
                sections.append(f"--- {name} logs ---\n{output}")
        return "\n".join(sections)
