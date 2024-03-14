import argparse
import io
import os
import pathlib
import queue
import sys
import tarfile
import time
from typing import Callable, Dict

import kubernetes
import watchdog
import yaml
from kubernetes import client, utils, stream, watch
from pyhocon import ConfigFactory
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


def parse_cli_args(cli_args):
    parser = argparse.ArgumentParser(prog="kusyn", description="sync your local files with a kubernetes pod")
    parser.add_argument("--config-file", '-f', default='kusyn.conf', help="path to kusyn config file")
    parser.add_argument("--context", help="specify your kubernetes context")
    parser.add_argument("--namespace", "-n", default="default", help="kubernetes namespace")
    parser.add_argument("--skip-first-sync", "-s", action="store_true", help="skip initial synchronize")
    parser.add_argument("--pod-exists", "-pe", action="store_true", help="skip initial dev pod lookup")
    args, _ = parser.parse_known_args(cli_args)
    return args


class KusynConfig:
    @staticmethod
    def parse_from_cli_and_conf_file(cli_args, env_vars):
        args = parse_cli_args(cli_args)
        config_file = pathlib.Path(args.config_file).absolute()
        if not config_file.exists():
            raise Exception(f"Can't find config file! You provide {args.config_file} result path {config_file}")

        result_config = ConfigFactory.parse_file(str(config_file.absolute()))
        result_config.config_file = config_file
        if not hasattr(result_config, 'ku') or not hasattr(result_config, 'syn'):
            raise Exception(f"Sections 'ku' and 'syn' in the configuration file {config_file} are mandatory")

        for key, value in vars(args).items():
            if not hasattr(result_config.ku, key):
                result_config.ku[key] = value
        for name, value in result_config.ku.items():
            if isinstance(value, str):
                result_config.ku[name] = value.format(**env_vars)

        result = KusynConfig(config_file, result_config)
        return result

    def __init__(self, parsed_file: pathlib.Path, kusyn_config):
        print("initializing kusyn in working dir ", pathlib.Path(os.curdir).absolute())
        self._config = kusyn_config
        self._dockerfile = None
        if not hasattr(kusyn_config.ku, 'pod_configuration_yaml'):
            raise Exception("Please specify configuration yaml for your development pod (pod_configuration_yaml)")
        syn = kusyn_config.syn.copy()
        syn.pop('dockerfile', None)
        self._src_dest = {}
        for k, v in dict(syn).items():
            self._src_dest[k.strip('"\'')] = v.strip('"\'')
        self._project_root = parsed_file.parent
        self._pod_configuration_yaml = parsed_file.parent.joinpath(kusyn_config.ku.pod_configuration_yaml)
        if not self._pod_configuration_yaml.exists():
            raise Exception(f"Can't find pod configuration yaml!"
                            f" In config {kusyn_config.ku.pod_configuration_yaml}"
                            f" result path {self._pod_configuration_yaml}")

        if hasattr(kusyn_config.syn, 'dockerfile'):
            path_to_dockerfile = pathlib.Path(kusyn_config.config_file).parent.joinpath(kusyn_config.syn.dockerfile)
            if path_to_dockerfile.exists():
                self._dockerfile = path_to_dockerfile
                self._project_root = path_to_dockerfile.parent
                self._src_dest.update(find_all_sources(path_to_dockerfile))
            else:
                raise Exception(f"Dockerfile doesn't exists! {str(path_to_dockerfile)}")

    @property
    def project_root(self) -> pathlib.Path:
        return self._project_root

    @property
    def src_dest(self):
        return self._src_dest

    @src_dest.setter
    def src_dest(self, src_dest):
        self.src_dest = src_dest

    @property
    def context(self):
        return self._config.ku.context

    @property
    def namespace(self):
        return self._config.ku.namespace

    @property
    def skip_first_sync(self):
        return self._config.ku.skip_first_sync

    @property
    def pod_exists(self) -> bool:
        return self._config.ku.pod_exists

    @property
    def pod_name(self):
        return self._config.ku.pod_name

    @property
    def dockerfile(self) -> pathlib.Path:
        return self._dockerfile

    @property
    def pod_configuration_yaml(self):
        return self._pod_configuration_yaml


def find_pod(kube_conn, namespace: str, pod_name: str):
    pods = kube_conn.list_namespaced_pod(namespace=namespace, field_selector='metadata.name=' + pod_name)

    for pod in pods.items:
        if pod.metadata.name == pod_name:
            return pod

    return None


def wait_for_pod_is_running(api_core, namespace, pod_name):
    # waiting for the pod has been created
    w = watch.Watch()
    for event in w.stream(api_core.list_namespaced_pod, namespace=namespace, watch=True):
        pod = event["object"]
        pod_status = pod.status.phase

        if pod.metadata.name == pod_name:
            print(f"Pod {pod.metadata.name} is in phase: {pod_status}")
            if pod_status == "Running":
                print(f"Pod {pod_name} has been created and is now running.")
                break


def find_or_create_pod(api_client, api_core, config: KusynConfig, pod_k8s_configuration_yaml: pathlib.Path):
    pod = find_pod(api_core, config.namespace, config.pod_name)
    if pod:
        print(f"found your pod {config.pod_name} in {config.namespace}")
    else:
        print(f"didn't foun your pod {config.pod_name} in {config.namespace}, creating it...")
        with open(pod_k8s_configuration_yaml) as template:
            env_vars = os.environ.copy()
            env_vars["POD_NAME"] = config.pod_name
            env_vars["NAMESPACE"] = config.namespace
            pod_yaml = template.read().replace("${", "{").format(**env_vars)
            load = yaml.safe_load(pod_yaml)
            utils.create_from_dict(api_client, load, verbose=True, namespace=config.namespace)

            wait_for_pod_is_running(api_core, config.namespace, config.pod_name)


def find_all_sources(dockerfile: pathlib.Path) -> dict[str, str]:
    """
    create a source to destination map off a Dockerfile
    :param dockerfile: Dockerfile of the project
    :return: dict with {source: destination} as it specified in the Dockerfile
    """
    with dockerfile.open("r") as file:
        copys = [line for line in file.read().splitlines() if line.startswith("COPY")]
        src_dest = {}
        for line in copys:
            copy_args = line.split(" ")
            dest = copy_args[-1]
            for src in copy_args[1:-1]:
                if src != "":
                    src_dest[src] = dest
        return src_dest


def create_transport_tar(src_dest: dict[pathlib.Path, pathlib.Path]) -> io.BytesIO:
    result_tar = io.BytesIO()
    with tarfile.open(fileobj=result_tar, mode="w:tar") as tar:
        for source, dest in src_dest.items():
            # watchdog for, some reason, can send a FileModifiedEvent before a FileDeletedEvent O_o
            if source.exists():
                print(f"prepare for transfer {source} => {dest}")
                tar.add(source, arcname=dest)
            else:
                print(f"skip file (doesn't exist) {source}")
    result_tar.seek(0)
    # todo maybe we may have a flag to save the tar file for debug purposes
    return result_tar


def send_tar_to_pod(kube_conn: client.CoreV1Api,
                    namespace: str,
                    pod_name: str,
                    transport_tar: io.BytesIO,
                    max_chunk_size=1024 * 10):
    byte_array = transport_tar.getvalue()
    commands = [byte_array[i:i + max_chunk_size] for i in range(0, len(byte_array), max_chunk_size)]

    # Copying file
    exec_command = ["tar", "xvf", "-", "-C", "/"]
    resp = stream.stream(
        kube_conn.connect_get_namespaced_pod_exec,
        pod_name,
        namespace,
        command=exec_command,
        stderr=True,
        stdin=True,
        stdout=True,
        tty=False,
        _preload_content=False,
    )

    while resp.is_open():
        resp.update()
        if commands:
            c = commands.pop(0)
            resp.write_stdin(c)
        else:
            break
    resp.close()


def remove_files_from_pod(
        kube_conn: client.CoreV1Api, namespace: str, pod_name: str, src_dest: dict[pathlib.Path, pathlib.Path]):
    remove_files = [str(dest.absolute()) for dest in src_dest.values()]
    if len(remove_files) != 0:
        rm_cmd = "rm -rf " + " ".join(remove_files)
        print(f"remove files with a cmd:\n{rm_cmd}")
        exec_command = ["/bin/sh", "-c", rm_cmd]
        print(exec_command)
        stream.stream(
            kube_conn.connect_get_namespaced_pod_exec,
            pod_name,
            namespace,
            command=exec_command,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
        )


def convert_absolute_path_to_src_dest(
        project_root: pathlib.Path,
        src_dest: dict[str, str],
        changed_files_abs_paths: [str]) -> dict[pathlib.Path, pathlib.Path]:
    result = {}
    for abs_path in changed_files_abs_paths:
        path = abs_path.replace(str(project_root.absolute()), "").strip("/")
        for source, dest in src_dest.items():
            if path.startswith(source.strip("/")):
                key = pathlib.Path(abs_path)
                if dest.endswith("/") and not source.startswith("/"):
                    result[key] = pathlib.Path(dest).joinpath(source)
                else:
                    # for cases like in Dockerfile 'COPY src/ app'
                    # and we caught a modification event for a file /abs/path/src/hello/kitty.py
                    sub_path = path[len(source.lstrip("/")):]
                    sub_path = sub_path if not sub_path.startswith("/") else sub_path[1:]
                    result[key] = pathlib.Path(dest).joinpath(sub_path)
                break

    return result


class FileSyncHandler(FileSystemEventHandler):
    def __init__(self, files_queue):
        super().__init__()
        self.queue = files_queue

    def on_moved(self, event):
        self.queue.put(event)

    def on_created(self, event):
        self.queue.put(event)

    def on_deleted(self, event):
        self.queue.put(event)

    def on_modified(self, event):
        if event.is_directory:
            return

        self.queue.put(event)


def drain_watchdog_event_queue(
        files_queue: queue.Queue,
        project_root: pathlib.Path,
        src_dest: dict[str, str],
        send_files_action: Callable[[Dict[pathlib.Path, pathlib.Path]], None],
        remove_files_action: Callable[[Dict[pathlib.Path, pathlib.Path]], None],
):
    send_files = set[str]()
    remove_files = set[str]()

    def handle_event(e):
        # todo configure it
        if e.src_path.endswith("~") and e.src_path.endswith(".swp") and e.src_path.endswith("4913"):
            return
        if e.event_type in [watchdog.events.EVENT_TYPE_CREATED, watchdog.events.EVENT_TYPE_MODIFIED]:
            send_files.add(e.src_path)
        if e.event_type == watchdog.events.EVENT_TYPE_MOVED:
            remove_files.add(e.src_path)
            send_files.add(e.dest_path)
        if e.event_type == watchdog.events.EVENT_TYPE_DELETED:
            remove_files.add(e.src_path)

    handle_event(files_queue.get())
    time.sleep(0.1)
    for i in range(0, files_queue.qsize()):
        handle_event(files_queue.get())
    print("\nfound changes in files:")
    for f in send_files:
        print(f"  {f}")
    remove_files_action(convert_absolute_path_to_src_dest(project_root, src_dest, remove_files))
    send_files_action(convert_absolute_path_to_src_dest(project_root, src_dest, send_files))
    files_queue.task_done()


def main():
    print(sys.argv)
    config = KusynConfig.parse_from_cli_and_conf_file(sys.argv, os.environ)

    kubernetes.config.load_kube_config(context=config.context)
    api_client = client.ApiClient()
    api_core = client.CoreV1Api()

    dockerfile = config.dockerfile
    project_root = dockerfile.parent

    print(f"Trying to find your pod {config.pod_name} with a command like...")
    print(f"$ kubectl"
          f" {'--context ' + config.context if config.context else ''}"
          f" {'-n ' + config.namespace if config.namespace else ''}"
          " get pods\n")
    pod_k8s_configuration_yaml = config.pod_configuration_yaml
    if not config.pod_exists:
        find_or_create_pod(api_client, api_core, config, pod_k8s_configuration_yaml)

    directories_to_watch = [str(project_root.absolute().joinpath(src)) for src in config.src_dest.keys()]
    if not config.skip_first_sync:
        print("Perform initial synchronize with the pod (you can disable it with the '--skip_first_sync' flag)")
        transfer_files = convert_absolute_path_to_src_dest(project_root, config.src_dest, directories_to_watch)
        send_tar_to_pod(api_core, config.namespace, config.pod_name, create_transport_tar(transfer_files))
        print("done!")
    else:
        print("Skip the first synchronize with the pod")

    print("\n\nYou can connect to your pod with this command:")
    print(f"$ kubectl"
          f" {'--context ' + config.context if config.context else ''}"
          f" {'-n ' + config.namespace if config.namespace else ''}"
          f" exec -it {config.pod_name} -- /bin/bash")

    print("\n\nNow we start watching your files for the changes...")
    files_queue = queue.Queue()
    event_handler = FileSyncHandler(files_queue)

    observer = Observer()
    for directory in directories_to_watch:
        observer.schedule(event_handler, directory, recursive=True)

    observer.start()

    try:
        while True:
            drain_watchdog_event_queue(
                files_queue,
                project_root,
                config.src_dest,
                lambda d: send_tar_to_pod(api_core, config.namespace, config.pod_name, create_transport_tar(d)),
                lambda d: remove_files_from_pod(api_core, config.namespace, config.pod_name, d),
            )

    except KeyboardInterrupt:
        observer.stop()

    observer.join()


if __name__ == "__main__":
    main()
