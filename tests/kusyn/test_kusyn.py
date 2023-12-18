import pathlib
import queue
import tarfile
from functools import partial

import watchdog.events

from kusyn.kusyn import (
    find_all_sources,
    create_transport_tar,
    convert_absolute_path_to_src_dest,
    drain_watchdog_event_queue,
    KusynConfig,
)

TEST_DATA_FOLDER = pathlib.Path(__file__).parent.joinpath("test_data")


def test_find_all_sources():
    sources = find_all_sources(pathlib.Path(__file__).parent.joinpath("test_data/Dockerfile"))
    assert sources == {
        "single_source": "/dest",
        "source1": "/dest_multy/",
        "source_with_type.lock": "/dest_multy/",
        ".hidden_source": "/dest_multy/",
        "source_folder/": "/dest_multy/",
        "more_sources/": "/dest3",
    }



def test_create_transport_tar():
    file_to_folder = "test_file_to_folder"
    dest_file_to_folder = "/dest1/test_file_to_folder"
    file_to_file = "test_file_to_file"
    dest_file_to_file = "/dest2/some_file"
    ignore_path = "/ignore"
    tar = create_transport_tar(
        src_dest={
            TEST_DATA_FOLDER / file_to_folder: pathlib.Path(dest_file_to_folder),
            TEST_DATA_FOLDER / file_to_file: pathlib.Path(dest_file_to_file),
            TEST_DATA_FOLDER / "test_file_does_not_exists": pathlib.Path(ignore_path),
        }
    )
    tar.seek(0)

    def verify_file(tar, original_name, archive_name):
        decode_from_archive = tar.extractfile(archive_name.strip('/')).read().decode('UTF-8')
        with open(TEST_DATA_FOLDER / original_name) as original:
            assert decode_from_archive == original.read()

    with tarfile.open(fileobj=tar, mode="r") as tar:
        verify_file(tar, file_to_folder, dest_file_to_folder)
        verify_file(tar, file_to_file, dest_file_to_file)
        for m in tar:
            assert not ignore_path.strip('/') in m.path


def test_convert_absolute_path_to_src_dest():
    test_files = [
        str(TEST_DATA_FOLDER / "test_file_to_folder"),
        str(TEST_DATA_FOLDER / "test_file_to_file"),
        str(TEST_DATA_FOLDER / "test/deep/folder/test_file_deep_in_folders"),
        str(TEST_DATA_FOLDER / "another_test/deep"),
        str(TEST_DATA_FOLDER / "another_test/deep/some_file"),
        str(TEST_DATA_FOLDER / "another_test/with_slash/"),
    ]
    src_dest = convert_absolute_path_to_src_dest(
        TEST_DATA_FOLDER,
        src_dest={
            "test_file_to_file": "/dest1/some_file",
            "test_file_to_folder": "/dest2/",
            "test": "/dest3",
            "another_test/deep": "/dest/deep",
            "another_test/with_slash/": "/dest_with_slash",
            "test_file42": "/dest42",
        },
        abs_paths=test_files,
    )
    assert src_dest == {
        TEST_DATA_FOLDER / "test_file_to_file": pathlib.Path("/dest1/some_file"),
        TEST_DATA_FOLDER / "test_file_to_folder": pathlib.Path("/dest2/test_file_to_folder"),
        TEST_DATA_FOLDER
        / "test/deep/folder/test_file_deep_in_folders": pathlib.Path("/dest3/deep/folder/test_file_deep_in_folders"),
        TEST_DATA_FOLDER / "another_test/deep": pathlib.Path("/dest/deep"),
        TEST_DATA_FOLDER / "another_test/deep/some_file": pathlib.Path("/dest/deep/some_file"),
        TEST_DATA_FOLDER / "another_test/with_slash/": pathlib.Path("/dest_with_slash"),
    }


def test_drain_watchdog_event_queue():
    q = queue.Queue()

    def action(original, copy_to):
        copy_to.update(original)

    send_dict = {}
    send_action = partial(action, copy_to=send_dict)
    remove_dict = {}
    remove_action = partial(action, copy_to=remove_dict)

    # files
    q.put(watchdog.events.FileCreatedEvent("test/file_create"))
    q.put(watchdog.events.FileModifiedEvent("test/file_modify"))
    q.put(watchdog.events.FileDeletedEvent("test/file_delete"))
    q.put(watchdog.events.FileMovedEvent("test/file_move", "test/move/file_move"))
    # dirs
    q.put(watchdog.events.DirCreatedEvent("test/dir_create"))
    q.put(watchdog.events.DirModifiedEvent("test/dir_modify"))
    q.put(watchdog.events.DirDeletedEvent("test/dir_delete"))
    q.put(watchdog.events.DirMovedEvent("test/dir_move", "test/move/dir_move"))
    # ignore
    q.put(watchdog.events.FileCreatedEvent("ignore/file_ignore1"))
    q.put(watchdog.events.FileModifiedEvent("ignore/file_ignore2"))
    q.put(watchdog.events.FileDeletedEvent("ignore/file_ignore3"))
    drain_watchdog_event_queue(q, TEST_DATA_FOLDER, {"test": "/dest"}, send_action, remove_action)

    assert send_dict == {
        pathlib.Path("test/file_create"): pathlib.Path("/dest/file_create"),
        pathlib.Path("test/file_modify"): pathlib.Path("/dest/file_modify"),
        pathlib.Path("test/move/file_move"): pathlib.Path("/dest/move/file_move"),
        pathlib.Path("test/dir_create"): pathlib.Path("/dest/dir_create"),
        pathlib.Path("test/dir_modify"): pathlib.Path("/dest/dir_modify"),
        pathlib.Path("test/move/dir_move"): pathlib.Path("/dest/move/dir_move"),
    }
    assert remove_dict == {
        pathlib.Path("test/file_delete"): pathlib.Path("/dest/file_delete"),
        pathlib.Path("test/file_move"): pathlib.Path("/dest/file_move"),
        pathlib.Path("test/dir_delete"): pathlib.Path("/dest/dir_delete"),
        pathlib.Path("test/dir_move"): pathlib.Path("/dest/dir_move"),
    }


def test_kusyn_config():
    config = KusynConfig.parse_from_cli_and_conf_file(
        [str(__file__),
         '-f', str(TEST_DATA_FOLDER / 'kusyn_test_config.conf'),
         '--garbage', 'some_value',
         '--skip-first-sync',
         ],
        {'USER': 'Spike_Spiegel'}
    )
    assert config.pod_name == 'pod_Spike_Spiegel_name'
    assert config.namespace == 'bebop'
    assert config.skip_first_sync
    assert config.project_root.absolute() == TEST_DATA_FOLDER / 'test_root'
    assert config.pod_configuration_yaml == TEST_DATA_FOLDER / 'test_file_to_file'
    assert config.src_dest == {
        'single_source': '/dest',
        'test/': '/test/',
        'another_file': '/another/dest',
    }


def test_kusyn_config_project_root():
    config = KusynConfig.parse_from_cli_and_conf_file(
        [str(__file__),
         '-f', str(TEST_DATA_FOLDER / 'kusyn_test_config_no_dockerfile.conf'),
         ],
        {}
    )
    assert config.namespace == 'default'
    assert config.context is None
    assert config.skip_first_sync is False
    assert config.project_root == TEST_DATA_FOLDER
    assert config.src_dest == {
        "my": "/dest/mymy"
    }
