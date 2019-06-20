# Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
# See License in the project root for license information.

# Use convenience function to redirect printing to stderr
from cruisecontrolclient.util.print import print_error

# To allow for more precise type hints
from requests import Response

# To print the JSONs
from pprint import pformat

# To be able to show the humans the JSON in tabular form
from pandas import DataFrame, set_option

# To be able to give more-precise type hints
from typing import Dict, Callable  # noqa

# To be able to load a JSON from a string
import json

# To be able to indent the correct level for conveying JSON descent
import textwrap


def get_key_to_display_function() -> Dict[str, Callable]:
    """
    Convenience mapping of the key we see to the function that should parse its contents.

    :return: a mapping of keys to expected functions
    """
    return {
        'AnomalyDetectorState': display_anomaly_detector_state,
        'brokers': display_brokers,
        'goalReadiness': display_goal_readiness,
        'goalSummary': display_goal_summary,
        'hosts': display_hosts,
        'loadAfterOptimization': display_load_after_optimization,
        'KafkaBrokerState': display_kafka_broker_state,
        'KafkaPartitionState': display_kafka_partition_state,
        'progress': display_progress,
        'records': display_records
    }


def print_with_indent(s: str, level=0) -> None:
    """
    Prints the string with a level of indentation

    :param s:
    :param level:
    :return:
    """
    print_error(textwrap.indent(s, "\t" * level))


def display_anomaly_detector_state(AnomalyDetectorState_json: dict, level=0) -> None:
    """
    Displays the 'AnomalyDetectorState' key in a cruise-control response

    :param AnomalyDetectorState_json: the JSON structure referenced by the
            'AnomalyDetectorState' key.
    :return:
    """
    for key in AnomalyDetectorState_json:
        if key == "recentGoalViolations":
            print_with_indent(f"\t'{key}:", level)
            for item in AnomalyDetectorState_json[key]:
                if 'optimizationResult' in item:
                    optimization_json = json.loads(item['optimizationResult'])
                    del item['optimizationResult']
                    print_with_indent(pformat(item), level + 1)
                    print_with_indent("'optimizationResult':", level + 2)
                    display_dict(optimization_json, level + 2)

                else:
                    print_with_indent(pformat(item), level + 1)
        else:
            print_error(f"\t'{key}: {AnomalyDetectorState_json[key]}")


def display_brokers(brokers_list: list, level=0) -> None:
    """
    Displays the 'brokers' key in a cruise-control response

    :param brokers_list: the list structure referenced by the 'brokers' key.
    :return:
    """
    df = DataFrame(brokers_list)
    df.set_index('Broker', inplace=True)
    print_with_indent(df.to_string(), level)


def display_hosts(hosts_list: list, level=0) -> None:
    """
    Displays the 'hosts' key in a cruise-control response

    :param hosts_list: the list structure referenced by the 'hosts' key.
    :return:
    """
    df = DataFrame(hosts_list)
    df.set_index('Host', inplace=True)
    print_with_indent(df.to_string(), level)


def display_load_after_optimization(loadAfterOptimization_json: dict, level=0) -> None:
    """
    Displays the 'loadAfterOptimization' key in a cruise-control response

    :param loadAfterOptimization_json: the JSON structure referenced by the
            'loadAfterOptimization' key.
    :return:
    """
    # Make and print the dataframe from the 'brokers' subset of the JSON
    df = DataFrame(loadAfterOptimization_json['brokers'])
    df.set_index('Broker', inplace=True)
    print_with_indent(df.to_string(), level)


def display_kafka_broker_state(KafkaBrokerState_json: dict, level=0) -> None:
    """
    Displays the 'KafkaBrokerState' key in a cruise-control response

    :param KafkaBrokerState_json: the JSON structure referenced by the
            'KafkaBrokerState' key.
    :return:
    """
    # Handle list-like values, which we expect for 'OnlineLogDirsByBrokerId' and
    # 'OfflineLogDirsByBrokerId'
    log_dir_df = None
    # Items under these keys should be like
    # '33967': []
    # '33967': ['/path/to/only/logdir']
    # '33967': ['/path/to/one/logdir', /path/to/another/logdir', ...]
    if 'OnlineLogDirsByBrokerId' in KafkaBrokerState_json and 'OfflineLogDirsByBrokerId' in KafkaBrokerState_json:

        # Create a dictionary of the comma-joined log-dirs
        broker_id_to_log_dir = {}
        for entry in ('OfflineLogDirsByBrokerId', 'OnlineLogDirsByBrokerId'):
            broker_id_to_log_dir[entry] = {key: ','.join(value) for key, value in
                                           KafkaBrokerState_json[entry].items()}
            # Delete from the original dict, since we later presume int32 type,
            # and need to preserve that assumption
            del KafkaBrokerState_json[entry]

        # Create a DataFrame of this logDir info.
        # We will join this to the main DataFrame once it is constructed.
        log_dir_df: DataFrame = DataFrame(broker_id_to_log_dir)
        log_dir_df.index.name = "BrokerId"

    # KafkaBrokerState_json should contain 'LeaderCountByBrokerId', 'OutOfSyncCountByBrokerId', and
    # 'ReplicaCountByBrokerId', each of which should map '{brokerID}': count
    df: DataFrame = DataFrame(KafkaBrokerState_json, dtype=int)
    df.fillna(0, inplace=True)
    df = df.astype('int32')
    df.index.name = "BrokerId"
    if log_dir_df is not None:
        df = df.join(log_dir_df)
    print_with_indent(df.to_string(), level)


def display_kafka_partition_state(KafkaPartitionState_json: dict, level=0) -> None:
    """
    Displays the 'KafkaPartitionState' key in a cruise-control response

    :param KafkaPartitionState_json: the JSON structure referenced by the
            'KafkaPartitionState' key.
    :return:
    """
    # We expect KafkaPartitionState_json to contain 'offline': [...] and 'urp': [...]
    for key in KafkaPartitionState_json:
        if type(KafkaPartitionState_json[key] == list):
            print_with_indent(f"\t'{key}'", level)
            if KafkaPartitionState_json[key]:
                # Reuse the topic-partition dataframe display function
                display_records(KafkaPartitionState_json[key], level)
            else:
                # If we're here, there's an empty list
                print_with_indent(str(KafkaPartitionState_json[key]), level)


def display_goal_readiness(goalReadiness_list: list, level=0) -> None:
    """
    Displays the 'goalReadiness' key in a cruise-control response.

    Note that this is not a top-level key, but is rather a sub-key of the
    AnalyzerState top-level key.

    :param goalReadiness_list: the list structure referenced by the
            'goalReadiness' key.
    :return:
    """
    for elem in goalReadiness_list:
        print_with_indent(f"{elem['name']}: {elem['status']}", level)
        print_with_indent(pformat(elem['modelCompleteRequirement']))
        print_error()


def display_goal_summary(goalSummary_json: dict, level=0) -> None:
    """
    Displays the 'goalSummary' key in a cruise-control response

    :param goalSummary_json: the JSON structure referenced by the 'goalSummary' key.
    :return:
    """
    # Make and print a dataframe for each of the goals in clusterModelState
    for elem in goalSummary_json:
        print_with_indent(f"{elem['goal']}: {elem['status']}", level)
        df = DataFrame(elem['clusterModelStats']['statistics'])
        print_with_indent(df.to_string(), level)
        print_error()


def display_progress(progress_list: list, level=0) -> None:
    """
    Displays the 'progress' key in a (non-final) cruise-control response

    :param progress_list: the list structure referenced by the 'progress' key.
    :return:
    """
    for elem in progress_list:
        print_error(f"operation: {elem['operation']}")
        df = DataFrame(elem['operationProgress'])
        try:
            df.set_index('step', inplace=True)
        # The dataframe we get back may not have 'step' as a key
        except KeyError:
            pass
        df_string = df.to_string()
        # Indent the dataframe for better display
        df_string = df_string.replace("\n", "\n    ")
        print_with_indent(df_string, level)

    # Print ellipses so the humans know we are waiting :)
    print_error(".........")


def display_records(records_list: list, level=0) -> None:
    """
    Displays the 'records' key in a cruise-control response

    :param records_list: the list structure referenced by the 'records' key.
    :return:
    """
    df = DataFrame(records_list)
    df.set_index(['topic', 'partition'], inplace=True)
    df.sort_index(inplace=True)
    print_with_indent(df.to_string(), level)


def display_response(response: Response) -> None:
    """
    Handles setting the display options and extracting the dict from the response,
    then beginning the (possibly recursive) display parsing.

    :param response:
    :return:
    """
    # Set pandas display options
    # Display floats with a ',' 1000s separator, to two decimal places
    set_option('display.float_format', "{:,.2f}".format)
    # Display all rows and columns in the dataframe.  Don't leave any out
    set_option('display.max_rows', int(1E12))
    set_option('display.max_columns', int(1E12))
    # Display the full column width, even if it's very wide.  Don't truncate it.
    set_option('display.max_colwidth', 10000)
    # Don't wrap the table we show
    set_option('display.expand_frame_repr', False)
    # Don't 'sparsify' sorted multi-index dataframes, since
    # sparsifying makes it harder to do bash-type processing of tabular data
    set_option('display.multi_sparse', False)

    j: dict = response.json()

    try:
        display_dict(j)
    except Exception as e:
        print_error(f"Unhandled exception during display; showing raw JSON", e)
        print_error(pformat(j))


def display_dict(j: dict, level=0) -> None:
    """
    Walks the cruise-control Response dict and attempts to display
    the given top-level keys to the humans.


    :param j:
    :return:
    """
    for key in j.keys():
        print_with_indent(f"'{key}':", level)
        # Display the contents of this key with its key-specific function, if possible.
        key_to_display_function = get_key_to_display_function()
        if key in key_to_display_function:
            key_to_display_function[key](j[key], level)
        # Otherwise, just pretty-print this key's contents.
        else:
            print_with_indent(pformat(j[key]), level)
        print_error()
