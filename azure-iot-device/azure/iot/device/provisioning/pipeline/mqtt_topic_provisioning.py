# --------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import logging
import six.moves.urllib as urllib

logger = logging.getLogger(__name__)


def _get_topic_base():
    """
    return the string that creates the beginning of all topics for DPS
    """
    return "$dps/registrations/"


def get_register_topic_for_subscribe():
    """
    :return: The topic string used to subscribe for receiving future responses from DPS.
    It is of the format "$dps/registrations/res/#"
    """
    return _get_topic_base() + "res/#"


def get_register_topic_for_publish(method, request_id):
    """
    :return: The topic string used to send a registration. It is of the format
    "$dps/registrations/<method>/iotdps-register/?$rid=<request_id>
    """
    return (_get_topic_base() + "{method}/iotdps-register/?$rid={request_id}").format(
        method=method, request_id=urllib.parse.quote_plus(request_id)
    )


def get_query_topic_for_publish(method, request_id, operation_id):
    """
    :return: The topic string used to send a query. It is of the format
    "$dps/registrations/<method>/iotdps-get-operationstatus/?$rid=<request_id>&operationId=<operation_id>
    """
    return (
        _get_topic_base()
        + "{method}/iotdps-get-operationstatus/?$rid={request_id}&operationId={operation_id}"
    ).format(
        method=method,
        request_id=urllib.parse.quote_plus(request_id),
        operation_id=urllib.parse.quote_plus(operation_id),
    )


def _get_topic_for_response():
    """
    return the topic string used to publish telemetry
    """
    return _get_topic_base() + "res/"


def is_dps_response_topic(topic):
    """
    Topics for responses from DPS are of the following format:
    $dps/registrations/res/<statuscode>/?$<key1>=<value1>&<key2>=<value2>...&<keyN>=<valueN>
    :param topic: The topic string
    """
    if _get_topic_for_response() in topic:
        return True
    return False


def extract_properties_from_dps_response_topic(topic):
    """
    Topics for responses from DPS are of the following format:
    $dps/registrations/res/<statuscode>/?$<key1>=<value1>&<key2>=<value2>...&<keyN>=<valueN>
    Extract key=value pairs from the latter part of the topic.
    :param topic: The topic string
    :return: a dictionary of property keys mapped to a list of property values.
    """
    topic_parts = topic.split("$")
    key_value_dict = urllib.parse.parse_qs(topic_parts[2])
    for k, v in key_value_dict.items():
        if len(v) > 1:
            raise ValueError("Duplicate keys in DPS response topic")
        else:
            key_value_dict[k] = v[0]
    return key_value_dict


def extract_status_code_from_dps_response_topic(topic):
    """
    Topics for responses from DPS are of the following format:
    $dps/registrations/res/<statuscode>/?$<key1>=<value1>&<key2>=<value2>...&<keyN>=<valueN>
    Extract the status code part from the topic.
    :param topic: The topic string
    :return: The status code from the DPS response topic, as a string
    """
    POS_STATUS_CODE_IN_TOPIC = 3
    topic_parts = topic.split("$")
    url_parts = topic_parts[1].split("/")
    status_code = url_parts[POS_STATUS_CODE_IN_TOPIC]
    return status_code


def get_optional_element(content, element_name, index=0):
    """
    Gets an optional element from json string , or dictionary.
    :param content: The content from which the element needs to be retrieved.
    :param element_name: The name of the element
    :param index: Optional index in case the return is a collection of elements.
    """
    element = None if element_name not in content else content[element_name]
    if element is None:
        return None
    else:
        if isinstance(element, list):
            return element[index]
        elif isinstance(element, object):
            return element
        else:
            return str(element)
