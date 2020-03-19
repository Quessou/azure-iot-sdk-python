# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import pytest
import logging
from azure.iot.device.provisioning.pipeline import mqtt_topic_provisioning

logging.basicConfig(level=logging.DEBUG)

# NOTE: All tests (that require it) are parametrized with multiple values for URL encoding.
# This is to show that the URL encoding is done correctly - not all URL encoding encodes
# the '+' character. Thus we must make sure any URL encoded value can encode a '+' specifically,
# in addition to standard URL encoding.


@pytest.mark.describe(".get_register_topic_for_subscribe()")
class TestGetRegisterTopicForSubscribe(object):
    @pytest.mark.it("Returns the topic for subscribing to registration responses from DPS")
    def test_returns_topic(self):
        topic = mqtt_topic_provisioning.get_register_topic_for_subscribe()
        assert topic == "$dps/registrations/res/#"


# NOTE: While there is an ability to pivot on the the 'method' parameter of this function, the only
# valid cases are all using the "PUT" method, so it is the only one under test.
# Consider removing this argument.
@pytest.mark.describe(".get_register_topic_for_publish()")
class TestGetRegisterTopicForPublish(object):
    @pytest.mark.it("Returns the topic for publishing registration requests to DPS")
    @pytest.mark.parametrize(
        "request_id, expected_topic",
        [
            # UUID
            pytest.param(
                "3226c2f7-3d30-425c-b83b-0c34335f8220",
                "$dps/registrations/PUT/iotdps-register/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220",
                id="'3226c2f7-3d30-425c-b83b-0c34335f8220' ==> '$dps/registrations/PUT/iotdps-register/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220'",
            ),
            # Arbitrary Value
            pytest.param(
                "1234",
                "$dps/registrations/PUT/iotdps-register/?$rid=1234",
                id="'1234' ==> '$dps/registrations/PUT/iotdps-register/?$rid=1234'",
            ),
        ],
    )
    def test_returns_topic(self, request_id, expected_topic):
        topic = mqtt_topic_provisioning.get_register_topic_for_publish(request_id)
        assert topic == expected_topic

    # NOTE: request_id should NEVER require URL encoding.
    # There are no valid values for which it would require to be URL encoded.
    # However, we encode them anyway for safety.
    # PLEASE NOTE THAT ALL OF THESE TEST CASES ARE INVALID AS A RESULT.
    @pytest.mark.it(
        "URL encodes the request id (even though any request id needing URL encoding is invalid)"
    )
    @pytest.mark.parametrize(
        "request_id, expected_topic",
        [
            # URL Encode
            pytest.param(
                "invalid/request?id",
                "$dps/registrations/PUT/iotdps-register/?$rid=invalid%2Frequest%3Fid",
                id="Regular URL Encoding",
            ),
            # URL Encode (+)
            pytest.param(
                "invalid+request+id",
                "$dps/registrations/PUT/iotdps-register/?$rid=invalid%2Brequest%2Bid",
                id="URL Encoding of '+' character",
            ),
        ],
    )
    def test_url_encoding(self, request_id, expected_topic):
        topic = mqtt_topic_provisioning.get_register_topic_for_publish(request_id)
        assert topic == expected_topic


# # NOTE: While there is an ability to pivot on the the 'method' parameter of this function, the only
# # valid cases are all using the "GET" method, so it is the only one under test.
# # Consider removing this argument.
class TestGetQueryTopicForPublish(object):
    @pytest.mark.it("Returns the topic for publishing query requests to DPS")
    @pytest.mark.parametrize(
        "request_id, operation_id, expected_topic",
        [
            # Realistic values
            pytest.param(
                "3226c2f7-3d30-425c-b83b-0c34335f8220",
                "4.79f33f69d8eb3870.da2d9251-3097-43e9-b81c-782718485ce7",
                "$dps/registrations/GET/iotdps-get-operationstatus/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220&operationId=4.79f33f69d8eb3870.da2d9251-3097-43e9-b81c-782718485ce7",
                id="('3226c2f7-3d30-425c-b83b-0c34335f8220', '4.79f33f69d8eb3870.da2d9251-3097-43e9-b81c-782718485ce7') ==> '$dps/registrations/GET/iotdps-get-operationstatus/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220&operationId=4.79f33f69d8eb3870.da2d9251-3097-43e9-b81c-782718485ce7'",
            ),
            # Arbitrary values
            pytest.param(
                "1234",
                "5678",
                "$dps/registrations/GET/iotdps-get-operationstatus/?$rid=1234&operationId=5678",
                id="('1234', '5678') ==> '$dps/registrations/GET/iotdps-get-operationstatus/?$rid=1234&operationId=5678'",
            ),
        ],
    )
    def test_returns_topic(self, request_id, operation_id, expected_topic):
        topic = mqtt_topic_provisioning.get_query_topic_for_publish(request_id, operation_id)
        assert topic == expected_topic

    # NOTE: request_id and operation_id should NEVER require URL encoding.
    # There are no valid values for which they would require to be URL encoded.
    # However, we encode them anyway for safety.
    # PLEASE NOTE THAT ALL OF THESE TEST CASES ARE INVALID AS A RESULT.
    @pytest.mark.it(
        "URL encodes the request id and operation id (even though values needing URL encoding are invalid)"
    )
    @pytest.mark.parametrize(
        "request_id, operation_id, expected_topic",
        [
            # URL Encode
            pytest.param(
                "invalid/request?id",
                "invalid?operation$id",
                "$dps/registrations/GET/iotdps-get-operationstatus/?$rid=invalid%2Frequest%3Fid&operationId=invalid%3Foperation%24id",
                id="Regular URL Encoding",
            ),
            # URL Encode (+)
            pytest.param(
                "invalid+request+id",
                "invalid+operation+id",
                "$dps/registrations/GET/iotdps-get-operationstatus/?$rid=invalid%2Brequest%2Bid&operationId=invalid%2Boperation%2Bid",
                id="URL Encoding of '+' character",
            ),
        ],
    )
    def test_url_encoding(self, request_id, operation_id, expected_topic):
        topic = mqtt_topic_provisioning.get_query_topic_for_publish(request_id, operation_id)
        assert topic == expected_topic


@pytest.mark.describe(".is_dps_response_topic()")
class TestIsDpsResponseTopic(object):
    @pytest.mark.it("Returns True if the topic is a DPS response topic")
    @pytest.mark.parametrize(
        "topic",
        [
            pytest.param(
                "$dps/registrations/res/200/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220",
                id="Successful (200) response",
            ),
            pytest.param(
                "$dps/registrations/res/202/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220&retry-after=3",
                id="Retry-after (202) response",
            ),
            pytest.param(
                "$dps/registrations/res/401/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220",
                id="Unauthorized (401) response",
            ),
        ],
    )
    def test_is_dps_response_topic(self, topic):
        assert mqtt_topic_provisioning.is_dps_response_topic(topic)

    @pytest.mark.it("Returns False if the topic is not a DPS response topic")
    @pytest.mark.parametrize(
        "topic",
        [
            pytest.param("not a topic", id="Not a topic"),
            pytest.param(
                "$dps/registrations/PUT/iotdps-register/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220",
                id="Topic of wrong type",
            ),
            pytest.param(
                "$dps/resigtrations/res/200/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220",
                id="Malformed topic",
            ),
        ],
    )
    def test_is_not_dps_response_topic(self, topic):
        assert not mqtt_topic_provisioning.is_dps_response_topic(topic)


@pytest.mark.describe(".extract_properties_from_dps_response_topic()")
class TestExtractPropertiesFromDpsResponseTopic(object):
    @pytest.mark.it("Returns the properties from a valid DPS response topic as a dictionary")
    @pytest.mark.parametrize(
        "topic, expected_dict",
        [
            pytest.param(
                "$dps/registrations/res/200/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220",
                {"rid": "3226c2f7-3d30-425c-b83b-0c34335f8220"},
                id="Successful (200) response",
            ),
            pytest.param(
                "$dps/registrations/res/202/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220&retry-after=3",
                {"rid": "3226c2f7-3d30-425c-b83b-0c34335f8220", "retry-after": "3"},
                id="Retry-after (202) response",
            ),
            pytest.param(
                "$dps/registrations/res/401/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220",
                {"rid": "3226c2f7-3d30-425c-b83b-0c34335f8220"},
                id="Unauthorized (401) response",
            ),
            pytest.param(
                "$dps/registrations/res/200/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220&foo=value1&bar=value2&buzz=value3",
                {
                    "rid": "3226c2f7-3d30-425c-b83b-0c34335f8220",
                    "foo": "value1",
                    "bar": "value2",
                    "buzz": "value3",
                },
                id="Arbitrary number of properties in response",
            ),
        ],
    )
    def test_returns_properties(self, topic, expected_dict):
        assert (
            mqtt_topic_provisioning.extract_properties_from_dps_response_topic(topic)
            == expected_dict
        )

    @pytest.mark.it(
        "Raises ValueError if there are duplicate property keys in the DPS response topic"
    )
    def test_duplicate_keys(self):
        topic = "$dps/registrations/res/200/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220&rid=something-else"
        with pytest.raises(ValueError):
            mqtt_topic_provisioning.extract_properties_from_dps_response_topic(topic)


@pytest.mark.describe(".extract_status_code_from_dps_response_topic()")
class TestExtractStatusCodeFromDpsResponseTopic(object):
    @pytest.mark.it("Returns the status code from a valid DPS response topic")
    @pytest.mark.parametrize(
        "topic, expected_status",
        [
            pytest.param(
                "$dps/registrations/res/200/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220",
                "200",
                id="Successful (200) response",
            ),
            pytest.param(
                "$dps/registrations/res/202/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220&retry-after=3",
                "202",
                id="Retry-after (202) response",
            ),
            pytest.param(
                "$dps/registrations/res/401/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220",
                "401",
                id="Unauthorized (401) response",
            ),
        ],
    )
    def test_returns_status(self, topic, expected_status):
        assert (
            mqtt_topic_provisioning.extract_status_code_from_dps_response_topic(topic)
            == expected_status
        )
