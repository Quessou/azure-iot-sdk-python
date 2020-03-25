# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

import pytest
import logging
from azure.iot.device.iothub.pipeline import mqtt_topic_iothub
from azure.iot.device import Message

logging.basicConfig(level=logging.DEBUG)

# NOTE: All tests (that require it) are parametrized with multiple values for URL encoding.
# This is to show that the URL encoding is done correctly - not all URL encoding encodes
# the same way. We must always test the ' ' and '/' characters specifically, in addition
# to a generic URL encoding value (e.g. $, #, etc.)
#
# For URL decoding, we must always test the '+' character speicifically, in addition to
# a generic URL encoding value (e.g. $, #, etc.)
#
# PLEASE DO THESE TESTS FOR EVEN CASES WHERE THOSE CHARACTERS SHOULD NOT OCCUR FOR SAFETY.


@pytest.mark.describe(".get_c2d_topic_for_subscribe()")
class TestGetC2DTopicForSubscribe(object):
    @pytest.mark.it("Returns the topic for subscribing to C2D messages from IoTHub")
    def test_returns_topic(self):
        device_id = "my_device"
        expected_topic = "devices/my_device/messages/devicebound/#"
        topic = mqtt_topic_iothub.get_c2d_topic_for_subscribe(device_id)
        assert topic == expected_topic

    @pytest.mark.it("URL encodes the device_id when generating the topic")
    @pytest.mark.parametrize(
        "device_id, expected_topic",
        [
            pytest.param(
                "my$device", "devices/my%24device/messages/devicebound/#", id="id contains '$'"
            ),
            pytest.param(
                "my device", "devices/my%20device/messages/devicebound/#", id="id contains ' '"
            ),
            pytest.param(
                "my/device", "devices/my%2Fdevice/messages/devicebound/#", id="id contains '/'"
            ),
        ],
    )
    def test_url_encoding(self, device_id, expected_topic):
        topic = mqtt_topic_iothub.get_c2d_topic_for_subscribe(device_id)
        assert topic == expected_topic


@pytest.mark.describe(".get_input_topic_for_subscribe()")
class TestGetInputTopicForSubscribe(object):
    @pytest.mark.it("Returns the topic for subscribing to Input messages from IoTHub")
    def test_returns_topic(self):
        device_id = "my_device"
        module_id = "my_module"
        expected_topic = "devices/my_device/modules/my_module/inputs/#"
        topic = mqtt_topic_iothub.get_input_topic_for_subscribe(device_id, module_id)
        assert topic == expected_topic

    @pytest.mark.it("URL encodes the device_id and module_id when generating the topic")
    @pytest.mark.parametrize(
        "device_id, module_id, expected_topic",
        [
            pytest.param(
                "my$device",
                "my$module",
                "devices/my%24device/modules/my%24module/inputs/#",
                id="ids contain '$'",
            ),
            pytest.param(
                "my device",
                "my module",
                "devices/my%20device/modules/my%20module/inputs/#",
                id="ids contain ' '",
            ),
            pytest.param(
                "my/device",
                "my/module",
                "devices/my%2Fdevice/modules/my%2Fmodule/inputs/#",
                id="ids contain '/'",
            ),
        ],
    )
    def test_url_encoding(self, device_id, module_id, expected_topic):
        topic = mqtt_topic_iothub.get_input_topic_for_subscribe(device_id, module_id)
        assert topic == expected_topic


@pytest.mark.describe(".get_method_topic_for_subscribe()")
class TestGetMethodTopicForSubscribe(object):
    @pytest.mark.it("Returns the topic for subscribing to methods from IoTHub")
    def test_returns_topic(self):
        topic = mqtt_topic_iothub.get_method_topic_for_subscribe()
        assert topic == "$iothub/methods/POST/#"


@pytest.mark.describe("get_twin_response_topic_for_subscribe()")
class TestGetTwinResponseTopicForSubscribe(object):
    @pytest.mark.it("Returns the topic for subscribing to twin repsonse from IoTHub")
    def test_returns_topic(self):
        topic = mqtt_topic_iothub.get_twin_response_topic_for_subscribe()
        assert topic == "$iothub/twin/res/#"


@pytest.mark.describe("get_twin_patch_topic_for_subscribe()")
class TestGetTwinPatchTopicForSubscribe(object):
    @pytest.mark.it("Returns the topic for subscribing to twin patches from IoTHub")
    def test_returns_topic(self):
        topic = mqtt_topic_iothub.get_twin_patch_topic_for_subscribe()
        assert topic == "$iothub/twin/PATCH/properties/desired/#"


@pytest.mark.describe(".get_telemetry_topic_for_publish()")
class TestGetTelemetryTopicForPublish(object):
    @pytest.mark.it("Returns the topic for sending telemetry to IoTHub")
    @pytest.mark.parametrize(
        "device_id, module_id, expected_topic",
        [
            pytest.param("my_device", None, "devices/my_device/messages/events/", id="Device"),
            pytest.param(
                "my_device",
                "my_module",
                "devices/my_device/modules/my_module/messages/events/",
                id="Module",
            ),
        ],
    )
    def test_returns_topic(self, device_id, module_id, expected_topic):
        topic = mqtt_topic_iothub.get_telemetry_topic_for_publish(device_id, module_id)
        assert topic == expected_topic

    @pytest.mark.it("URL encodes the device_id and module_id when generating the topic")
    @pytest.mark.parametrize(
        "device_id, module_id, expected_topic",
        [
            pytest.param(
                "my$device",
                None,
                "devices/my%24device/messages/events/",
                id="Device, id contains '$'",
            ),
            pytest.param(
                "my device",
                None,
                "devices/my%20device/messages/events/",
                id="Device, id contains ' '",
            ),
            pytest.param(
                "my/device",
                None,
                "devices/my%2Fdevice/messages/events/",
                id="Device, id contains '/'",
            ),
            pytest.param(
                "my$device",
                "my$module",
                "devices/my%24device/modules/my%24module/messages/events/",
                id="Module, ids contain '$'",
            ),
            pytest.param(
                "my device",
                "my module",
                "devices/my%20device/modules/my%20module/messages/events/",
                id="Module, ids contain ' '",
            ),
            pytest.param(
                "my/device",
                "my/module",
                "devices/my%2Fdevice/modules/my%2Fmodule/messages/events/",
                id="Module, ids contain '/'",
            ),
        ],
    )
    def test_url_encoding(self, device_id, module_id, expected_topic):
        topic = mqtt_topic_iothub.get_telemetry_topic_for_publish(device_id, module_id)
        assert topic == expected_topic


@pytest.mark.describe(".get_method_topic_for_publish()")
class TestGetMethodTopicForPublish(object):
    @pytest.mark.it("Returns the topic for sending a method response to IoTHub")
    @pytest.mark.parametrize(
        "request_id, status, expected_topic",
        [
            pytest.param("1", "200", "$iothub/methods/res/200/?$rid=1", id="Succesful result"),
            pytest.param(
                "475764", "500", "$iothub/methods/res/500/?$rid=475764", id="Failure result"
            ),
        ],
    )
    def test_returns_topic(self, request_id, status, expected_topic):
        topic = mqtt_topic_iothub.get_method_topic_for_publish(request_id, status)
        assert topic == expected_topic

    @pytest.mark.it("URL encodes provided values when generating the topic")
    @pytest.mark.parametrize(
        "request_id, status, expected_topic",
        [
            pytest.param(
                "invalid#request?id",
                "invalid$status",
                "$iothub/methods/res/invalid%24status/?$rid=invalid%23request%3Fid",
                id="Standard URL Encoding",
            ),
            pytest.param(
                "invalid request id",
                "invalid status",
                "$iothub/methods/res/invalid%20status/?$rid=invalid%20request%20id",
                id="URL Encoding of ' ' character",
            ),
            pytest.param(
                "invalid/request/id",
                "invalid/status",
                "$iothub/methods/res/invalid%2Fstatus/?$rid=invalid%2Frequest%2Fid",
                id="URL Encoding of '/' character",
            ),
        ],
    )
    def test_url_encoding(self, request_id, status, expected_topic):
        topic = mqtt_topic_iothub.get_method_topic_for_publish(request_id, status)
        assert topic == expected_topic


@pytest.mark.describe(".get_twin_topic_for_publish()")
class TestGetTwinTopicForPublish(object):
    @pytest.mark.it("Returns topic for sending a twin request to IoTHub")
    @pytest.mark.parametrize(
        "method, resource_location, request_id, expected_topic",
        [
            # Get Twin
            pytest.param(
                "GET",
                "/",
                "3226c2f7-3d30-425c-b83b-0c34335f8220",
                "$iothub/twin/GET/?$rid=3226c2f7-3d30-425c-b83b-0c34335f8220",
                id="Get Twin",
            ),
            # Patch Twin
            pytest.param(
                "POST",
                "/properties/reported/",
                "5002b415-af16-47e9-b89c-8680e01b502f",
                "$iothub/twin/POST/properties/reported/?$rid=5002b415-af16-47e9-b89c-8680e01b502f",
                id="Patch Twin",
            ),
        ],
    )
    def test_returns_topic(self, method, resource_location, request_id, expected_topic):
        topic = mqtt_topic_iothub.get_twin_topic_for_publish(method, resource_location, request_id)
        assert topic == expected_topic

    @pytest.mark.it("URL encodes 'request_id' parameter")
    @pytest.mark.parametrize(
        "method, resource_location, request_id, expected_topic",
        [
            pytest.param(
                "GET",
                "/",
                "invalid$request?id",
                "$iothub/twin/GET/?$rid=invalid%24request%3Fid",
                id="Get Twin, Standard URL Encoding",
            ),
            pytest.param(
                "GET",
                "/",
                "invalid request id",
                "$iothub/twin/GET/?$rid=invalid%20request%20id",
                id="Get Twin, URL Encoding of ' ' character",
            ),
            pytest.param(
                "GET",
                "/",
                "invalid/request/id",
                "$iothub/twin/GET/?$rid=invalid%2Frequest%2Fid",
                id="Get Twin, URL Encoding of '/' character",
            ),
            pytest.param(
                "POST",
                "/properties/reported/",
                "invalid$request?id",
                "$iothub/twin/POST/properties/reported/?$rid=invalid%24request%3Fid",
                id="Patch Twin, Standard URL Encoding",
            ),
            pytest.param(
                "POST",
                "/properties/reported/",
                "invalid request id",
                "$iothub/twin/POST/properties/reported/?$rid=invalid%20request%20id",
                id="Patch Twin, URL Encoding of ' ' character",
            ),
            pytest.param(
                "POST",
                "/properties/reported/",
                "invalid/request/id",
                "$iothub/twin/POST/properties/reported/?$rid=invalid%2Frequest%2Fid",
                id="Patch Twin, URL Encoding of '/' character",
            ),
        ],
    )
    def test_url_encoding(self, method, resource_location, request_id, expected_topic):
        topic = mqtt_topic_iothub.get_twin_topic_for_publish(method, resource_location, request_id)
        assert topic == expected_topic


@pytest.mark.describe(".is_c2d_topic()")
class TestIsC2DTopic(object):
    @pytest.mark.it(
        "Returns True if the provided topic is a C2D topic and matches the provided device id"
    )
    def test_is_c2d_topic(self):
        topic = "devices/fake_device/messages/devicebound/%24.mid=6b822696-f75a-46f5-8b02-0680db65abf5&%24.to=%2Fdevices%2Ffake_device%2Fmessages%2FdeviceBound&iothub-ack=full"
        device_id = "fake_device"
        assert mqtt_topic_iothub.is_c2d_topic(topic, device_id)

    @pytest.mark.it("URL encodes the device id when matching to the topic")
    @pytest.mark.parametrize(
        "topic, device_id",
        [
            pytest.param(
                "devices/fake%3Fdevice/messages/devicebound/%24.mid=6b822696-f75a-46f5-8b02-0680db65abf5&%24.to=%2Fdevices%2Ffake%3Fdevice%2Fmessages%2FdeviceBound&iothub-ack=full",
                "fake?device",
                id="Standard URL encoding required for device_id",
            ),
            pytest.param(
                "devices/fake%20device/messages/devicebound/%24.mid=6b822696-f75a-46f5-8b02-0680db65abf5&%24.to=%2Fdevices%2Ffake%20device%2Fmessages%2FdeviceBound&iothub-ack=full",
                "fake device",
                id="URL encoding of ' ' character required for device_id",
            ),
            # Note that this topic string is completely broken, even beyond the fact that device id's can't have a '/' in them.
            # A device id with a '/' would not be possible to unencode correctly, because the '/' in the device name encoded in the
            # system properties would cause the system properties to not be able to be decoded correctly. But, like many tests
            # this is just for completeness, safety, and consistency.
            pytest.param(
                "devices/fake%2Fdevice/messages/devicebound/%24.mid=6b822696-f75a-46f5-8b02-0680db65abf5&%24.to=%2Fdevices%2Ffake%2Fdevice%2Fmessages%2FdeviceBound&iothub-ack=full",
                "fake/device",
                id="URL encoding of '/' character required for device_id",
            ),
        ],
    )
    def test_url_encodes(self, topic, device_id):
        assert mqtt_topic_iothub.is_c2d_topic(topic, device_id)

    @pytest.mark.it("Returns False if the provided topic is not a C2D topic")
    @pytest.mark.parametrize(
        "topic, device_id",
        [
            pytest.param("not a topic", "fake_device", id="Not a topic"),
            pytest.param(
                "devices/fake_device/modules/fake_module/inputs/fake_input/%24.mid=6b822696-f75a-46f5-8b02-0680db65abf5&%24.to=%2Fdevices%2Ffake_device%2Fmessages%2FdeviceBound&iothub-ack=full",
                "fake_device",
                id="Topic of wrong type",
            ),
            pytest.param(
                "devices/fake_device/msgs/devicebound/%24.mid=6b822696-f75a-46f5-8b02-0680db65abf5&%24.to=%2Fdevices%2Ffake_device%2Fmessages%2FdeviceBound&iothub-ack=full",
                "fake_device",
                id="Malformed topic",
            ),
        ],
    )
    def test_is_not_c2d_topic(self, topic, device_id):
        assert not mqtt_topic_iothub.is_c2d_topic(topic, device_id)

    @pytest.mark.it(
        "Returns False if the provided topic is a C2D topic, but does not match the provided device id"
    )
    def test_is_c2d_topic_but_wrong_device_id(self):
        topic = "devices/fake_device/messages/devicebound/%24.mid=6b822696-f75a-46f5-8b02-0680db65abf5&%24.to=%2Fdevices%2Ffake_device%2Fmessages%2FdeviceBound&iothub-ack=full"
        device_id = "VERY_fake_device"
        assert not mqtt_topic_iothub.is_c2d_topic(topic, device_id)


# NOTE: The tests in this class don't use entirely realisitic topic strings, as they do not
# contain a message. However, this doesn't affect the tests themselves, because the implementation
# does not look at that part. Still, perhaps it would be wise to enhance these topic strings used.
# (Finding a realistic topic string for input messages requires setting up and debugging through
# an Edge scenario)
@pytest.mark.describe(".is_input_topic()")
class TestIsInputTopic(object):
    @pytest.mark.it(
        "Returns True if the provided topic is an input topic and matches the provided device id and module id"
    )
    def test_is_input_topic(self):
        topic = "devices/fake_device/modules/fake_module/inputs/"
        device_id = "fake_device"
        module_id = "fake_module"
        assert mqtt_topic_iothub.is_input_topic(topic, device_id, module_id)

    @pytest.mark.it("URL encodes the device id and module_id when matching to the topic")
    @pytest.mark.parametrize(
        "topic, device_id, module_id",
        [
            pytest.param(
                "devices/fake%3Fdevice/modules/fake%24module/inputs/",
                "fake?device",
                "fake$module",
                id="Standard URL encoding required for ids",
            ),
            pytest.param(
                "devices/fake%20device/modules/fake%20module/inputs/",
                "fake device",
                "fake module",
                id="URL encoding for ' ' character required for ids",
            ),
            pytest.param(
                "devices/fake%2Fdevice/modules/fake%2Fmodule/inputs/",
                "fake/device",
                "fake/module",
                id="URL encoding for '/' character required for ids",
            ),
        ],
    )
    def test_url_encodes(self, topic, device_id, module_id):
        assert mqtt_topic_iothub.is_input_topic(topic, device_id, module_id)

    @pytest.mark.it("Returns False if the provided topic is not an input topic")
    @pytest.mark.parametrize(
        "topic, device_id, module_id",
        [
            pytest.param("not a topic", "fake_device", "fake_module", id="Not a topic"),
            pytest.param(
                "devices/fake_device/messages/devicebound/%24.mid=6b822696-f75a-46f5-8b02-0680db65abf5&%24.to=%2Fdevices%2Ffake_device%2Fmessages%2FdeviceBound&iothub-ack=full",
                "fake_device",
                "fake_module",
                id="Topic of wrong type",
            ),
            pytest.param(
                "deivces/fake_device/modules/fake_module/inputs/",
                "fake_device",
                "fake_module",
                id="Malformed topic",
            ),
        ],
    )
    def test_is_not_input_topic(self, topic, device_id, module_id):
        assert not mqtt_topic_iothub.is_input_topic(topic, device_id, module_id)

    @pytest.mark.it(
        "Returns False if the provided topic is an input topic, but does match the provided device id and/or module_id"
    )
    @pytest.mark.parametrize(
        "device_id, module_id",
        [
            pytest.param("VERY_fake_device", "fake_module", id="Non-matching device_id"),
            pytest.param("fake_device", "VERY_fake_module", id="Non-matching module_id"),
            pytest.param(
                "VERY_fake_device", "VERY_fake_module", id="Non-matching device_id AND module_id"
            ),
        ],
    )
    def test_is_input_topic_but_wrong_id(self, device_id, module_id):
        topic = "devices/fake_device/modules/fake_module/inputs/"
        assert not mqtt_topic_iothub.is_input_topic(topic, device_id, module_id)


@pytest.mark.describe(".is_method_topic()")
class TestIsMethodTopic(object):
    @pytest.mark.it("Returns True if the provided topic is a method topic")
    def test_is_method_topic(self):
        topic = "$iothub/methods/POST/fake_method/?$rid=1"
        assert mqtt_topic_iothub.is_method_topic(topic)

    @pytest.mark.it("Returns False if the provided topic is not a method topic")
    @pytest.mark.parametrize(
        "topic",
        [
            pytest.param("not a topic", id="Not a topic"),
            pytest.param(
                "devices/fake_device/messages/devicebound/%24.mid=6b822696-f75a-46f5-8b02-0680db65abf5&%24.to=%2Fdevices%2Ffake_device%2Fmessages%2FdeviceBound&iothub-ack=full",
                id="Topic of wrong type",
            ),
            pytest.param("$iothub/mthds/POST/fake_method/?$rid=1", id="Malformed topic"),
        ],
    )
    def test_is_not_method_topic(self, topic):
        assert not mqtt_topic_iothub.is_method_topic(topic)


@pytest.mark.describe(".is_twin_response_topic()")
class TestIsTwinResponseTopic(object):
    @pytest.mark.it("Returns True if the provided topic is a twin response topic")
    def test_is_twin_response_topic(self):
        topic = "$iothub/twin/res/200/?$rid=d9d7ce4d-3be9-498b-abde-913b81b880e5"
        assert mqtt_topic_iothub.is_twin_response_topic(topic)

    @pytest.mark.it("Returns False if the provided topic is not a twin response topic")
    @pytest.mark.parametrize(
        "topic",
        [
            pytest.param("not a topic", id="Not a topic"),
            pytest.param("$iothub/methods/POST/fake_method/?$rid=1", id="Topic of wrong type"),
            pytest.param(
                "$iothub/twin/rs/200/?$rid=d9d7ce4d-3be9-498b-abde-913b81b880e5",
                id="Malformed topic",
            ),
        ],
    )
    def test_is_not_twin_response_topic(self, topic):
        assert not mqtt_topic_iothub.is_twin_response_topic(topic)


# TODO: THESE TESTS
@pytest.mark.describe(".is_twin_desired_property_patch_topic()")
class TestIsTwinDesiredPropertyPatchTopic(object):
    @pytest.mark.it("Returns True if the provided topic is a desired property patch topic")
    def test_is_desired_property_patch_topic(self):
        pass

    @pytest.mark.it("Returns False if the provided topic is not a desired property patch topic")
    def test_is_not_desired_property_patch_topic(self):
        pass


# NOTE: The tests in this class don't use entirely realisitic topic strings, as they do not
# contain a message. However, this doesn't affect the tests themselves, because the implementation
# does not look at that part. Still, perhaps it would be wise to enhance these topic strings used.
# (Finding a realistic topic string for input messages requires setting up and debugging through
# an Edge scenario)
@pytest.mark.describe(".get_input_name_from_topic()")
class TestGetInputNameFromTopic(object):
    @pytest.mark.it("Returns the input name from an input topic")
    def test_valid_input_topic(self):
        topic = "devices/fake_device/modules/fake_module/inputs/fake_input"
        expected_input_name = "fake_input"

        assert mqtt_topic_iothub.get_input_name_from_topic(topic) == expected_input_name

    @pytest.mark.it("URL decodes the returned input name")
    def test_url_decodes_value(self):
        topic = "devices/fake_device/modules/fake_module/inputs/fake%24input"
        expected_input_name = "fake$input"
        assert mqtt_topic_iothub.get_input_name_from_topic(topic) == expected_input_name

    @pytest.mark.it("Raises a ValueError if the provided topic is not an input name topic")
    @pytest.mark.parametrize(
        "topic",
        [
            pytest.param("not a topic", id="Not a topic"),
            pytest.param("$iothub/methods/POST/fake_method/?$rid=1", id="Topic of wrong type"),
            pytest.param("devices/fake_device/inputs/fake_input", id="Malformed topic"),
        ],
    )
    def test_invalid_input_topic(self, topic):
        with pytest.raises(ValueError):
            mqtt_topic_iothub.get_input_name_from_topic(topic)


@pytest.mark.describe(".get_method_name_from_topic()")
class TestGetMethodNameFromTopic(object):
    @pytest.mark.it("Returns the method name from a method topic")
    def test_valid_method_topic(self):
        topic = "$iothub/methods/POST/fake_method/?$rid=1"
        expected_method_name = "fake_method"

        assert mqtt_topic_iothub.get_method_name_from_topic(topic) == expected_method_name

    @pytest.mark.it("URL decodes the returned method name")
    def test_url_decodes_value(self):
        topic = "$iothub/methods/POST/fake%24method/?$rid=1"
        expected_method_name = "fake$method"
        assert mqtt_topic_iothub.get_method_name_from_topic(topic) == expected_method_name

    @pytest.mark.it("Raises a ValueError if the provided topic is not a method topic")
    @pytest.mark.parametrize(
        "topic",
        [
            pytest.param("not a topic", id="Not a topic"),
            pytest.param(
                "devices/fake_device/modules/fake_module/inputs/fake_input",
                id="Topic of wrong type",
            ),
            pytest.param("$iothub/methdos/POST/fake_method/?$rid=1", id="Malformed topic"),
        ],
    )
    def test_invalid_method_topic(self, topic):
        with pytest.raises(ValueError):
            mqtt_topic_iothub.get_method_name_from_topic(topic)


@pytest.mark.describe(".get_method_request_id_from_topic()")
class TestGetMethodRequestIdFromTopic(object):
    @pytest.mark.it("Returns the request id from a method topic")
    def test_valid_method_topic(self):
        topic = "$iothub/methods/POST/fake_method/?$rid=1"
        expected_request_id = "1"

        assert mqtt_topic_iothub.get_method_request_id_from_topic(topic) == expected_request_id

    # NOTE: valid request ids shouldn't need to be URL decoded, but we do it for safety
    # and consistency. As a result, this test covers request_id values that are not valid
    @pytest.mark.it("URL decodes the returned value")
    def test_url_decodes_value(self):
        topic = "$iothub/methods/POST/fake_method/?$rid=fake%24request%2Fid"
        expected_request_id = "fake$request/id"
        assert mqtt_topic_iothub.get_method_request_id_from_topic(topic) == expected_request_id

    @pytest.mark.it("Raises a ValueError if the provided topic is not a method topic")
    @pytest.mark.parametrize(
        "topic",
        [
            pytest.param("not a topic", id="Not a topic"),
            pytest.param(
                "devices/fake_device/modules/fake_module/inputs/fake_input",
                id="Topic of wrong type",
            ),
            pytest.param("$iothub/methdos/POST/fake_method/?$rid=1", id="Malformed topic"),
        ],
    )
    def test_invalid_method_topic(self, topic):
        with pytest.raises(ValueError):
            mqtt_topic_iothub.get_method_request_id_from_topic(topic)


@pytest.mark.describe(".get_twin_request_id_from_topic()")
class TestGetTwinRequestIdFromTopic(object):
    @pytest.mark.it("Returns the request id from a twin response topic")
    def test_valid_twin_response_topic(self):
        topic = "$iothub/twin/res/200/?rid=1"
        expected_request_id = "1"

        assert mqtt_topic_iothub.get_twin_request_id_from_topic(topic) == expected_request_id

    # NOTE: valid request ids shouldn't need to be URL decoded, but we do it for safety
    # and consistency. As a result, this test covers request_id values that are not valid
    @pytest.mark.it("URL decodes the returned value")
    def test_url_decodes_value(self):
        topic = "$iothub/twin/res/200/?rid=fake%24request%2Fid"
        expected_request_id = "fake$request/id"
        assert mqtt_topic_iothub.get_twin_request_id_from_topic(topic) == expected_request_id

    @pytest.mark.it("Raises a ValueError if the provided topic is not a twin response topic")
    @pytest.mark.parametrize(
        "topic",
        [
            pytest.param("not a topic", id="Not a topic"),
            pytest.param(
                "devices/fake_device/modules/fake_module/inputs/fake_input",
                id="Topic of wrong type",
            ),
            pytest.param("$iothub/twn/res/200?rid=1", id="Malformed topic"),
        ],
    )
    def test_invalid_twin_response_topic(self, topic):
        with pytest.raises(ValueError):
            mqtt_topic_iothub.get_twin_request_id_from_topic(topic)


@pytest.mark.describe(".get_twin_status_code_from_topic()")
class TestGetTwinStatusCodeFromTopic(object):
    @pytest.mark.it("Returns the status from a twin response topic")
    def test_valid_twin_response_topic(self):
        topic = "$iothub/twin/res/200/?rid=1"
        expected_status = "200"

        assert mqtt_topic_iothub.get_twin_status_code_from_topic(topic) == expected_status

    @pytest.mark.it("Raises a ValueError if the provided topic is not a twin response topic")
    @pytest.mark.parametrize(
        "topic",
        [
            pytest.param("not a topic", id="Not a topic"),
            pytest.param(
                "devices/fake_device/modules/fake_module/inputs/fake_input",
                id="Topic of wrong type",
            ),
            pytest.param("$iothub/twn/res/200?rid=1", id="Malformed topic"),
        ],
    )
    def test_invalid_twin_response_topic(self, topic):
        with pytest.raises(ValueError):
            mqtt_topic_iothub.get_twin_request_id_from_topic(topic)


# CT-TODO: add input message topic tests for all tests in this class
@pytest.mark.describe(".extract_message_properties_from_topic()")
class TestExtractMessagePropertiesFromTopic(object):
    @pytest.mark.it("Adds properties from topic to Message object")
    @pytest.mark.parametrize(
        "topic, expected_system_properties, expected_custom_properties",
        [
            pytest.param(
                "devices/fake_device/messages/devicebound/%24.mid=6b822696-f75a-46f5-8b02-0680db65abf5&%24.to=%2Fdevices%2Ffake_device%2Fmessages%2Fdevicebound",
                {
                    "mid": "6b822696-f75a-46f5-8b02-0680db65abf5",
                    "to": "/devices/fake_device/messages/devicebound",
                },
                {},
                id="C2D message topic, No optional properties",
            ),
            pytest.param(
                "devices/fake_device/messages/devicebound/%24.exp=3237-07-19T23%3A06%3A40.0000000Z&%24.cid=fake_corid&%24.mid=6b822696-f75a-46f5-8b02-0680db65abf5&%24.to=%2Fdevices%2Ffake_device%2Fmessages%2Fdevicebound&%24.ct=fake_content_type&%24.ce=utf-8",
                {
                    "mid": "6b822696-f75a-46f5-8b02-0680db65abf5",
                    "to": "/devices/fake_device/messages/devicebound",
                    "exp": "3237-07-19T23:06:40.0000000Z",
                    "cid": "fake_corid",
                    "ct": "fake_content_type",
                    "ce": "utf-8",
                },
                {},
                id="C2D message topic, All system properties",
            ),
            pytest.param(
                "devices/fake_device/messages/devicebound/%24.mid=6b822696-f75a-46f5-8b02-0680db65abf5&%24.to=%2Fdevices%2Ffake_device%2Fmessages%2Fdevicebound&custom1=value1&custom2=value2&custom3=value3",
                {
                    "mid": "6b822696-f75a-46f5-8b02-0680db65abf5",
                    "to": "/devices/fake_device/messages/devicebound",
                },
                {"custom1": "value1", "custom2": "value2", "custom3": "value3"},
                id="C2D message topic, Custom properties",
            ),
            # pytest.param("", {}, {}, id="Input message topic, No optional properties"),
            # pytest.param("", {}, {}, id="Input message topic, System properties"),
            # pytest.param("", {}, {}, id="Input message topic, Custom properties"),
        ],
    )
    def test_extracts_properties(
        self, topic, expected_system_properties, expected_custom_properties
    ):
        msg = Message("fake message")
        mqtt_topic_iothub.extract_message_properties_from_topic(topic, msg)

        # Validate MANDATORY system properties
        assert msg.to == expected_system_properties["to"]
        assert msg.message_id == expected_system_properties["mid"]

        # Validate OPTIONAL system properties
        assert msg.correlation_id == expected_system_properties.get("cid", None)
        assert msg.user_id == expected_system_properties.get("uid", None)
        assert msg.content_type == expected_system_properties.get("ct", None)
        assert msg.content_encoding == expected_system_properties.get("ce", None)
        assert msg.expiry_time_utc == expected_system_properties.get("exp", None)

        # Validate custom properties
        assert msg.custom_properties == expected_custom_properties

    @pytest.mark.it("URL decodes properties from the topic when extracting")
    @pytest.mark.parametrize(
        "topic, expected_system_properties, expected_custom_properties",
        [
            pytest.param(
                "devices/fake%24device/messages/devicebound/%24.exp=3237-07-19T23%3A06%3A40.0000000Z&%24.cid=fake%23corid&%24.mid=message%24id&%24.to=%2Fdevices%2Ffake%24device%2Fmessages%2Fdevicebound&%24.ct=fake%23content%24type&%24.ce=utf-%24&custom%2A=value%23&custom%26=value%24&custom%25=value%40",
                {
                    "mid": "message$id",
                    "to": "/devices/fake$device/messages/devicebound",
                    "exp": "3237-07-19T23:06:40.0000000Z",
                    "cid": "fake#corid",
                    "ct": "fake#content$type",
                    "ce": "utf-$",
                },
                {"custom*": "value#", "custom&": "value$", "custom%": "value@"},
                id="C2D message topic, Standard URL decoding",
            )
        ],
    )
    def test_url_decode(self, topic, expected_system_properties, expected_custom_properties):
        msg = Message("fake message")
        mqtt_topic_iothub.extract_message_properties_from_topic(topic, msg)

        # Validate MANDATORY system properties
        assert msg.to == expected_system_properties["to"]
        assert msg.message_id == expected_system_properties["mid"]

        # Validate OPTIONAL system properties
        assert msg.correlation_id == expected_system_properties.get("cid", None)
        assert msg.user_id == expected_system_properties.get("uid", None)
        assert msg.content_type == expected_system_properties.get("ct", None)
        assert msg.content_encoding == expected_system_properties.get("ce", None)
        assert msg.expiry_time_utc == expected_system_properties.get("exp", None)

        # Validate custom properties
        assert msg.custom_properties == expected_custom_properties

    @pytest.mark.it("Ignores 'iothub-ack' property in the topic, and does NOT extract it")
    @pytest.mark.parametrize(
        "topic, expected_custom_properties",
        [
            pytest.param(
                "devices/fake_device/messages/devicebound/%24.mid=6b822696-f75a-46f5-8b02-0680db65abf5&%24.to=%2Fdevices%2Ffake_device%2Fmessages%2Fdevicebound&iothub-ack=full",
                {},
                id="C2D Message Topic",
            )
        ],
    )
    def test_iothub_ack(self, topic, expected_custom_properties):
        msg = Message("fake message")
        mqtt_topic_iothub.extract_message_properties_from_topic(topic, msg)
        assert msg.custom_properties == expected_custom_properties

    @pytest.mark.it(
        "Raises a ValueError if the provided topic is not a c2d topic or an input message topic"
    )
    @pytest.mark.parametrize(
        "topic",
        [
            pytest.param("not a topic", id="Not a topic"),
            pytest.param(
                "$iothub/twin/res/200/?$rid=d9d7ce4d-3be9-498b-abde-913b81b880e5",
                id="Topic of wrong type",
            ),
            pytest.param(
                "devices/fake_device/messages/devicebnd/%24.mid=6b822696-f75a-46f5-8b02-0680db65abf5&%24.to=%2Fdevices%2Ffake_device%2Fmessages%2Fdevicebound",
                id="Malformed C2D topic",
            ),
            # pytest.param("", id="Malformed input message topic")
        ],
    )
    def test_bad_topic(self, topic):
        msg = Message("fake message")
        with pytest.raises(ValueError):
            mqtt_topic_iothub.extract_message_properties_from_topic(topic, msg)
