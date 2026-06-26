/* OPC UA PubSub MQTT Subscriber – Azure IoT Hub with X.509 Certificate Auth
 *
 * Derived from open62541 tutorial_pubsub_mqtt_subscribe.c (CC0 1.0 Universal).
 *
 * Azure IoT Hub C2D (Cloud-to-Device) topic the subscriber listens on:
 *   devices/<device-id>/messages/devicebound/#
 *
 * Environment variables (required):
 *   AZURE_IOT_HUB_NAME   – e.g. "myhub"
 *   AZURE_DEVICE_ID      – e.g. "my-opc-device"
 *
 * Certificate paths (mounted into the container via docker-compose):
 *   /certs/device.crt
 *   /certs/device.key
 *   /certs/azure_root_ca.pem
 */

#include <open62541/plugin/log_stdout.h>
#include <open62541/server.h>
#include <open62541/server_pubsub.h>
#include <open62541/types.h>

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Transport profile ─────────────────────────────────────────────────── */
#define TRANSPORT_PROFILE_URI \
    "http://opcfoundation.org/UA-Profile/Transport/pubsub-mqtt-json"

/* ── Connection / naming ────────────────────────────────────────────────── */
#define CONNECTION_NAME         "Azure IoT Hub Subscriber"
#define MQTT_CLIENT_ID_PREFIX   "opcua-sub-"

/* ── Certificate paths ─────────────────────────────────────────────────── */
#define DEVICE_CERT_PATH        "/certs/device.crt"
#define DEVICE_KEY_PATH         "/certs/device.key"
#define AZURE_ROOT_CA_PATH      "/certs/azure_root_ca.pem"

/* ── State ──────────────────────────────────────────────────────────────── */
static UA_NodeId connectionIdent;
static UA_NodeId readerGroupIdent;
static UA_NodeId dataSetReaderIdent;

static volatile UA_Boolean running = true;
static void stopHandler(int sig) {
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Caught signal %d", sig);
    running = false;
}

/* ── Helper: read env or die ────────────────────────────────────────────── */
static const char *requireEnv(const char *name) {
    const char *val = getenv(name);
    if(!val || val[0] == '\0') {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                     "Required environment variable %s is not set.", name);
        exit(EXIT_FAILURE);
    }
    return val;
}

static char *buildBrokerUrl(const char *hubName, char *buf, size_t bufSize) {
    snprintf(buf, bufSize, "opc.mqtts://%s.azure-devices.net:8883", hubName);
    return buf;
}

static char *buildMqttUsername(const char *hubName,
                                const char *deviceId,
                                char *buf, size_t bufSize) {
    snprintf(buf, bufSize,
             "%s.azure-devices.net/%s/?api-version=2021-04-12",
             hubName, deviceId);
    return buf;
}

/* C2D subscribe topic */
static char *buildSubscribeTopic(const char *deviceId,
                                  char *buf, size_t bufSize) {
    snprintf(buf, bufSize, "devices/%s/messages/devicebound/#", deviceId);
    return buf;
}

/* ══════════════════════════════════════════════════════════════════════════
 *  PubSub setup functions
 * ══════════════════════════════════════════════════════════════════════════ */

static void addPubSubConnection(UA_Server *server,
                                 const char *hubName,
                                 const char *deviceId) {
    char brokerUrl[256];
    char mqttUsername[256];
    char clientId[128];

    buildBrokerUrl(hubName, brokerUrl, sizeof(brokerUrl));
    buildMqttUsername(hubName, deviceId, mqttUsername, sizeof(mqttUsername));
    snprintf(clientId, sizeof(clientId), "%s%s", MQTT_CLIENT_ID_PREFIX, deviceId);

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                "Subscribing via broker: %s", brokerUrl);

    UA_PubSubConnectionConfig connectionConfig;
    memset(&connectionConfig, 0, sizeof(UA_PubSubConnectionConfig));
    connectionConfig.name = UA_STRING(CONNECTION_NAME);
    connectionConfig.transportProfileUri =
        UA_STRING(TRANSPORT_PROFILE_URI);
    connectionConfig.enabled = UA_TRUE;

    UA_NetworkAddressUrlDataType networkAddressUrl;
    networkAddressUrl.networkInterface = UA_STRING_NULL;
    networkAddressUrl.url              = UA_STRING(brokerUrl);
    UA_Variant_setScalar(&connectionConfig.address, &networkAddressUrl,
                         &UA_TYPES[UA_TYPES_NETWORKSADDRESSURLDATATYPE]);

#define NUM_CONN_OPTIONS 6
    UA_KeyValuePair connectionOptions[NUM_CONN_OPTIONS];
    UA_UInt32 optIdx = 0;

    connectionOptions[optIdx].key = UA_QUALIFIEDNAME(0, "mqttClientId");
    UA_String mqttClientIdStr = UA_STRING(clientId);
    UA_Variant_setScalar(&connectionOptions[optIdx].value,
                         &mqttClientIdStr, &UA_TYPES[UA_TYPES_STRING]);
    optIdx++;

    connectionOptions[optIdx].key = UA_QUALIFIEDNAME(0, "mqttUsername");
    UA_String mqttUsernameStr = UA_STRING(mqttUsername);
    UA_Variant_setScalar(&connectionOptions[optIdx].value,
                         &mqttUsernameStr, &UA_TYPES[UA_TYPES_STRING]);
    optIdx++;

    connectionOptions[optIdx].key = UA_QUALIFIEDNAME(0, "mqttPassword");
    UA_String mqttPasswordStr = UA_STRING_NULL;
    UA_Variant_setScalar(&connectionOptions[optIdx].value,
                         &mqttPasswordStr, &UA_TYPES[UA_TYPES_STRING]);
    optIdx++;

    connectionOptions[optIdx].key = UA_QUALIFIEDNAME(0, "mqttUseTLS");
    UA_Boolean useTLS = UA_TRUE;
    UA_Variant_setScalar(&connectionOptions[optIdx].value,
                         &useTLS, &UA_TYPES[UA_TYPES_BOOLEAN]);
    optIdx++;

    connectionOptions[optIdx].key = UA_QUALIFIEDNAME(0, "mqttCaFilePath");
    UA_String caFileStr = UA_STRING(AZURE_ROOT_CA_PATH);
    UA_Variant_setScalar(&connectionOptions[optIdx].value,
                         &caFileStr, &UA_TYPES[UA_TYPES_STRING]);
    optIdx++;

    connectionOptions[optIdx].key = UA_QUALIFIEDNAME(0, "mqttClientCertPath");
    UA_String clientCertStr = UA_STRING(DEVICE_CERT_PATH);
    UA_Variant_setScalar(&connectionOptions[optIdx].value,
                         &clientCertStr, &UA_TYPES[UA_TYPES_STRING]);
    optIdx++;

    connectionConfig.connectionProperties.mapSize = optIdx;
    connectionConfig.connectionProperties.map     = connectionOptions;

    UA_Server_addPubSubConnection(server, &connectionConfig, &connectionIdent);
}

/* ── ReaderGroup ────────────────────────────────────────────────────────── */
static void addReaderGroup(UA_Server *server) {
    UA_ReaderGroupConfig readerGroupConfig;
    memset(&readerGroupConfig, 0, sizeof(UA_ReaderGroupConfig));
    readerGroupConfig.name = UA_STRING("AzureReaderGroup");
    UA_Server_addReaderGroup(server, connectionIdent,
                             &readerGroupConfig, &readerGroupIdent);
    UA_Server_setReaderGroupOperational(server, readerGroupIdent);
}

/* ── DataSetReader ──────────────────────────────────────────────────────── */
static void addDataSetReader(UA_Server *server, const char *deviceId) {
    char subscribeTopic[256];
    buildSubscribeTopic(deviceId, subscribeTopic, sizeof(subscribeTopic));

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                "Subscribe topic: %s", subscribeTopic);

    UA_DataSetReaderConfig readerConfig;
    memset(&readerConfig, 0, sizeof(UA_DataSetReaderConfig));
    readerConfig.name           = UA_STRING("AzureDataSetReader");
    readerConfig.publisherId.idType = UA_PUBLISHERIDTYPE_STRING;
    readerConfig.publisherId.id.string = UA_STRING_NULL; /* accept any */
    readerConfig.writerGroupId   = 0;   /* accept any */
    readerConfig.dataSetWriterId = 0;   /* accept any */

    /* Transport (subscribe) settings */
    UA_BrokerDataSetReaderTransportDataType brokerTransportSettings;
    memset(&brokerTransportSettings, 0,
           sizeof(UA_BrokerDataSetReaderTransportDataType));
    brokerTransportSettings.queueName = UA_STRING(subscribeTopic);
    brokerTransportSettings.requestedDeliveryGuarantee =
        UA_BROKERTRANSPORTQUALITYOFSERVICE_ATMOSTON;

    UA_ExtensionObject transportSettings;
    UA_ExtensionObject_init(&transportSettings);
    transportSettings.encoding = UA_EXTENSIONOBJECT_DECODED;
    transportSettings.content.decoded.type =
        &UA_TYPES[UA_TYPES_BROKERDATASETREADERTRANSPORTDATATYPE];
    transportSettings.content.decoded.data = &brokerTransportSettings;
    readerConfig.transportSettings          = transportSettings;

    /* Minimal DataSetMetaData so the reader accepts messages */
    UA_DataSetMetaDataType *pMetaData = &readerConfig.dataSetMetaData;
    UA_DataSetMetaDataType_init(pMetaData);
    pMetaData->name = UA_STRING("AzureMetaData");

    UA_FieldMetaData fieldMetaData;
    UA_FieldMetaData_init(&fieldMetaData);
    fieldMetaData.name          = UA_STRING("ServerTime");
    fieldMetaData.builtInType   = UA_NS0ID_DATETIME;
    fieldMetaData.dataType      = UA_NODEID_NUMERIC(0, UA_NS0ID_DATETIME);
    fieldMetaData.valueRank     = -1; /* scalar */

    pMetaData->fields     = &fieldMetaData;
    pMetaData->fieldsSize = 1;

    UA_Server_addDataSetReader(server, readerGroupIdent,
                               &readerConfig, &dataSetReaderIdent);
}

/* ══════════════════════════════════════════════════════════════════════════
 *  main
 * ══════════════════════════════════════════════════════════════════════════ */
int main(void) {
    signal(SIGINT,  stopHandler);
    signal(SIGTERM, stopHandler);

    const char *hubName  = requireEnv("AZURE_IOT_HUB_NAME");
    const char *deviceId = requireEnv("AZURE_DEVICE_ID");

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                "Azure IoT Hub: %s.azure-devices.net", hubName);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                "Device ID: %s", deviceId);

    UA_Server *server = UA_Server_new();

    addPubSubConnection(server, hubName, deviceId);
    addReaderGroup(server);
    addDataSetReader(server, deviceId);

    UA_StatusCode retval = UA_Server_runUntilInterrupt(server);

    UA_Server_delete(server);
    return retval == UA_STATUSCODE_GOOD ? EXIT_SUCCESS : EXIT_FAILURE;
}
