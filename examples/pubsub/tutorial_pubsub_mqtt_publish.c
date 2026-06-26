/* OPC UA PubSub MQTT Publisher – Azure IoT Hub with X.509 Certificate Auth
 *
 * Derived from open62541 tutorial_pubsub_mqtt_publish.c (CC0 1.0 Universal).
 *
 * Azure IoT Hub MQTT specifics:
 *   Endpoint  : <hub>.azure-devices.net:8883
 *   Client-ID : <device-id>
 *   Username  : <hub>.azure-devices.net/<device-id>/?api-version=2021-04-12
 *   Password  : (empty – X.509 cert is the authenticator)
 *   Topic     : devices/<device-id>/messages/events/
 *   TLS       : mutual TLS; client sends device cert + key; server cert is
 *               validated against the DigiCert Global Root G2 CA bundle.
 *
 * Environment variables (required):
 *   AZURE_IOT_HUB_NAME   – e.g. "myhub"  (without .azure-devices.net)
 *   AZURE_DEVICE_ID      – e.g. "my-opc-device"
 *
 * Certificate paths (mounted into the container via docker-compose):
 *   /certs/device.crt    – Device certificate (PEM)
 *   /certs/device.key    – Device private key  (PEM)
 *   /certs/azure_root_ca.pem – DigiCert Global Root G2 (trust anchor)
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

/* ── Connection / topic names ──────────────────────────────────────────── */
#define CONNECTION_NAME          "Azure IoT Hub Publisher"
#define MQTT_CLIENT_ID_PREFIX    "opcua-pub-"   /* appended with device id  */

/* ── Certificate paths (fixed inside the container) ───────────────────── */
#define DEVICE_CERT_PATH         "/certs/device.crt"
#define DEVICE_KEY_PATH          "/certs/device.key"
#define AZURE_ROOT_CA_PATH       "/certs/azure_root_ca.pem"

/* ── Publish interval ──────────────────────────────────────────────────── */
#define PUBLISH_INTERVAL_MS      1000

/* ── Node identifiers ──────────────────────────────────────────────────── */
static UA_NodeId connectionIdent;
static UA_NodeId publishedDataSetIdent;
static UA_NodeId writerGroupIdent;
static UA_NodeId dataSetWriterIdent;

/* ── Signal flag ───────────────────────────────────────────────────────── */
static volatile UA_Boolean running = true;
static void stopHandler(int sig) {
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Caught signal %d", sig);
    running = false;
}

/* ── Helper: read env or die ───────────────────────────────────────────── */
static const char *requireEnv(const char *name) {
    const char *val = getenv(name);
    if(!val || val[0] == '\0') {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                     "Required environment variable %s is not set.", name);
        exit(EXIT_FAILURE);
    }
    return val;
}

/* ── Build the broker URL string ───────────────────────────────────────── */
/* Result written into buf (caller supplies). Returns buf. */
static char *buildBrokerUrl(const char *hubName, char *buf, size_t bufSize) {
    /* Azure IoT Hub MQTT-over-TLS endpoint is port 8883 */
    snprintf(buf, bufSize, "opc.mqtts://%s.azure-devices.net:8883", hubName);
    return buf;
}

/* ── Build the MQTT username ────────────────────────────────────────────── */
/* Format: <hub>.azure-devices.net/<deviceId>/?api-version=2021-04-12      */
static char *buildMqttUsername(const char *hubName,
                                const char *deviceId,
                                char *buf, size_t bufSize) {
    snprintf(buf, bufSize,
             "%s.azure-devices.net/%s/?api-version=2021-04-12",
             hubName, deviceId);
    return buf;
}

/* ── Build the D2C topic ────────────────────────────────────────────────── */
static char *buildPublishTopic(const char *deviceId, char *buf, size_t bufSize) {
    snprintf(buf, bufSize, "devices/%s/messages/events/", deviceId);
    return buf;
}

/* ══════════════════════════════════════════════════════════════════════════
 *  PubSub setup functions
 * ══════════════════════════════════════════════════════════════════════════ */

static void
addPubSubConnection(UA_Server *server,
                    const char *hubName,
                    const char *deviceId) {

    char brokerUrl[256];
    char mqttUsername[256];
    char clientId[128];
    char publishTopic[256];

    buildBrokerUrl(hubName, brokerUrl, sizeof(brokerUrl));
    buildMqttUsername(hubName, deviceId, mqttUsername, sizeof(mqttUsername));
    snprintf(clientId, sizeof(clientId), "%s%s", MQTT_CLIENT_ID_PREFIX, deviceId);
    buildPublishTopic(deviceId, publishTopic, sizeof(publishTopic));

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                "Connecting to broker: %s", brokerUrl);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                "MQTT username: %s", mqttUsername);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                "Publish topic: %s", publishTopic);

    UA_PubSubConnectionConfig connectionConfig;
    memset(&connectionConfig, 0, sizeof(UA_PubSubConnectionConfig));
    connectionConfig.name = UA_STRING(CONNECTION_NAME);
    connectionConfig.transportProfileUri =
        UA_STRING(TRANSPORT_PROFILE_URI);
    connectionConfig.enabled = UA_TRUE;

    UA_NetworkAddressUrlDataType networkAddressUrl;
    networkAddressUrl.networkInterface = UA_STRING_NULL;
    networkAddressUrl.url = UA_STRING(brokerUrl);

    UA_Variant_setScalar(&connectionConfig.address, &networkAddressUrl,
                         &UA_TYPES[UA_TYPES_NETWORKSADDRESSURLDATATYPE]);

    /* ── Connection options ─────────────────────────────────────────────── */
    /* We need: clientId, username, password (empty), TLS CA, cert, key      */
#define NUM_CONN_OPTIONS 6
    UA_KeyValuePair connectionOptions[NUM_CONN_OPTIONS];
    UA_UInt32 optIdx = 0;

    /* 1. MQTT Client ID */
    connectionOptions[optIdx].key =
        UA_QUALIFIEDNAME(0, "mqttClientId");
    UA_String mqttClientIdStr = UA_STRING(clientId);
    UA_Variant_setScalar(&connectionOptions[optIdx].value,
                         &mqttClientIdStr, &UA_TYPES[UA_TYPES_STRING]);
    optIdx++;

    /* 2. MQTT Username */
    connectionOptions[optIdx].key =
        UA_QUALIFIEDNAME(0, "mqttUsername");
    UA_String mqttUsernameStr = UA_STRING(mqttUsername);
    UA_Variant_setScalar(&connectionOptions[optIdx].value,
                         &mqttUsernameStr, &UA_TYPES[UA_TYPES_STRING]);
    optIdx++;

    /* 3. MQTT Password – empty for X.509 auth */
    connectionOptions[optIdx].key =
        UA_QUALIFIEDNAME(0, "mqttPassword");
    UA_String mqttPasswordStr = UA_STRING_NULL;
    UA_Variant_setScalar(&connectionOptions[optIdx].value,
                         &mqttPasswordStr, &UA_TYPES[UA_TYPES_STRING]);
    optIdx++;

    /* 4. Enable TLS */
    connectionOptions[optIdx].key =
        UA_QUALIFIEDNAME(0, "mqttUseTLS");
    UA_Boolean useTLS = UA_TRUE;
    UA_Variant_setScalar(&connectionOptions[optIdx].value,
                         &useTLS, &UA_TYPES[UA_TYPES_BOOLEAN]);
    optIdx++;

    /* 5. CA Certificate file (DigiCert Global Root G2) */
    connectionOptions[optIdx].key =
        UA_QUALIFIEDNAME(0, "mqttCaFilePath");
    UA_String caFileStr = UA_STRING(AZURE_ROOT_CA_PATH);
    UA_Variant_setScalar(&connectionOptions[optIdx].value,
                         &caFileStr, &UA_TYPES[UA_TYPES_STRING]);
    optIdx++;

    /* 6. Client Certificate (device cert) */
    connectionOptions[optIdx].key =
        UA_QUALIFIEDNAME(0, "mqttClientCertPath");
    UA_String clientCertStr = UA_STRING(DEVICE_CERT_PATH);
    UA_Variant_setScalar(&connectionOptions[optIdx].value,
                         &clientCertStr, &UA_TYPES[UA_TYPES_STRING]);
    optIdx++;

    /* NOTE: mqttClientKeyPath would go here if the open62541 version you build
     * supports it. Check your open62541 MQTT plugin headers. Some builds expose
     * it; if yours does, uncomment and increment NUM_CONN_OPTIONS:
     *
     * connectionOptions[optIdx].key =
     *     UA_QUALIFIEDNAME(0, "mqttClientKeyPath");
     * UA_String clientKeyStr = UA_STRING(DEVICE_KEY_PATH);
     * UA_Variant_setScalar(&connectionOptions[optIdx].value,
     *                      &clientKeyStr, &UA_TYPES[UA_TYPES_STRING]);
     * optIdx++;
     */

    connectionConfig.connectionProperties.mapSize = optIdx;
    connectionConfig.connectionProperties.map    = connectionOptions;

    UA_Server_addPubSubConnection(server, &connectionConfig, &connectionIdent);
}

/* ── PublishedDataSet ───────────────────────────────────────────────────── */
static void addPublishedDataSet(UA_Server *server) {
    UA_PublishedDataSetConfig pdsConfig;
    memset(&pdsConfig, 0, sizeof(UA_PublishedDataSetConfig));
    pdsConfig.publishedDataSetType =
        UA_PUBSUB_DATASET_PUBLISHEDITEMS;
    pdsConfig.name = UA_STRING("AzureDataSet");
    UA_Server_addPublishedDataSet(server, &pdsConfig,
                                  &publishedDataSetIdent);
}

/* ── DataSetField (example: server time) ───────────────────────────────── */
static void addDataSetField(UA_Server *server) {
    UA_DataSetFieldConfig dsFieldConfig;
    memset(&dsFieldConfig, 0, sizeof(UA_DataSetFieldConfig));
    dsFieldConfig.dataSetFieldType =
        UA_PUBSUB_DATASETFIELD_VARIABLE;
    dsFieldConfig.field.variable.fieldNameAlias =
        UA_STRING("ServerTime");
    dsFieldConfig.field.variable.promotedField = UA_FALSE;
    dsFieldConfig.field.variable.publishParameters.publishedVariable =
        UA_NODEID_NUMERIC(0, UA_NS0ID_SERVER_SERVERSTATUS_CURRENTTIME);
    dsFieldConfig.field.variable.publishParameters.attributeId =
        UA_ATTRIBUTEID_VALUE;
    UA_DataSetFieldResult result;
    UA_Server_addDataSetField(server, publishedDataSetIdent,
                              &dsFieldConfig, &result);
}

/* ── WriterGroup ────────────────────────────────────────────────────────── */
static void addWriterGroup(UA_Server *server, const char *deviceId) {
    char publishTopic[256];
    buildPublishTopic(deviceId, publishTopic, sizeof(publishTopic));

    UA_WriterGroupConfig writerGroupConfig;
    memset(&writerGroupConfig, 0, sizeof(UA_WriterGroupConfig));
    writerGroupConfig.name             = UA_STRING("AzureWriterGroup");
    writerGroupConfig.publishingInterval = PUBLISH_INTERVAL_MS;
    writerGroupConfig.enabled          = UA_FALSE;
    writerGroupConfig.writerGroupId    = 100;
    writerGroupConfig.encodingMimeType = UA_PUBSUB_ENCODING_JSON;

    /* MQTT topic for this writer group */
    UA_BrokerWriterGroupTransportDataType brokerTransportSettings;
    memset(&brokerTransportSettings, 0,
           sizeof(UA_BrokerWriterGroupTransportDataType));
    brokerTransportSettings.queueName      = UA_STRING(publishTopic);
    brokerTransportSettings.requestedDeliveryGuarantee =
        UA_BROKERTRANSPORTQUALITYOFSERVICE_ATMOSTON;

    UA_ExtensionObject transportSettings;
    UA_ExtensionObject_init(&transportSettings);
    transportSettings.encoding = UA_EXTENSIONOBJECT_DECODED;
    transportSettings.content.decoded.type =
        &UA_TYPES[UA_TYPES_BROKERWRITERGROUPTRANSPORTDATATYPE];
    transportSettings.content.decoded.data = &brokerTransportSettings;
    writerGroupConfig.transportSettings    = transportSettings;

    UA_JsonWriterGroupMessageDataType jsonWGMConfig;
    memset(&jsonWGMConfig, 0, sizeof(UA_JsonWriterGroupMessageDataType));
    jsonWGMConfig.networkMessageContentMask =
        (UA_JsonNetworkMessageContentMask)(
            UA_JSONNETWORKMESSAGECONTENTMASK_NETWORKMESSAGEHEADER |
            UA_JSONNETWORKMESSAGECONTENTMASK_DATASETMESSAGEHEADER |
            UA_JSONNETWORKMESSAGECONTENTMASK_SINGLEDATASETMESSAGE |
            UA_JSONNETWORKMESSAGECONTENTMASK_PUBLISHERID |
            UA_JSONNETWORKMESSAGECONTENTMASK_DATASETCLASSID);

    UA_ExtensionObject messageSettings;
    UA_ExtensionObject_init(&messageSettings);
    messageSettings.encoding = UA_EXTENSIONOBJECT_DECODED;
    messageSettings.content.decoded.type =
        &UA_TYPES[UA_TYPES_JSONWRITERGROUPTRANSPORTDATATYPE];
    messageSettings.content.decoded.data = &jsonWGMConfig;
    writerGroupConfig.messageSettings     = messageSettings;

    UA_Server_addWriterGroup(server, connectionIdent,
                             &writerGroupConfig, &writerGroupIdent);
}

/* ── DataSetWriter ──────────────────────────────────────────────────────── */
static void addDataSetWriter(UA_Server *server, const char *deviceId) {
    char publishTopic[256];
    buildPublishTopic(deviceId, publishTopic, sizeof(publishTopic));

    UA_DataSetWriterConfig dataSetWriterConfig;
    memset(&dataSetWriterConfig, 0, sizeof(UA_DataSetWriterConfig));
    dataSetWriterConfig.name             = UA_STRING("AzureDataSetWriter");
    dataSetWriterConfig.dataSetWriterId  = 62541;
    dataSetWriterConfig.keyFrameCount    = 10;

    UA_BrokerDataSetWriterTransportDataType brokerTransportSettings;
    memset(&brokerTransportSettings, 0,
           sizeof(UA_BrokerDataSetWriterTransportDataType));
    brokerTransportSettings.queueName = UA_STRING(publishTopic);

    UA_ExtensionObject transportSettings;
    UA_ExtensionObject_init(&transportSettings);
    transportSettings.encoding = UA_EXTENSIONOBJECT_DECODED;
    transportSettings.content.decoded.type =
        &UA_TYPES[UA_TYPES_BROKERDATASETWRITERTRANSPORTDATATYPE];
    transportSettings.content.decoded.data = &brokerTransportSettings;
    dataSetWriterConfig.transportSettings  = transportSettings;

    UA_JsonDataSetWriterMessageDataType jsonDSWMConfig;
    memset(&jsonDSWMConfig, 0, sizeof(UA_JsonDataSetWriterMessageDataType));
    jsonDSWMConfig.dataSetMessageContentMask =
        (UA_JsonDataSetMessageContentMask)(
            UA_JSONDATASETMESSAGECONTENTMASK_DATASETWRITERID |
            UA_JSONDATASETMESSAGECONTENTMASK_SEQUENCENUMBER |
            UA_JSONDATASETMESSAGECONTENTMASK_STATUS |
            UA_JSONDATASETMESSAGECONTENTMASK_METADATAVERSION |
            UA_JSONDATASETMESSAGECONTENTMASK_TIMESTAMP);

    UA_ExtensionObject messageSettings;
    UA_ExtensionObject_init(&messageSettings);
    messageSettings.encoding = UA_EXTENSIONOBJECT_DECODED;
    messageSettings.content.decoded.type =
        &UA_TYPES[UA_TYPES_JSONDATASETWRITERMESSAGEDATATYPE];
    messageSettings.content.decoded.data = &jsonDSWMConfig;
    dataSetWriterConfig.messageSettings   = messageSettings;

    UA_Server_addDataSetWriter(server, writerGroupIdent,
                               publishedDataSetIdent,
                               &dataSetWriterConfig, &dataSetWriterIdent);
}

/* ══════════════════════════════════════════════════════════════════════════
 *  main
 * ══════════════════════════════════════════════════════════════════════════ */
int main(void) {
    signal(SIGINT,  stopHandler);
    signal(SIGTERM, stopHandler);

    /* ── Read configuration from environment ─────────────────────────── */
    const char *hubName  = requireEnv("AZURE_IOT_HUB_NAME");
    const char *deviceId = requireEnv("AZURE_DEVICE_ID");

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                "Azure IoT Hub: %s.azure-devices.net", hubName);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                "Device ID: %s", deviceId);

    /* ── Build and configure server ──────────────────────────────────── */
    UA_Server *server = UA_Server_new();

    addPubSubConnection(server, hubName, deviceId);
    addPublishedDataSet(server);
    addDataSetField(server);
    addWriterGroup(server, deviceId);
    addDataSetWriter(server, deviceId);

    UA_Server_enableAllPubSubComponents(server);

    /* ── Run ─────────────────────────────────────────────────────────── */
    UA_StatusCode retval = UA_Server_runUntilInterrupt(server);

    UA_Server_delete(server);
    return retval == UA_STATUSCODE_GOOD ? EXIT_SUCCESS : EXIT_FAILURE;
}
