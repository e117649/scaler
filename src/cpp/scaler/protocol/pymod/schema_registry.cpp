#include "scaler/protocol/pymod/schema_registry.h"

#include <stdexcept>

#include "protocol/common.capnp.h"
#include "protocol/message.capnp.h"
#include "protocol/object_storage.capnp.h"
#include "protocol/status.capnp.h"

namespace scaler::protocol::pymod {

template <typename T>
void SchemaRegistry::registerCompiledSchema(const char* moduleName, const char* typeName)
{
    _loader.loadCompiledTypeAndDependencies<T>();
    auto schema = capnp::Schema::from<T>();
    _moduleSchemas[moduleName].push_back(schema);
    _topLevelTypeIds.emplace(typeName, schema.getProto().getId());
}

// Pyodide's SIDE_MODULE relocator mis-resolves offsets within mergeable
// `.rodata.str1.1` sections, causing tail-merged short string literals to
// resolve to bytes inside longer literals at load time. Storing module names
// in dedicated `static const char[]` arrays forces the linker to allocate
// them in non-mergeable storage so the addresses remain valid.
namespace {
static const char kModCommon[]         = "common";
static const char kModStatus[]         = "status";
static const char kModMessage[]        = "message";
static const char kModObjectStorage[] = "object_storage";
}  // namespace

bool SchemaRegistry::init()
{
    if (_initialized) {
        return true;
    }

    registerCompiledSchema<scaler::protocol::TaskResultType>(kModCommon, "TaskResultType");
    registerCompiledSchema<scaler::protocol::TaskCancelConfirmType>(kModCommon, "TaskCancelConfirmType");
    registerCompiledSchema<scaler::protocol::TaskTransition>(kModCommon, "TaskTransition");
    registerCompiledSchema<scaler::protocol::TaskState>(kModCommon, "TaskState");
    registerCompiledSchema<scaler::protocol::WorkerState>(kModCommon, "WorkerState");
    registerCompiledSchema<scaler::protocol::TaskCapability>(kModCommon, "TaskCapability");
    registerCompiledSchema<scaler::protocol::ObjectMetadata>(kModCommon, "ObjectMetadata");
    registerCompiledSchema<scaler::protocol::ObjectStorageAddress>(kModCommon, "ObjectStorageAddress");

    registerCompiledSchema<scaler::protocol::Resource>(kModStatus, "Resource");
    registerCompiledSchema<scaler::protocol::ObjectManagerStatus>(kModStatus, "ObjectManagerStatus");
    registerCompiledSchema<scaler::protocol::ClientManagerStatus>(kModStatus, "ClientManagerStatus");
    registerCompiledSchema<scaler::protocol::TaskManagerStatus>(kModStatus, "TaskManagerStatus");
    registerCompiledSchema<scaler::protocol::ProcessorStatus>(kModStatus, "ProcessorStatus");
    registerCompiledSchema<scaler::protocol::WorkerStatus>(kModStatus, "WorkerStatus");
    registerCompiledSchema<scaler::protocol::WorkerManagerStatus>(kModStatus, "WorkerManagerStatus");
    registerCompiledSchema<scaler::protocol::ScalingManagerStatus>(kModStatus, "ScalingManagerStatus");
    registerCompiledSchema<scaler::protocol::BinderStatus>(kModStatus, "BinderStatus");

    registerCompiledSchema<scaler::protocol::Task>(kModMessage, "Task");
    registerCompiledSchema<scaler::protocol::TaskCancel>(kModMessage, "TaskCancel");
    registerCompiledSchema<scaler::protocol::TaskLog>(kModMessage, "TaskLog");
    registerCompiledSchema<scaler::protocol::TaskResult>(kModMessage, "TaskResult");
    registerCompiledSchema<scaler::protocol::TaskCancelConfirm>(kModMessage, "TaskCancelConfirm");
    registerCompiledSchema<scaler::protocol::GraphTask>(kModMessage, "GraphTask");
    registerCompiledSchema<scaler::protocol::ClientHeartbeat>(kModMessage, "ClientHeartbeat");
    registerCompiledSchema<scaler::protocol::ClientHeartbeatEcho>(kModMessage, "ClientHeartbeatEcho");
    registerCompiledSchema<scaler::protocol::WorkerHeartbeat>(kModMessage, "WorkerHeartbeat");
    registerCompiledSchema<scaler::protocol::WorkerHeartbeatEcho>(kModMessage, "WorkerHeartbeatEcho");
    registerCompiledSchema<scaler::protocol::WorkerManagerHeartbeat>(kModMessage, "WorkerManagerHeartbeat");
    registerCompiledSchema<scaler::protocol::WorkerManagerHeartbeatEcho>(kModMessage, "WorkerManagerHeartbeatEcho");
    registerCompiledSchema<scaler::protocol::WorkerManagerCommandType>(kModMessage, "WorkerManagerCommandType");
    registerCompiledSchema<scaler::protocol::WorkerManagerCommand>(kModMessage, "WorkerManagerCommand");
    registerCompiledSchema<scaler::protocol::WorkerManagerCommandResponse>(kModMessage, "WorkerManagerCommandResponse");
    registerCompiledSchema<scaler::protocol::ObjectInstruction>(kModMessage, "ObjectInstruction");
    registerCompiledSchema<scaler::protocol::DisconnectRequest>(kModMessage, "DisconnectRequest");
    registerCompiledSchema<scaler::protocol::DisconnectResponse>(kModMessage, "DisconnectResponse");
    registerCompiledSchema<scaler::protocol::ClientDisconnect>(kModMessage, "ClientDisconnect");
    registerCompiledSchema<scaler::protocol::ClientShutdownResponse>(kModMessage, "ClientShutdownResponse");
    registerCompiledSchema<scaler::protocol::StateClient>(kModMessage, "StateClient");
    registerCompiledSchema<scaler::protocol::StateObject>(kModMessage, "StateObject");
    registerCompiledSchema<scaler::protocol::StateBalanceAdvice>(kModMessage, "StateBalanceAdvice");
    registerCompiledSchema<scaler::protocol::StateScheduler>(kModMessage, "StateScheduler");
    registerCompiledSchema<scaler::protocol::StateWorker>(kModMessage, "StateWorker");
    registerCompiledSchema<scaler::protocol::StateTask>(kModMessage, "StateTask");
    registerCompiledSchema<scaler::protocol::StateGraphTask>(kModMessage, "StateGraphTask");
    registerCompiledSchema<scaler::protocol::ProcessorInitialized>(kModMessage, "ProcessorInitialized");
    registerCompiledSchema<scaler::protocol::InformationRequest>(kModMessage, "InformationRequest");
    registerCompiledSchema<scaler::protocol::InformationResponse>(kModMessage, "InformationResponse");
    registerCompiledSchema<scaler::protocol::Message>(kModMessage, "Message");

    registerCompiledSchema<scaler::protocol::ObjectRequestHeader>(kModObjectStorage, "ObjectRequestHeader");
    registerCompiledSchema<scaler::protocol::ObjectID>(kModObjectStorage, "ObjectID");
    registerCompiledSchema<scaler::protocol::ObjectResponseHeader>(kModObjectStorage, "ObjectResponseHeader");

    for (const auto& schema: _loader.getAllLoaded()) {
        _schemasById.emplace(schema.getProto().getId(), schema);
    }

    _initialized = true;
    return true;
}

capnp::Schema SchemaRegistry::getSchemaById(uint64_t schemaId)
{
    init();
    return _schemasById.at(schemaId);
}

capnp::StructSchema SchemaRegistry::getStructById(uint64_t schemaId)
{
    return getSchemaById(schemaId).asStruct();
}

capnp::EnumSchema SchemaRegistry::getEnumById(uint64_t schemaId)
{
    return getSchemaById(schemaId).asEnum();
}

capnp::StructSchema SchemaRegistry::getStructByName(const std::string& typeName)
{
    auto type_id = _topLevelTypeIds.find(typeName);
    if (type_id != _topLevelTypeIds.end()) {
        return getStructById(type_id->second);
    }

    auto separator = typeName.rfind('.');
    if (separator == std::string::npos) {
        throw std::out_of_range("unknown Cap'n Proto struct type");
    }

    return getStructById(_topLevelTypeIds.at(typeName.substr(separator + 1)));
}

const std::vector<capnp::Schema>* SchemaRegistry::getModuleSchemas(const std::string& moduleName) const
{
    auto it = _moduleSchemas.find(moduleName);
    if (it == _moduleSchemas.end()) {
        return nullptr;
    }
    return &it->second;
}

}  // namespace scaler::protocol::pymod
