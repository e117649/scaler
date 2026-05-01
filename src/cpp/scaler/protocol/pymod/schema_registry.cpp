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
static const char MOD_COMMON[]         = "common";
static const char MOD_STATUS[]         = "status";
static const char MOD_MESSAGE[]        = "message";
static const char MOD_OBJECT_STORAGE[] = "object_storage";
}  // namespace

bool SchemaRegistry::init()
{
    if (_initialized) {
        return true;
    }

    registerCompiledSchema<scaler::protocol::TaskResultType>(MOD_COMMON, "TaskResultType");
    registerCompiledSchema<scaler::protocol::TaskCancelConfirmType>(MOD_COMMON, "TaskCancelConfirmType");
    registerCompiledSchema<scaler::protocol::TaskTransition>(MOD_COMMON, "TaskTransition");
    registerCompiledSchema<scaler::protocol::TaskState>(MOD_COMMON, "TaskState");
    registerCompiledSchema<scaler::protocol::WorkerState>(MOD_COMMON, "WorkerState");
    registerCompiledSchema<scaler::protocol::TaskCapability>(MOD_COMMON, "TaskCapability");
    registerCompiledSchema<scaler::protocol::ObjectMetadata>(MOD_COMMON, "ObjectMetadata");
    registerCompiledSchema<scaler::protocol::ObjectStorageAddress>(MOD_COMMON, "ObjectStorageAddress");

    registerCompiledSchema<scaler::protocol::Resource>(MOD_STATUS, "Resource");
    registerCompiledSchema<scaler::protocol::ObjectManagerStatus>(MOD_STATUS, "ObjectManagerStatus");
    registerCompiledSchema<scaler::protocol::ClientManagerStatus>(MOD_STATUS, "ClientManagerStatus");
    registerCompiledSchema<scaler::protocol::TaskManagerStatus>(MOD_STATUS, "TaskManagerStatus");
    registerCompiledSchema<scaler::protocol::ProcessorStatus>(MOD_STATUS, "ProcessorStatus");
    registerCompiledSchema<scaler::protocol::WorkerStatus>(MOD_STATUS, "WorkerStatus");
    registerCompiledSchema<scaler::protocol::WorkerManagerStatus>(MOD_STATUS, "WorkerManagerStatus");
    registerCompiledSchema<scaler::protocol::ScalingManagerStatus>(MOD_STATUS, "ScalingManagerStatus");
    registerCompiledSchema<scaler::protocol::BinderStatus>(MOD_STATUS, "BinderStatus");

    registerCompiledSchema<scaler::protocol::Task>(MOD_MESSAGE, "Task");
    registerCompiledSchema<scaler::protocol::TaskCancel>(MOD_MESSAGE, "TaskCancel");
    registerCompiledSchema<scaler::protocol::TaskLog>(MOD_MESSAGE, "TaskLog");
    registerCompiledSchema<scaler::protocol::TaskResult>(MOD_MESSAGE, "TaskResult");
    registerCompiledSchema<scaler::protocol::TaskCancelConfirm>(MOD_MESSAGE, "TaskCancelConfirm");
    registerCompiledSchema<scaler::protocol::GraphTask>(MOD_MESSAGE, "GraphTask");
    registerCompiledSchema<scaler::protocol::ClientHeartbeat>(MOD_MESSAGE, "ClientHeartbeat");
    registerCompiledSchema<scaler::protocol::ClientHeartbeatEcho>(MOD_MESSAGE, "ClientHeartbeatEcho");
    registerCompiledSchema<scaler::protocol::WorkerHeartbeat>(MOD_MESSAGE, "WorkerHeartbeat");
    registerCompiledSchema<scaler::protocol::WorkerHeartbeatEcho>(MOD_MESSAGE, "WorkerHeartbeatEcho");
    registerCompiledSchema<scaler::protocol::WorkerManagerHeartbeat>(MOD_MESSAGE, "WorkerManagerHeartbeat");
    registerCompiledSchema<scaler::protocol::WorkerManagerHeartbeatEcho>(MOD_MESSAGE, "WorkerManagerHeartbeatEcho");
    registerCompiledSchema<scaler::protocol::WorkerManagerCommandType>(MOD_MESSAGE, "WorkerManagerCommandType");
    registerCompiledSchema<scaler::protocol::WorkerManagerCommand>(MOD_MESSAGE, "WorkerManagerCommand");
    registerCompiledSchema<scaler::protocol::WorkerManagerCommandResponse>(MOD_MESSAGE, "WorkerManagerCommandResponse");
    registerCompiledSchema<scaler::protocol::ObjectInstruction>(MOD_MESSAGE, "ObjectInstruction");
    registerCompiledSchema<scaler::protocol::DisconnectRequest>(MOD_MESSAGE, "DisconnectRequest");
    registerCompiledSchema<scaler::protocol::DisconnectResponse>(MOD_MESSAGE, "DisconnectResponse");
    registerCompiledSchema<scaler::protocol::ClientDisconnect>(MOD_MESSAGE, "ClientDisconnect");
    registerCompiledSchema<scaler::protocol::ClientShutdownResponse>(MOD_MESSAGE, "ClientShutdownResponse");
    registerCompiledSchema<scaler::protocol::StateClient>(MOD_MESSAGE, "StateClient");
    registerCompiledSchema<scaler::protocol::StateObject>(MOD_MESSAGE, "StateObject");
    registerCompiledSchema<scaler::protocol::StateBalanceAdvice>(MOD_MESSAGE, "StateBalanceAdvice");
    registerCompiledSchema<scaler::protocol::StateScheduler>(MOD_MESSAGE, "StateScheduler");
    registerCompiledSchema<scaler::protocol::StateWorker>(MOD_MESSAGE, "StateWorker");
    registerCompiledSchema<scaler::protocol::StateTask>(MOD_MESSAGE, "StateTask");
    registerCompiledSchema<scaler::protocol::StateGraphTask>(MOD_MESSAGE, "StateGraphTask");
    registerCompiledSchema<scaler::protocol::ProcessorInitialized>(MOD_MESSAGE, "ProcessorInitialized");
    registerCompiledSchema<scaler::protocol::InformationRequest>(MOD_MESSAGE, "InformationRequest");
    registerCompiledSchema<scaler::protocol::InformationResponse>(MOD_MESSAGE, "InformationResponse");
    registerCompiledSchema<scaler::protocol::Message>(MOD_MESSAGE, "Message");

    registerCompiledSchema<scaler::protocol::ObjectRequestHeader>(MOD_OBJECT_STORAGE, "ObjectRequestHeader");
    registerCompiledSchema<scaler::protocol::ObjectID>(MOD_OBJECT_STORAGE, "ObjectID");
    registerCompiledSchema<scaler::protocol::ObjectResponseHeader>(MOD_OBJECT_STORAGE, "ObjectResponseHeader");

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
