#include <Python.h>

#include <new>

#include "scaler/protocol/pymod/bootstrap.h"
#include "scaler/protocol/pymod/module_state.h"
#include "scaler/utility/pymod/compatibility.h"

namespace scaler::protocol::pymod {

int capnp_module_traverse(PyObject* module, visitproc visit, void* arg)
{
    return traverse_module_state(module, visit, arg);
}

int capnp_module_clear(PyObject* module)
{
    return clear_module_state(module);
}

void capnp_module_free(void* module)
{
    clear_module_state(static_cast<PyObject*>(module));
}

PyModuleDef MODULE_DEF;

}  // namespace scaler::protocol::pymod

PyMODINIT_FUNC PyInit_capnp(void)
{
    using scaler::utility::pymod::OwnedPyObject;

    // The module name is materialized as a static char array (not a string literal)
    // because Pyodide's SIDE_MODULE wasm relocator can mis-resolve offsets within
    // mergeable .rodata.str sections, causing the module name to be truncated.
    static const char MODULE_NAME[] = {'c', 'a', 'p', 'n', 'p', '\0'};
    scaler::protocol::pymod::MODULE_DEF = {
        PyModuleDef_HEAD_INIT,
        MODULE_NAME,
        nullptr,
        sizeof(scaler::protocol::pymod::CapnpModuleState),
        nullptr,
        nullptr,
        scaler::protocol::pymod::capnp_module_traverse,
        scaler::protocol::pymod::capnp_module_clear,
        scaler::protocol::pymod::capnp_module_free,
    };

    OwnedPyObject<> module {PyModule_Create(&scaler::protocol::pymod::MODULE_DEF)};
    if (!module) {
        return nullptr;
    }

    auto* state = scaler::protocol::pymod::get_module_state(module.get());
    if (!state) {
        PyErr_SetString(PyExc_RuntimeError, "failed to allocate capnp module state");
        return nullptr;
    }
    new (state) scaler::protocol::pymod::CapnpModuleState {};

    scaler::protocol::pymod::set_initializing_module(module.get());
    if (!scaler::protocol::pymod::initialize_runtime_modules(module.get())) {
        scaler::protocol::pymod::set_initializing_module(nullptr);
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_RuntimeError, "failed to initialize capnp runtime modules");
        }
        return nullptr;
    }
    scaler::protocol::pymod::set_initializing_module(nullptr);

    // Defensive: explicitly stamp the fully-qualified __name__ / __package__
    // on the module. With single-phase init the import system normally
    // overwrites __name__ from the spec after PyInit_ returns, but Pyodide's
    // SIDE_MODULE loader path has been observed to leave __name__ as the
    // bare m_name ("capnp") — which makes ``from scaler.protocol.capnp import X``
    // synthesize ``.X`` inside _handle_fromlist and recurse into __import__("")
    // → ValueError("Empty module name"). Same anti-relocator char-array
    // construction as MODULE_NAME above so the literal cannot be merged into
    // a section the wasm relocator mis-resolves.
    static const char FULL_MODULE_NAME[] = {
        's', 'c', 'a', 'l', 'e', 'r', '.', 'p', 'r', 'o', 't', 'o', 'c', 'o', 'l', '.', 'c', 'a', 'p', 'n', 'p', '\0'};
    static const char PACKAGE_NAME[] = {
        's', 'c', 'a', 'l', 'e', 'r', '.', 'p', 'r', 'o', 't', 'o', 'c', 'o', 'l', '\0'};
    static const char NAME_ATTR[]    = {'_', '_', 'n', 'a', 'm', 'e', '_', '_', '\0'};
    static const char PACKAGE_ATTR[] = {'_', '_', 'p', 'a', 'c', 'k', 'a', 'g', 'e', '_', '_', '\0'};
    OwnedPyObject<> full_name {PyUnicode_FromString(FULL_MODULE_NAME)};
    OwnedPyObject<> package_name {PyUnicode_FromString(PACKAGE_NAME)};
    if (!full_name || !package_name ||
        PyObject_SetAttrString(module.get(), NAME_ATTR, full_name.get()) < 0 ||
        PyObject_SetAttrString(module.get(), PACKAGE_ATTR, package_name.get()) < 0) {
        return nullptr;
    }

    return module.take();
}
