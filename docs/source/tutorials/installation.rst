.. _installation_options:

Installation
============

The ``opengris-scaler`` package is available on PyPI and can be installed using any compatible package manager. The examples below use `uv <https://docs.astral.sh/uv/getting-started/installation>`_.

Base installation:

.. code-block:: bash

    uv pip install opengris-scaler

If you need the web GUI:

.. code-block:: bash

    uv pip install opengris-scaler[gui]

If you use GraphBLAS to solve DAG graph tasks:

.. code-block:: bash

    uv pip install opengris-scaler[graphblas]

If you need all optional dependencies:

.. code-block:: bash

    uv pip install opengris-scaler[all]


Browser (WebAssembly) client
----------------------------

A WebAssembly build of the Scaler client can run inside a browser via Pyodide / JupyterLite. The pre-built wasm wheel is shipped under ``_static/wasm/`` in the published docs and can be installed with ``micropip``.

The wasm wheel is built against the CPython interpreter shipped by Pyodide, currently **CPython 3.13**. Because the capnp protocol bindings rely on the CPython ABI, the scheduler and worker(s) the browser client connects to **must also be running CPython 3.13**. Mixing a wasm 3.13 client with a 3.10 / 3.11 / 3.12 scheduler is unsupported and surfaces as opaque capnp struct decoding failures.

If you maintain the cluster, pin the scheduler / worker venv to Python 3.13 before pointing a browser client at it.
