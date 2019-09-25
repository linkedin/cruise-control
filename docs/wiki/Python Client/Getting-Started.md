# Introduction
The `cruise-control-client` directory provides a `python` client for `cruise-control`.  

`cruise-control-client` also provides `cccli`, which is a command-line interface to this `python` client.  
Additionally, the `Endpoint`, `Query`, and `Responder` classes can be used outside of `cccli`, to invoke `cruise-control-client` from another `python` application.

# Getting Started
The recommended method for installing `cruise-control-client` is to use the [Python Package Index](https://pypi.org/)  
## `pip` Install
`cruise-control-client` [exists on PyPI](https://pypi.org/project/cruise-control-client/).  
To create and activate a new [virtual environment](https://docs.python.org/3/library/venv.html), use the following:
```bash
python3.7 -m venv .
. bin/activate
```
Then, to install `cruise-control-client`:  
```bash
pip install cruise-control-client
```
The installation will bind `cccli` to invoke the `cruise-control-client` command-line interface script.  
So, to view help for `cccli`, run the following:  
```bash
cccli --help
```
## CLI Usage
In general, `cccli --socket-address {hostname:port} {endpoint} {options}`
where  
* `hostname:port` is the hostname and port of the `cruise-control` that you'd like to communicate with
* `endpoint` is the `cruise-control` endpoint that you'd like to use
* `options` are `argparse` flags that specify `cruise-control` parameters