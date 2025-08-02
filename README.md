<div style="margin-top: 16px; display: flex; align-items: center;">
    <img 
        src="https://github.com/divergex/dxlib/raw/main/logo.png" 
        alt="dxlib logo, showing the Chi distribution symbol" 
        width="64" 
        style="margin-right: 10px;" 
    />
    <h1 style="margin: 0;">dxlib - A Quantitative Finance Library</h1>
</div>

Built using Python, **dxlib** provides a comprehensive set of tools for the modern quantitative analyst.

It is a modular, extensible library tailored for both individual strategy developers and professional trading funds. 
Seamlessly integrating with powerful libraries like pandas and numpy, it fits naturally into your existing workflow.

## Motivation

Seeing as other libraries related to quantitative development already exist,
the goal of **dxlib** is to provide a more fledged alternative focusing on maintainability,
extendability and performance. Below, we compare some alternatives:

- **QuandL** has been archived, and **dxlib** is a great alternative.
- **QuantLib** is a great library, and should be used in conjunction with **dxlib**, but its focus differs.
  Eventually, [dxcore](https://github.com/divergex/dxcore/) is meant to replace `QuantLib` within the context of **dxlib**.
- **pandas**, **numpy**, **scikit** and others are great, and should be used in conjunction with **dxlib**.

At the moment, **dxlib** can be used together with [dxforge](https://github.com/divergex/dxforge/)
for **strategy management**, such as **scheduling**, **automatic containerization** and pre-built networking utils.


## Quick Start

Take a look at our [Examples](examples).

## Where We Are Now

All modules and objects serializable, deserializable, and extendable - 
focusing on thread-safety and lock-free implementations for parallel and/or distributed environments.
For now, the cache system uses both HDF5 and JSON, 
and the networking system allows for interfacing with other systems through JSON serialization.

All modules and classes are built using Domain Driven Design, and are designed to be easily understood and used.
I myself come from a computer science background,
and whenever starting a new quant project, always found my code to end up extremely convoluted and messy.
Therefore, I believe creating a library with a strong focus on modularization and performance rather 
than a collection of scripts is the way to go for professional trading setups.

## Future Plans

In the future, **dxlib** will provide low-level routine encapsulation from [dxcore](https://github.com/divergex/dxcore/).
Additionally, [dxstudio](https://github.com/divergex/dxstudio/) will provide GUI access to most of **dxlib** endpoints,
for easy-to-use strategy prototyping, data analysis and other features!

- Current inbuilt handlers include **REST** and **Websockets**.
- Future encodings are planned to include **FIX**, **SBE**, and **Protobuf**.
- Future handlers are planned to include **ZeroMQ**, **gRPC** and rough **UDP**.


## Development Setup

This project uses uv for dependency management, virtual environments, and build automation.

To get started, clone the repository and install the dependencies

```bash

git clone git@github.com:divergex/dxlib.git && cd dxlib
uv venv               # create virtual environment
uv sync --group dev   # install dev dependencies
```
