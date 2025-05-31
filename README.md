<div style="margin-top: 16px; display: flex; align-items: center;">
    <img src="dxlib.png" alt="image" width="64" style="margin-right: 10px;" />
    <h1 style="margin: 0;">dxlib - A Quantitative Finance Library</h1>
</div>


Built using Python, **dxlib** focuses on providing a comprehensive set of tools for quantitative finance. 

The library is designed to be easily adapted and used with other libraries such as **pandas** and **numpy**, 
and inserted into existing workflows.

For low-level development and *HFT*, look at the companion library **dxcore**. **dxlib** is an interface to **dxcore**,
with additional API and network interfacing, QPM, and other tools.

## Motivation

Seeing as some existing libraries targeting quantitative development already exist, 
the goal of **dxlib** is to provide easier and act as a more maintainable library for both
big and small projects.

- **QuandL** has been archived, and **dxlib** is a great alternative.
- **QuantLib** is a great library, and should be used in conjunction with **dxlib**, but its focus differs. 
Eventually, `dxcore` should replace `QuantLib` within the context of **dxlib**.
- **pandas** and **numpy** are great, and should be used in conjunction with **dxlib**.

All modules and classes are built using Domain Driven Design, and are designed to be easily understood and used.
I myself come from a computer science background, 
and whenever starting a new quant project, always found my code to end up extremely convoluted and messy.
Therefore, I believe creating a library with a strong foundation rather than a collection of scripts is the way to go.

All classes and methods are supposed to be easily serializable, deserializable, and extendable - to be freely used in a distributed and/or parallel environment.
For now, the cache system uses both HDF5 and JSON, and the networking system allow for
easily interfacing with other systems. Current inbuilt handlers include **REST** and **Websockets**.
Future encodings are planned to include **FIX**, **SBE**.
Future handlers are planned to include **ZeroMQ**, **gRPC** and rough **UDP**.

In the future, **dxlib** will be able to interface with **dxcore** for low-level development, and **dxforge** for high-level network development.
Look at **dxhft** for implementation-specific high-frequency trading tools.

## Quick Start
