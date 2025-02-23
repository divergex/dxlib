# Building and Running Sphinx Documentation

This repository contains Sphinx documentation for the `History` class. Follow the steps below to build and view the documentation.

## Prerequisites

Ensure you have Python installed and set up a virtual environment (optional but recommended):

```sh
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```

Install the required dependencies:

```sh
pip install -r dxlib/docs/requirements.txt
```

## Building the Documentation

Navigate to the `docs` directory and run the following command to generate the HTML documentation:

```sh
sphinx-build -b html source build
```

Alternatively, use the `Makefile` (Linux/macOS) or `make.bat` (Windows):

```sh
make html  # On Linux/macOS
make.bat html  # On Windows
```

## Viewing the Documentation

Once built, open the `build/html/index.html` file in a web browser to view the documentation.

## Cleaning Up

To remove generated documentation files, run:

```sh
make clean
```

## Auto-Generating Documentation

To update documentation from docstrings automatically, run:

```sh
sphinx-apidoc -o source ../your_module
```

Then, rebuild the documentation as described above.

---

Now you're ready to compile and view your Sphinx documentation!


## Test build locally with GitHub CLI - nektos/act

```sh
gh act -j build
```
