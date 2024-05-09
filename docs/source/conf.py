# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

project = "dxlib"
copyright = "2023, Rafael Zimmer"
author = "Rafael Zimmer"
release = "1.0.13"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.githubpages",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosummary",
    "sphinx_copybutton",
    'sphinx_exec_code',
    "sphinx.ext.mathjax",
    "sphinx.ext.viewcode",
    "furo.sphinxext",
    "sphinx_design",
    "sphinx_inline_tabs",
    "sphinxext.opengraph",
]

templates_path = ["_templates"]
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output
# html_title = ('<img src="https://avatars.githubusercontent.com/u/116703758?s=200&v=4" alt="dxlib" width="25" '
#               'height="25" align="middle" style="margin-bottom: 15px"> dxlib')
html_theme = "furo"
# extensions += ["sphinxawesome_theme.highlighting"]
html_static_path = ["_static"]
html_favicon = "_static/favicon.ico"
# html_sidebars = {
#     '**': [
#     ],
# }
html_logo = "_static/logo_small.png"
html_theme_options = {
    "source_repository": "https://github.com/divergex/dxlib/",
    "source_branch": "main",
    "source_directory": "docs/source/",
}

exec_code_working_dir = '../..'
