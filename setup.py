from setuptools import setup, find_packages
from pathlib import Path

def parse_requirements(filename):
    reqs = Path(filename).read_text().splitlines()
    return [req for req in reqs if req and not req.startswith('#')]  # Ignore empty lines and comments

requirements = parse_requirements('requirements.txt')

setup(
    name='dxlib',
    use_scm_version=True,
    setup_requires=['setuptools>=42', 'setuptools_scm'],
    packages=find_packages(),
    install_requires=requirements,
    tests_require=[
        'pytest',
    ],
    test_suite='tests',
    author='Rafael Zimmer',
    author_email='rzimmerdev@gmail.com',
    description='A library for quantitative finance, providing tools for data handling, network interfacing, and mathematical modeling.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/divergex/dxlib',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.9',
)
