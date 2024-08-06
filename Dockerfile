FROM python:3.10-slim

WORKDIR /dxlib/

COPY dxlib dxlib/

COPY requirements.txt .

COPY README.md .

COPY setup.py .

RUN pip install -e /dxlib/
