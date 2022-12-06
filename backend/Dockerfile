# syntax=docker/dockerfile:1.2

FROM python:3.9-bullseye AS build

ENV PIP_ROOT_USER_ACTION=ignore
RUN --mount=type=cache,target=/root/.cache \
    pip install --upgrade pip setuptools wheel

FROM build AS build-import
WORKDIR /build/
COPY /backend ./backend
COPY /setup.py .
COPY /requirements.in .
COPY /MANIFEST.in .
COPY /VERSION .
COPY /README.md .
RUN python setup.py bdist_wheel
