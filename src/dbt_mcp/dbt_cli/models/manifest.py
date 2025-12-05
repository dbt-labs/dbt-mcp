#!/usr/bin/env python3
# This is a SUPER simplified version of the dbt manifest.json structure,
# only including the fields we need

from pydantic import BaseModel, Field


class Node(BaseModel):
    name: str


class Source(BaseModel):
    identifier: str


class Exposure(BaseModel):
    name: str


class Manifest(BaseModel):
    parent_map: dict[str, list[str]] = Field(default_factory=dict)
    child_map: dict[str, list[str]] = Field(default_factory=dict)
    nodes: dict[str, Node] = Field(default_factory=dict)
    sources: dict[str, Source] = Field(default_factory=dict)
    exposures: dict[str, Exposure] = Field(default_factory=dict)
