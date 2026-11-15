"""Pydantic request models."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

LogicalType = Literal["int64", "float64", "string"]


class FieldSpec(BaseModel):
    field_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    type: LogicalType
    nullable: bool = True


class InitSchemaRequest(BaseModel):
    fields: list[FieldSpec] = Field(min_length=1)


class RenameRequest(BaseModel):
    field_id: str = Field(min_length=1)
    new_name: str = Field(min_length=1)


class TightenRequest(BaseModel):
    field_id: str = Field(min_length=1)
