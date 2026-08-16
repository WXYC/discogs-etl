"""A YAML loader that understands CloudFormation's short-form intrinsic tags.

``yaml.safe_load`` raises ``ConstructorError`` on ``!Ref`` / ``!Sub`` /
``!GetAtt`` / ``!If`` / ``!Not`` / ``!Equals``, which every template in
``infra/`` uses. Intrinsics are preserved as their long-form dicts
(``!Ref X`` -> ``{"Ref": "X"}``) so assertions can distinguish a literal from
an unresolved reference -- a distinction that is load-bearing in
``test_ephemeral_rebuild_template.py``, where ``State: !Ref ScheduleState`` and
``Enabled: !Ref ScheduleEnabled`` are semantically different despite looking
alike.

Shared rather than duplicated: this is the second template under test, and a
per-file copy is how a repo ends up with several subtly different loaders.
"""

from __future__ import annotations

from typing import Any

import yaml


class CfnLoader(yaml.SafeLoader):
    """SafeLoader extended with CloudFormation's ``!`` tag family."""


def _construct_cfn_tag(loader: yaml.Loader, tag_suffix: str, node: yaml.Node) -> Any:
    key = "Ref" if tag_suffix == "Ref" else f"Fn::{tag_suffix}"
    if isinstance(node, yaml.ScalarNode):
        value: Any = loader.construct_scalar(node)
        if key == "Fn::GetAtt":
            value = value.split(".")
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    else:
        value = loader.construct_mapping(node, deep=True)
    return {key: value}


CfnLoader.add_multi_constructor("!", _construct_cfn_tag)


def load_cfn(path: Any) -> dict[str, Any]:
    """Parse a CloudFormation template, intrinsics preserved as dicts."""
    with open(path, encoding="utf-8") as fh:
        return yaml.load(fh, Loader=CfnLoader)  # noqa: S506 -- CfnLoader subclasses SafeLoader
