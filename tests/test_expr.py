"""Expression data model: all 7 primitives representable + node-id assignment."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from wildflows.expr import (
    Ask,
    Combine,
    Dispatch,
    Do,
    Edit,
    Inplace,
    Loop,
    RigRef,
    Setup,
    Until,
    assign_node_ids,
    parse_expr,
)


def test_all_seven_primitives_construct() -> None:
    do = Do(task="write", rig=RigRef(name="echo"))
    dispatch = Dispatch(children=[do, do])
    combine = Combine(task="merge", rig=RigRef(name="echo"), inputs=[do])
    loop = Loop(body=do, until=Until(kind="flag"), cap=3)
    inplace = Inplace(edits=[Edit(path="a.txt", content="x")])
    ask = Ask(question="which?")
    setup = Setup(cmd="npm ci")
    for e in (do, dispatch, combine, loop, inplace, ask, setup):
        assert e.node_id == ""  # unassigned until admitted


def test_discriminated_parse_roundtrip() -> None:
    tree = Dispatch(children=[Do(task="t", rig=RigRef(name="echo"))])
    data = tree.model_dump()
    back = parse_expr(data)
    assert isinstance(back, Dispatch)
    assert isinstance(back.children[0], Do)


def test_assign_node_ids_is_deterministic_preorder() -> None:
    tree = Dispatch(
        children=[
            Do(task="a", rig=RigRef(name="echo")),
            Loop(body=Do(task="b", rig=RigRef(name="echo")), until=Until(kind="flag"), cap=2),
        ]
    )
    assign_node_ids(tree)
    assert tree.node_id == "n0"
    assert tree.children[0].node_id == "n0.0"
    assert tree.children[1].node_id == "n0.1"
    loop = tree.children[1]
    assert isinstance(loop, Loop)
    assert loop.body.node_id == "n0.1.0"
    # stable across a second identical build
    tree2 = Dispatch(
        children=[
            Do(task="a", rig=RigRef(name="echo")),
            Loop(body=Do(task="b", rig=RigRef(name="echo")), until=Until(kind="flag"), cap=2),
        ]
    )
    assign_node_ids(tree2)
    assert tree2.children[1].node_id == "n0.1"


def test_loop_cap_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        Loop(body=Do(task="b", rig=RigRef(name="echo")), until=Until(kind="flag"), cap=0)
