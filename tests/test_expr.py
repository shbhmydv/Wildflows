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
    Seq,
    Setup,
    Until,
    assign_node_ids,
    parse_expr,
)


def test_all_eight_expression_kinds_construct() -> None:
    do = Do(task="write", rig=RigRef(name="echo"))
    dispatch = Dispatch(children=[do, do])
    seq = Seq(children=[do, do])
    combine = Combine(task="merge", rig=RigRef(name="echo"), inputs=[do])
    loop = Loop(body=do, until=Until(kind="flag"), cap=3)
    inplace = Inplace(edits=[Edit(path="a.txt", content="x")])
    ask = Ask(question="which?")
    setup = Setup(cmd="npm ci")
    for e in (do, dispatch, seq, combine, loop, inplace, ask, setup):
        assert e.node_id == ""  # unassigned until admitted


def test_seq_is_ordered_and_assigns_child_ids() -> None:
    tree = Seq(
        children=[
            Inplace(edits=[Edit(path="a.txt", content="x")]),
            Do(task="then", rig=RigRef(name="echo")),
        ]
    )
    assign_node_ids(tree)
    assert tree.kind == "seq"
    assert tree.node_id == "n0"
    assert [c.node_id for c in tree.children] == ["n0.0", "n0.1"]
    back = parse_expr(tree.model_dump())
    assert isinstance(back, Seq)


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


@pytest.mark.parametrize("bad", ["/abs/f", "../escape", "a/../b", ".git", ".git/config", "-rf"])
def test_edit_rejects_unsafe_paths_at_admission(bad: str) -> None:
    # Lexical containment guards are local Pydantic invariants (item 5): absolute, `..`,
    # `.git`, and option-like paths never reach the engine.
    with pytest.raises(ValidationError):
        Edit(path=bad, content="x")


@pytest.mark.parametrize("bad", ["/abs/f", "../escape", ".git/config"])
def test_file_ctx_ref_rejects_unsafe_paths_at_admission(bad: str) -> None:
    from wildflows.expr import CtxRef

    with pytest.raises(ValidationError):
        CtxRef(kind="file", ref=bad)


def test_node_ctx_ref_is_not_path_checked() -> None:
    from wildflows.expr import CtxRef

    # A node ref is a node id, not a path — a leading `n` id like "n0.1" is fine.
    assert CtxRef(kind="node", ref="n0.1").ref == "n0.1"


def test_inplace_rejects_duplicate_paths() -> None:
    with pytest.raises(ValidationError):
        Inplace(edits=[Edit(path="a.txt", content="x"), Edit(path="a.txt", content="y")])
