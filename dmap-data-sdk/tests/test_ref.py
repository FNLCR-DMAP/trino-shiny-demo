"""Tests for Ref class - version reference logic."""
import pytest
from dmap_data_sdk import Ref


def test_ref_pretty_formatting():
    """Test Ref.pretty() returns correct string for each ref type (used in lineage JSON)."""
    assert Ref(snapshot_id=12345).pretty() == "snapshot:12345"
    assert Ref(as_of_ts_millis=1700000000000).pretty() == "ts:1700000000000"
    assert Ref(branch="feature-x").pretty() == "branch:feature-x"
    assert Ref(tag="v1.0").pretty() == "tag:v1.0"
    assert Ref().pretty() == "latest"


def test_ref_is_unset():
    """Test is_unset() returns True when no dimension is provided."""
    assert Ref().is_unset() is True
    assert Ref(branch="main").is_unset() is False
    assert Ref(snapshot_id=123).is_unset() is False


def test_ref_immutability():
    """Test Ref is frozen and cannot be modified."""
    ref = Ref(branch="main")
    with pytest.raises(Exception):  # FrozenInstanceError
        ref.branch = "feature"  # type: ignore
