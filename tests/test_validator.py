import pytest

from eeql.engine import parser, validator
from eeql.engine.validator import ValidationError


def test_validator_requires_aggregation_when_selector_omitted(sample_catalog):
    text = """
    select first user_signed_up (
      user_id as user_id
    )
    join after user_logged_in (
      using user_id
      event_id as event_id
    )
    """
    ast = parser.parse_with_spans(text)
    with pytest.raises(ValidationError) as excinfo:
        validator.validate_query(ast, sample_catalog)
    assert "requires aggregated column" in str(excinfo.value)


def test_validator_passes_option_b(sample_catalog):
    text = """
    select first user_signed_up (
      user_id as user_id,
      ts as signup_ts
    )
    join after user_logged_in (
      using user_id
      count(event_id) as login_count,
      last_value(ts) as last_login_ts
    )
    """
    ast = parser.parse_with_spans(text)
    validator.validate_query(ast, sample_catalog)  # should not raise


def test_validator_unknown_event(sample_catalog):
    text = """
    select made_up_event (
      id as id
    )
    """
    ast = parser.parse_with_spans(text)
    with pytest.raises(ValidationError) as excinfo:
        validator.validate_query(ast, sample_catalog)
    assert "Unknown event" in str(excinfo.value)
