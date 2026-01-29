from eeql.engine import parser
from eeql.ast import SelectorKind, JoinQualifier


def test_parse_option_b_with_join():
    text = """
    select first user_signed_up (
      user_id as user_id,
      ts as signup_ts
    )
    join after user_logged_in (
      using user_id
      count(event_id) as login_count,
      last_value(ts) as last_login_ts,
      filter(status = 'ok')
    )
    """
    ast = parser.parse_with_spans(text)

    assert ast.select.event_name == "user_signed_up"
    assert ast.select.selector.kind == SelectorKind.FIRST
    assert len(ast.select.columns) == 2

    assert len(ast.joins) == 1
    join = ast.joins[0]
    assert join.qualifier == JoinQualifier.AFTER
    assert join.selector.kind == SelectorKind.OMITTED
    assert join.using_entities == ["user_id"]
    assert join.filter is not None
    assert {c.alias for c in join.columns} == {"login_count", "last_login_ts"}
