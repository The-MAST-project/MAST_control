"""The controller applies the IERS policy before it builds anything (MAST_common#139).

The scheduler reaches `Site.observing_window()`, which transforms the Sun and so needs
UT1-UTC. astropy raises once its table's predictions are more than 30 days old, which is
what stopped a unit's mount on 2026-10-01. `common` guards that one call, so this is not
the only protection -- it is what makes the policy explicit and logged at startup.

AST, not import: importing app.py builds a Controller's dependencies and configures logging,
and this must run anywhere.
"""

import ast
import pathlib

APP = pathlib.Path(__file__).resolve().parent.parent / "app.py"


def test_policy_is_applied_at_module_scope():
    module = ast.parse(APP.read_text(encoding="utf-8"))
    calls = [
        node.lineno
        for node in module.body
        if isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Call)
        and ast.unparse(node.value.func) == "iers_policy.configure_astropy"
    ]
    assert calls, "app.py does not call iers_policy.configure_astropy() at module scope"


def test_policy_is_in_force_before_the_controller_exists():
    """Module scope runs at import, the lifespan after: so a module-scope call precedes
    every `Controller()`. This pins that no one moves it down into the lifespan, where it
    would run after the app is built."""
    module = ast.parse(APP.read_text(encoding="utf-8"))
    lifespan = next(n for n in ast.walk(module) if isinstance(n, ast.AsyncFunctionDef) and n.name == "lifespan")
    inside = [
        n.lineno
        for n in ast.walk(lifespan)
        if isinstance(n, ast.Call) and ast.unparse(n.func) == "iers_policy.configure_astropy"
    ]
    assert not inside, "configure_astropy() belongs at module scope, not in the lifespan"
