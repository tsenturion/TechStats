from __future__ import annotations

import casbin
from casbin import persist


MODEL_CONF = """
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act

[role_definition]
g = _, _

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = g(r.sub, p.sub) && keyMatch2(r.obj, p.obj) && regexMatch(r.act, p.act)
""".strip()


POLICY_LINES = [
    "p, guest, /api/v1/health*, (GET|HEAD)",
    "p, guest, /api/v1/metrics*, (GET|HEAD)",
    "p, guest, /api/v1/runtime-settings/public, (GET|HEAD)",
    "p, guest, /api/v1/cache/stats, (GET|HEAD)",
    "p, guest, /api/v1/analysis/results/*, (GET|HEAD)",
    "p, user, /api/v1/*, (GET|POST|PUT|PATCH|DELETE|HEAD)",
    "p, admin, /api/v1/*, (GET|POST|PUT|PATCH|DELETE|HEAD)",
    "g, user, guest",
    "g, admin, user",
]


class _InMemoryAdapter(persist.Adapter):
    def load_policy(self, model):
        for line in POLICY_LINES:
            persist.load_policy_line(line, model)

    def save_policy(self, model):  # pragma: no cover - not required at runtime
        return True

    def add_policy(self, sec, ptype, rule):  # pragma: no cover
        return None

    def remove_policy(self, sec, ptype, rule):  # pragma: no cover
        return None

    def remove_filtered_policy(self, sec, ptype, field_index, *field_values):  # pragma: no cover
        return None


def build_enforcer() -> casbin.Enforcer:
    model = casbin.Model()
    model.load_model_from_text(MODEL_CONF)
    enforcer = casbin.Enforcer(model, _InMemoryAdapter())
    enforcer.load_policy()
    return enforcer
