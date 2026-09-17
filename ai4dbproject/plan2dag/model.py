import re
from dataclasses import dataclass, field

_NOISE = re.compile(r"\boptional:\s*[^,]*$", re.IGNORECASE)
_CAST = re.compile(r"::[a-zA-Z_][\w\s]*(\[\])?")


@dataclass
class OpNode:
    op: str
    out_rows: int = 0
    in_rows: int | None = None
    table: str | None = None
    predicate: str | None = None
    join_cond: str | None = None
    group_keys: list[str] = field(default_factory=list)
    raw_name: str = ""
    children: list["OpNode"] = field(default_factory=list)

    def signature(self, child_sigs: tuple) -> tuple:
        return (
            self.op,
            self.table,
            _norm(self.predicate),
            _norm(self.join_cond),
            tuple(_norm(k) for k in self.group_keys),
            child_sigs,
        )


def _norm(s):
    if s is None:
        return None
    s = str(s)
    s = _NOISE.sub("", s).strip()
    s = re.sub(r"\bAND\s*$", "", s).strip()
    s = _CAST.sub("", s)
    s = s.replace("(", "").replace(")", "").replace("::", "")
    return " ".join(s.split())
