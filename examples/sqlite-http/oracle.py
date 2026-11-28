"""Client-side contract. The oracle observes only HTTP replies and the fault window."""


def checks(phase, operation, receipt, balance, killed_at):
    if phase == "retry":
        committed = killed_at == "after_commit"
        expected = {"count": int(committed), "total": operation["amount"] if committed else 0}
        return [compare("ledger.recovery", "wrong_state_after_restart", expected, balance)]
    if phase == "done":
        expected = {"count": 1, "total": operation["amount"]}
        return [compare("ledger.receipt", "incorrect_acknowledgement", operation, receipt),
                compare("ledger.once", "incorrect_durable_effect", expected, balance)]
    return []


def compare(prop, signature, expected, actual):
    result = {"id": prop, "ok": expected == actual}
    if not result["ok"]:
        result.update(signature=signature, details={"expected": expected, "actual": actual})
    return result
