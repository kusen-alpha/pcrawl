from __future__ import annotations

from pcrawl import AsyncSpider, Item, PoolSpec, ResourceSetup, ResourceSpec, Settings, Task


class AccountSessionSpider(AsyncSpider):
    """Generic session-resource example.

    pcrawl only knows that ``account`` is a lease. How the account payload is
    used (HTTP login, browser context, API client, ...) stays in this spider.
    ``start_resources`` is mode-neutral: the engine uses an embedded purser
    here and a LocalScheduler if this class is switched to a thread/process
    Spider.
    """

    name = "account-session"
    resources = {"account": ResourceSpec(pool="account", use_limit=10)}
    settings = Settings(concurrency=2, resource_wait_timeout=5)

    async def start_resources(self):
        return ResourceSetup(
            pools={
                "account": PoolSpec(
                    kind="account",
                    config={"max_slots": 1},
                    seed=[
                        {"id": "acct-1", "username": "user-1", "password": "secret-1"},
                        {"id": "acct-2", "username": "user-2", "password": "secret-2"},
                    ],
                    import_options={"import_validate": "on_import"},
                )
            }
        )

    def start_tasks(self):
        for index in range(6):
            yield Task(payload={"query": f"item-{index}"})

    def open_session(self, session):
        account = session.get("account")
        session.user["account"] = account.payload

    async def start_requests(self, task, ctx):
        account = ctx.user["account"]
        ctx.get_lease("account").mark_used()
        yield Item(
            query=task.payload["query"],
            account=account.get("username") or account.get("id"),
            session_id=ctx.session.id,
        )

    def close_session(self, session, reason):
        session.user.clear()
