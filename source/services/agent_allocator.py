from __future__ import annotations

from schemas.agent_assigment import AgentAssignment


def allocate_urls_to_agents(urls: list[str], agent_count: int) -> list[AgentAssignment]:
    if agent_count < 1:
        raise ValueError("agent_count must be at least 1")

    assignments: list[AgentAssignment] = [
        {
            "agent_index": agent_index,
            "status": "queued" if urls else "idle",
            "urls": [],
            "url_count": 0,
        }
        for agent_index in range(agent_count)
    ]

    for url_index, raw_url in enumerate(urls):
        url = raw_url.strip()
        if not url:
            continue

        assignment = assignments[url_index % len(assignments)]
        assignment["urls"].append(url)

    for assignment in assignments:
        assignment["url_count"] = len(assignment["urls"])
        if assignment["url_count"] == 0:
            assignment["status"] = "idle"

    return assignments
