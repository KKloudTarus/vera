"""MCP tool-visibility profiles for coding agents and compatibility clients."""

from __future__ import annotations

from vera.config.settings import McpToolProfile

TOOL_PROFILES: tuple[McpToolProfile, ...] = ("coding", "advanced", "compatibility")

CODING_TOOLS = frozenset(
    {
        "knowledge_bootstrap",
        "knowledge_get_context",
        "knowledge_get_context_pack",
        "knowledge_search",
        "knowledge_explain_fact",
        "knowledge_get_evidence",
        "knowledge_feedback",
        "knowledge_propose",
        "knowledge_retract_proposal",
        "knowledge_proposal_report",
    }
)


def tool_is_visible(profile: McpToolProfile, name: str) -> bool:
    """Return whether a registered tool belongs to the configured discovery profile."""
    if name.startswith("memory_"):
        return profile == "compatibility"
    if name.startswith("knowledge_"):
        return profile != "coding" or name in CODING_TOOLS
    return True
