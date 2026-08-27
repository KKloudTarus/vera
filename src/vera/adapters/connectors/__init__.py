"""Source connectors: map external systems' records to pipeline artifacts."""

from vera.adapters.connectors.cmdb import CmdbConnector
from vera.adapters.connectors.confluence import ConfluenceConnector
from vera.adapters.connectors.git import GitConnector
from vera.adapters.connectors.jira import JiraConnector
from vera.adapters.connectors.pdf import PdfConnector
from vera.adapters.connectors.slack import SlackConnector

__all__ = [
    "CmdbConnector",
    "ConfluenceConnector",
    "GitConnector",
    "JiraConnector",
    "PdfConnector",
    "SlackConnector",
]
