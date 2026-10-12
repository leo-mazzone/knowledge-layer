"""Readable sections and attachment links from Content API HTML fragments."""

from html.parser import HTMLParser

from knowledge.models import Section


class ContentHTML(HTMLParser):
    def __init__(self, headings=(), locator=None):
        super().__init__(convert_charrefs=True)
        self.headings = list(headings)
        self.prefix = len(headings)
        self.heading_stack = []
        self.base_locator = locator
        self.locator = locator
        self.text = []
        self.sections = []
        self.links = []
        self.heading = None
        self.heading_text = []
        self.heading_anchor = None
        self.ignored = 0
        self.active_link = None

    def flush(self):
        text = "\n".join(" ".join(line.split()) for line in "".join(self.text).splitlines()).strip()
        if text:
            self.sections.append(Section(text, tuple(self.headings), self.locator))
        self.text = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag in ("script", "style"):
            self.ignored += 1
        if self.ignored:
            return
        if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            self.flush()
            self.heading = int(tag[1])
            self.heading_text = []
            self.heading_anchor = attrs.get("id")
        elif tag == "a":
            self.active_link = {"url": attrs.get("href", ""), "title": ""}
            if self.heading and not self.heading_anchor:
                self.heading_anchor = attrs.get("id") or attrs.get("name")
        elif tag in ("p", "div", "ul", "ol", "li", "tr", "br", "section"):
            self.text.append("\n")
            if tag == "li":
                self.text.append("- ")
        elif tag in ("td", "th"):
            self.text.append(" | ")

    def handle_endtag(self, tag):
        if tag in ("script", "style") and self.ignored:
            self.ignored -= 1
            return
        if self.ignored:
            return
        if self.heading and tag == f"h{self.heading}":
            title = " ".join("".join(self.heading_text).split())
            while self.heading_stack and self.heading_stack[-1][0] >= self.heading:
                self.heading_stack.pop()
            if title:
                self.heading_stack.append((self.heading, title))
            self.headings = self.headings[: self.prefix] + [
                entry[1] for entry in self.heading_stack
            ]
            if not (self.base_locator or "").startswith("part:"):
                self.locator = self.heading_anchor or self.base_locator
            self.text.append(title + "\n")
            self.heading = None
        elif tag == "a" and self.active_link is not None:
            self.active_link["title"] = " ".join(self.active_link["title"].split())
            self.links.append(self.active_link)
            self.active_link = None
        elif tag in ("p", "div", "li", "tr", "section"):
            self.text.append("\n")

    def handle_data(self, data):
        if self.ignored:
            return
        if self.heading:
            self.heading_text.append(data)
        else:
            self.text.append(data)
        if self.active_link is not None:
            self.active_link["title"] += data


def html_sections(body, headings=(), locator=None):
    if not isinstance(body, str) or not body.strip():
        return []
    parser = ContentHTML(headings, locator)
    parser.feed(body)
    parser.close()
    parser.flush()
    return parser.sections


def attachment_links(details):
    """Prefer structured metadata; legacy documents contains rendered links."""
    attachments = [dict(item) for item in details.get("attachments") or []]
    known = {item.get("url") for item in attachments}
    for fragment in details.get("documents") or []:
        if not isinstance(fragment, str):
            continue
        parser = ContentHTML()
        parser.feed(fragment)
        for item in parser.links:
            if item["url"] and item["url"] not in known:
                attachments.append(item)
                known.add(item["url"])
    return attachments
