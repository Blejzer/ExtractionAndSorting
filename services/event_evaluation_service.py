"""Structured, source-preserving parsing of event-wide evaluation summaries."""

import csv
import hashlib
from io import StringIO
import math
import re
import unicodedata

MAX_REPORT_CHARS = 200_000
RATINGS = {
    "rate the overall training": "overall_training",
    "the presented material is relevant to my work": "material_relevance",
    "the instructors were clear in their presentation": "instructor_clarity",
    "instructors provided enough practical examples": "practical_examples",
    "instructors were available for questions and discussion": "instructor_availability",
    "knowledge exchange outside of the classroom": "knowledge_exchange",
    "established contacts will be useful for future work": "useful_contacts",
    "was the training facility adequate": "training_facility",
    "was the lodging adequate": "lodging",
    "was the food adequate": "food",
    "please evaluate translation": "translation",
}
CHOICES = {
    "are you an instructor": "instructor",
    "would you recommend this training to your colleagues (yes, no, maybe)": "recommendation",
}
METRICS = {
    "total": ("overall_average", "Overall average", "score"),
    "overall satisfaction rate": ("satisfaction", "Overall satisfaction rate", "%"),
    "pretest": ("pretest", "Pretest", "%"),
    "posttest": ("posttest", "Posttest", "%"),
    "average increase in knowledge": ("reported_knowledge_increase", "Reported knowledge increase", "%"),
}
CATEGORIES = {
    "recommendations": "Recommendations and proposals",
    "requested_topics": "Requested future training topics",
    "other_comments": "Other comments and thoughts",
}
_NUMBER = re.compile(r"^([+-]?\d+(?:[.,]\d+)?)\s*(%)?$")
_SCALE = re.compile(r"\bscale\s*(\d+(?:[.,]\d+)?)\s*[-–—]\s*(\d+(?:[.,]\d+)?)", re.I)
_COUNT = re.compile(r"(\d+)\s*(YES|NO|MAYBE|Y|N|M)(?=\s|[,;/]|$|\d)", re.I)
_OPTIONS = {"yes": "yes", "y": "yes", "no": "no", "n": "no", "maybe": "maybe", "m": "maybe"}


class EvaluationParseError(ValueError):
    pass


def _label(value):
    text = unicodedata.normalize("NFKC", value).casefold()
    text = re.sub(r"^\s*\d+[.)]\s*", "", text)
    return " ".join(text.split()).strip(" :?.–—-")


def _number(value):
    match = _NUMBER.fullmatch(value.strip())
    if not match:
        return None
    number = float(match[1].replace(",", "."))
    return (number, bool(match[2])) if math.isfinite(number) else None


def _counts(value):
    matches = list(_COUNT.finditer(value))
    if not matches or _COUNT.sub("", value).strip(" \t,;/"):
        return None
    counts = {}
    for match in matches:
        option = _OPTIONS[match[2].casefold()]
        if option in counts:
            return None
        try:
            counts[option] = int(match[1])
        except ValueError:
            return None
    return counts


def _category(line):
    text = _label(line)
    if text.startswith(("your recommendations", "recommendations and proposals")):
        return "recommendations"
    if text.startswith("what training topics"):
        return "requested_topics"
    if text in ("other comments", "other comments and thoughts"):
        return "other_comments"
    return None


def _comments(lines):
    rows, pending, category = [], [], "other_comments"
    def flush():
        if pending:
            rows.append({"category": category, "text": " ".join(pending)})
            pending.clear()
    for line in lines:
        new_category = _category(line)
        if new_category:
            flush()
            category = new_category
        elif _label(line) == "comments" or not line.strip():
            flush()
        else:
            bullet = re.match(r"^\s*(?:[•*]\s+|[-–]\s+|\d+[.)]\s+)(.+)$", line)
            if bullet:
                flush()
            pending.append(bullet[1].strip() if bullet else line.strip())
    flush()
    return rows


def parse_evaluation(text):
    if not isinstance(text, str) or not text.strip():
        raise EvaluationParseError("Paste the event evaluation report.")
    if len(text) > MAX_REPORT_CHARS:
        raise EvaluationParseError("Paste at most 200,000 characters.")
    lines = text.replace("\r\n", "\n").replace("\r", "\n").replace("\xa0", " ").splitlines()
    report = dict(schema_version=1, kind="event_summary", scale_min=None, scale_max=None,
                  ratings=[], metrics=[], choices=[], comments=[], warnings=[], errors=[])
    comment_start = next((i for i, line in enumerate(lines)
                          if _label(line) == "comments" or _category(line)), len(lines))
    scales = {(float(a.replace(",", ".")), float(b.replace(",", ".")))
              for a, b in _SCALE.findall("\n".join(lines[:comment_start]))}
    if any(re.match(r"^\s*training\s+evaluation.*\bscale", line, re.I)
           for line in lines[comment_start + 1:]):
        report["errors"].append("More than one evaluation summary was pasted. Save one event summary at a time.")
    if len(scales) == 1:
        low, high = next(iter(scales))
        if math.isfinite(low) and math.isfinite(high) and low < high:
            report.update(scale_min=low, scale_max=high)
        else:
            report["errors"].append("The rating scale must have a finite minimum below its maximum.")
    elif len(scales) > 1:
        report["errors"].append("Different rating scales were found. Paste one event summary.")
    seen, ignored, started, i = set(), [], False, 0
    while i < len(lines):
        line = lines[i].strip()
        if i == comment_start:
            report["comments"] = _comments(lines[i:])
            break
        if not line or _SCALE.search(line):
            started = started or bool(_SCALE.search(line))
            i += 1
            continue
        cells = [cell.strip() for cell in re.split(r"\t+|\s*\|\s*", line.strip("|")) if cell.strip()]
        question, answer, consumed = line, None, i
        if len(cells) > 1 and _counts(" ".join(cells[1:])):
            question, answer = cells[0], " ".join(cells[1:])
        elif len(cells) > 1 and (_number(cells[-1]) or _counts(cells[-1])):
            question, answer = " ".join(cells[:-1]), cells[-1]
        else:
            inline = re.match(r"^(.+?)\s{2,}(.+)$", line)
            if inline and (_number(inline[2]) or _counts(inline[2])):
                question, answer = inline[1], inline[2]
            if answer is None:
                inline = re.match(r"^(.+?)[\s:]+([+-]?\d+(?:[.,]\d+)?\s*%?|\d+\s*[YNM](?:\s+\d+\s*[YNM])*)$", line, re.I)
                if inline and _label(inline[1]) in {*RATINGS, *METRICS, *CHOICES}:
                    question, answer = inline[1], inline[2]
            if answer is None:
                j = i + 1
                while j < len(lines) and not lines[j].strip():
                    j += 1
                if j < len(lines) and (_number(lines[j]) or _counts(lines[j])):
                    answer, consumed = lines[j].strip(), j
        label = _label(question)
        if label.startswith("would you recommend this training to your colleagues"):
            label = "would you recommend this training to your colleagues (yes, no, maybe)"
        known = label in RATINGS or label in METRICS or label in CHOICES
        if answer is None:
            if known and not (label == "total" and ":" not in question):
                report["warnings"].append(f"No numeric/count answer found for: {question}")
            ignored.append(line)
            i += 1
            continue
        if not known and not started and not question.rstrip().endswith("?"):
            ignored.append(line)
            i += 1
            continue
        started = True
        number, counts = _number(answer), _counts(answer)
        key = RATINGS.get(label) or CHOICES.get(label) or (METRICS.get(label) or (
            "question_" + hashlib.sha256(label.encode()).hexdigest()[:16],))[0]
        if key in seen:
            report["errors"].append(f"Question repeated: {question}. Paste one event summary.")
            i = consumed + 1
            continue
        seen.add(key)
        if counts is not None:
            if label in RATINGS or label in METRICS:
                report["errors"].append(f"A numeric score was expected for: {question}")
            else:
                report["choices"].append(dict(key=key, question=question, counts=counts,
                                              responses=sum(counts.values())))
        elif label in CHOICES:
            report["errors"].append(f"Yes/No counts were expected for: {question}")
        elif label in METRICS or number[1]:
            metric = METRICS.get(label, (key, question, "%"))
            value = number[0]
            if number[1] and (label in RATINGS or metric[2] == "score"):
                report["errors"].append(f"A score, not a percentage, was expected for: {question}")
            if metric[2] == "%" and not (-100 if metric[0] == "reported_knowledge_increase" else 0) <= value <= 100:
                report["errors"].append(f"Percentage outside the valid range: {question}")
            if metric[2] == "score" and report["scale_min"] is not None and not report["scale_min"] <= value <= report["scale_max"]:
                report["errors"].append(f"Score outside the stated scale: {question}")
            report["metrics"].append(dict(key=metric[0], label=metric[1], value=value, unit=metric[2]))
        else:
            value = number[0]
            if report["scale_min"] is not None and not report["scale_min"] <= value <= report["scale_max"]:
                report["errors"].append(f"Rating outside the stated scale: {question}")
            report["ratings"].append(dict(key=key, question=question, value=value))
        i = consumed + 1
    if report["ratings"] and report["scale_min"] is None:
        report["warnings"].append("No rating scale was stated; scores remain on their reported scale.")
    if not any(report[field] for field in ("ratings", "metrics", "choices", "comments")):
        report["warnings"].append("No structured answers were recognized. Check the text before saving it for reference.")
    report["unparsed_header"] = "\n".join(ignored).strip()
    report["warnings"] = list(dict.fromkeys(report["warnings"]))
    return report


def evaluation_analysis(report):
    metrics = {row["key"]: row["value"] for row in report.get("metrics", [])}
    gain = round(metrics["posttest"] - metrics["pretest"], 2) if "pretest" in metrics and "posttest" in metrics else None
    return dict(knowledge_gain_pp=gain, comment_groups=[
        dict(key=key, label=label, comments=[row["text"] for row in report.get("comments", [])
                                          if row["category"] == key])
        for key, label in CATEGORIES.items()])


def evaluation_csv(event_id, report):
    output = StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(["event_id", "record_type", "question_key", "question", "value", "unit",
                     "option", "comment_category", "comment", "scale_min", "scale_max"])
    def write(kind, key, question="", value="", unit="", option="", category="", comment=""):
        scored = kind == "rating" or unit == "score"
        row = [event_id, kind, key, question, value, unit, option, category, comment,
               report.get("scale_min") if scored else None, report.get("scale_max") if scored else None]
        writer.writerow(["'" + v if isinstance(v, str) and v.lstrip().startswith(("=", "+", "-", "@")) else v for v in row])
    for row in report.get("ratings", []):
        write("rating", row["key"], row["question"], row["value"], "score")
    for row in report.get("metrics", []):
        write("metric", row["key"], row["label"], row["value"], row["unit"])
    gain = evaluation_analysis(report)["knowledge_gain_pp"]
    if gain is not None:
        write("derived_metric", "knowledge_gain_pp", "Pre/post difference", gain, "percentage_points")
    for row in report.get("choices", []):
        for option, value in row["counts"].items():
            write("count", row["key"], row["question"], value, "responses", option)
    for index, row in enumerate(report.get("comments", []), 1):
        write("comment", f"comment_{index}", category=row["category"], comment=row["text"])
    return output.getvalue().encode("utf-8-sig")
