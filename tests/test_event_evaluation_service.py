"""Evaluation fixtures use generic form labels and fictional comments."""

import csv
from io import StringIO

import pytest

from services.event_evaluation_service import (
    EvaluationParseError, parse_evaluation, evaluation_analysis, evaluation_csv,
)

RATING_ROWS = [
    ("Rate the overall training.", "5.00"),
    ("The presented material is relevant to my work.", "4.95"),
    ("The instructors were clear in their presentation.", "4.88"),
    ("Instructors provided enough practical examples.", "4.84"),
    ("Instructors were available for questions and discussion.", "5.00"),
    ("Knowledge exchange outside of the classroom", "4.86"),
    ("Established contacts will be useful for future work", "4.81"),
    ("Was the training facility adequate", "5.00"),
    ("Was the lodging adequate", "5.00"),
    ("Was the food adequate", "5.00"),
    ("Please evaluate translation", "4.95"),
]
REPORT = "\n\n".join([
    "EXAMPLE TRAINING", "TRAINING EVALUATION – SCALE 1-5", "Total",
    *[part for row in RATING_ROWS for part in row],
    "Are you an instructor", "12Y 9N",
    "Would you recommend this training to your colleagues (YES, NO, MAYBE)?", "21Y 0N 0M",
    "TOTAL:", "4.94", "OVERALL SATISFACTION RATE:", "98.73%",
    "PRETEST:", "65.65%", "POSTTEST:", "89.52%", "AVERAGE INCREASE IN KNOWLEDGE:", "23,87%",
    "COMMENTS:", "Your recommendations and proposals to improve the training:",
    "More practical examples.", "Discuss a sample case\nin small groups.",
    "What training topics would you want in coming years?",
    "Cryptocurrencies", "Financial Investigations", "Cryptocurrencies",
    "Other comments and thoughts:", "Excellent interpretation.", "=SUM(1,1)",
]) + "\n"


def test_aggregate_report_keeps_stated_results_and_categorized_comments():
    report = parse_evaluation(REPORT)
    assert report["errors"] == [] and report["warnings"] == []
    assert (report["scale_min"], report["scale_max"]) == (1, 5)
    ratings = {row["key"]: row["value"] for row in report["ratings"]}
    assert len(ratings) == 11
    assert ratings["overall_training"] == 5 and ratings["translation"] == 4.95
    assert ratings["instructor_clarity"] == 4.88 and ratings["practical_examples"] == 4.84
    metrics = {row["key"]: row["value"] for row in report["metrics"]}
    assert metrics == dict(overall_average=4.94, satisfaction=98.73, pretest=65.65,
                           posttest=89.52, reported_knowledge_increase=23.87)
    choices = {row["key"]: row for row in report["choices"]}
    assert choices["instructor"]["counts"] == dict(yes=12, no=9)
    assert choices["recommendation"]["counts"] == dict(yes=21, no=0, maybe=0)
    assert choices["recommendation"]["responses"] == 21
    assert "respondent_count" not in report
    assert all("responses" not in row for row in report["ratings"])
    analysis = evaluation_analysis(report)
    groups = {group["key"]: group["comments"] for group in analysis["comment_groups"]}
    assert groups["recommendations"] == ["More practical examples.", "Discuss a sample case in small groups."]
    assert groups["requested_topics"] == ["Cryptocurrencies", "Financial Investigations", "Cryptocurrencies"]
    assert groups["other_comments"] == ["Excellent interpretation.", "=SUM(1,1)"]
    assert analysis["knowledge_gain_pp"] == 23.87


@pytest.mark.parametrize("separator", ["\t", " | ", "  "])
def test_table_rows_and_decimal_commas(separator):
    report = parse_evaluation("TRAINING EVALUATION – SCALE 1-5\n"
                              + "Rate the overall training." + separator + "4,95\n"
                              + "PRETEST:" + separator + "65,65%\n")
    assert report["errors"] == []
    assert report["ratings"][0]["value"] == 4.95 and report["metrics"][0]["value"] == 65.65


def test_word_whitespace_and_choice_cells():
    report = parse_evaluation(REPORT.replace("4.95", "4,95\u00a0").replace("\n", "\r\n\r\n"))
    assert len(report["ratings"]) == 11 and report["errors"] == []
    choices = parse_evaluation("Are you an instructor\t12Y\t9N\n")["choices"]
    assert choices[0]["key"] == "instructor" and choices[0]["counts"] == dict(yes=12, no=9)


@pytest.mark.parametrize("text", [
    "SCALE 1-5\nRate the overall training.\n6",
    "SCALE 1-5\nTOTAL:\n0", "PRETEST:\n101%",
    "SCALE 1-5\nRate the overall training.\n95%", "SCALE 1-5\nTOTAL:\n4.94%",
    "SCALE 5-1\nRate the overall training.\n4",
    "SCALE 1-5\nSCALE 1-10\nRate the overall training.\n4",
    "SCALE 1-5\nRate the overall training.\n4\nRate the overall training.\n5",
])
def test_invalid_data_and_duplicate_questions_require_correction(text):
    assert parse_evaluation(text)["errors"]


def test_new_questions_have_keys_and_comments_do_not_change_scale():
    report = parse_evaluation("SCALE 1-7\nA future question?\n6\n")
    assert report["ratings"][0]["key"].startswith("question_")
    assert report["ratings"][0]["value"] == 6 and report["scale_max"] == 7
    report = parse_evaluation("SCALE 1-5\nRate the overall training.\n4\nCOMMENTS:\n"
                              "Other comments and thoughts:\nDiscuss a scale 1-10 example.\n")
    assert report["scale_max"] == 5 and report["errors"] == []


@pytest.mark.parametrize("text", ["", "  ", "x" * 200001])
def test_empty_and_oversize_text_is_rejected(text):
    with pytest.raises(EvaluationParseError):
        parse_evaluation(text)


def test_csv_has_keys_units_counts_safe_comments_and_distinct_derived_values():
    rows = list(csv.DictReader(StringIO(evaluation_csv("EXAMPLE", parse_evaluation(REPORT)).decode("utf-8-sig"))))
    assert all(row["event_id"] == "EXAMPLE" for row in rows)
    assert sum(row["record_type"] == "rating" for row in rows) == 11
    pct = next(row for row in rows if row["question_key"] == "satisfaction")
    assert pct["value"] == "98.73" and pct["unit"] == "%"
    assert pct["scale_min"] == "" and pct["scale_max"] == ""
    gain = next(row for row in rows if row["question_key"] == "knowledge_gain_pp")
    assert gain["record_type"] == "derived_metric" and gain["value"] == "23.87"
    assert gain["unit"] == "percentage_points"
    choices = [row for row in rows if row["question_key"] == "recommendation"]
    assert {row["option"]: row["value"] for row in choices} == dict(yes="21", no="0", maybe="0")
    assert any(row["comment"] == "'=SUM(1,1)" for row in rows)


def test_a_second_full_report_after_comments_is_not_silently_treated_as_comments():
    report = parse_evaluation(REPORT + "\n" + REPORT)
    assert any("More than one evaluation summary" in error for error in report["errors"])
