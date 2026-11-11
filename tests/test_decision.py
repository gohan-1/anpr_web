"""One test per scenario. Run: pytest -q"""
from pipeline.decision import ImageResult as R, Route, decide


def test_s1_single_clear_high_confidence():
    d = decide([R("a", quality=0.9, plate="MH12AB1234", confidence=0.95)])
    assert (d.route, d.scenario, d.plate) == (Route.ACCEPTED, "S1", "MH12AB1234")


def test_s2_blurry_ignored_clear_one_decides():
    d = decide([
        R("blurry", quality=0.15, plate="MH12A81234", confidence=0.55),
        R("clear", quality=0.9, plate="MH12AB1234", confidence=0.96),
    ])
    assert (d.route, d.scenario, d.plate, d.best_image_id) == (Route.ACCEPTED, "S2", "MH12AB1234", "clear")


def test_s3_same_plate_takes_highest_with_consensus():
    d = decide([
        R("a", quality=0.8, plate="MH12AB1234", confidence=0.82),
        R("b", quality=0.85, plate="mh-12 ab 1234", confidence=0.93),   # normalised
        R("c", quality=0.7, plate="MH12AB1234", confidence=0.88),
    ])
    assert (d.route, d.scenario, d.best_image_id) == (Route.ACCEPTED, "S3", "b")
    assert d.candidates["MH12AB1234"]["count"] == 3


def test_s4_conflicting_plates():
    d = decide([
        R("a", quality=0.9, plate="MH12AB1234", confidence=0.94),
        R("b", quality=0.9, plate="MH12AB1284", confidence=0.91),
    ])
    assert (d.route, d.scenario) == (Route.REVIEW, "S4")


def test_s5_high_confidence_low_quality():
    d = decide([R("a", quality=0.2, plate="MH12AB1234", confidence=0.97)])
    assert (d.route, d.scenario) == (Route.REVIEW, "S5")


def test_s6_no_plate_detected():
    d = decide([R("a", quality=0.9), R("b", quality=0.8)])
    assert (d.route, d.scenario) == (Route.NO_PLATE, "S6")


def test_s7_low_score():
    d = decide([R("a", quality=0.6, plate="MH12AB1234", confidence=0.62)])
    assert (d.route, d.scenario) == (Route.REVIEW, "S7")


def test_s8_bad_format():
    d = decide([R("a", quality=0.9, plate="AB", confidence=0.95)])
    assert (d.route, d.scenario) == (Route.REVIEW, "S8")


def test_s9_all_api_errors():
    d = decide([R("a", quality=0.9, error="timeout"), R("b", quality=0.9, error="500")])
    assert (d.route, d.scenario) == (Route.FAILED, "S9")


def test_s9_partial_errors_no_plate_means_retry():
    d = decide([R("a", quality=0.9), R("b", quality=0.9, error="timeout")])
    assert (d.route, d.scenario) == (Route.FAILED, "S9")


def test_s10_duplicates_are_skipped():
    d = decide([
        R("a", quality=0.9, plate="MH12AB1234", confidence=0.95),
        R("a_copy", duplicate=True),
    ])
    assert (d.route, d.scenario) == (Route.ACCEPTED, "S1")


def test_s11_all_corrupt():
    d = decide([R("a", corrupt=True), R("b", corrupt=True)])
    assert (d.route, d.scenario) == (Route.FAILED, "S11")


def test_accept_survives_one_api_error():
    d = decide([
        R("a", quality=0.9, plate="MH12AB1234", confidence=0.95),
        R("b", quality=0.9, error="timeout"),
    ])
    assert d.route == Route.ACCEPTED and "API failed" in d.reason
