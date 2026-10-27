"""Offline search workflow checks; never log in, save applicants or touch state files."""
import ast
import contextlib
import io
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import linkedin_search as search


def process_job_function(candidate_processor):
    # Load only orchestration: importing the full module requires live DB/browser deps.
    tree = ast.parse(Path("recruit_manual.py").read_text(encoding="utf-8"))
    function = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "process_job")
    namespace = dict(FINAL={"saved"}, process_candidate_profiles=candidate_processor,
                     update_job_processing_status=Mock(), datetime=datetime, timezone=timezone)
    exec(compile(ast.Module(body=[function], type_ignores=[]), "recruit_manual.py", "exec"), namespace)
    return namespace["process_job"]


class RequirementTests(unittest.TestCase):
    def test_advance_flag_parsing(self):
        for value, expected in (("true", True), ("TRUE", True), ("false", False), ("0", False)):
            with patch.dict(os.environ, {"Advance": value}):
                self.assertIs(search.advanced_search_enabled(), expected)

    def test_new_advanced_url_takes_priority_and_old_alias_works(self):
        with patch.dict(os.environ, {"ADVANCED_SEARCH_URL": "https://example.invalid/new",
                                     "LINKEDIN_ADVANCED_SEARCH_URL": "https://example.invalid/old"}):
            self.assertEqual(search.advanced_search_url(), "https://example.invalid/new")
        with patch.dict(os.environ, {"ADVANCED_SEARCH_URL": "",
                                     "LINKEDIN_ADVANCED_SEARCH_URL": "https://example.invalid/old"}):
            self.assertEqual(search.advanced_search_url(), "https://example.invalid/old")
    def test_missing_jd_does_not_use_stale_requirements(self):
        self.assertEqual(search.job_search_requirements(dict(skills=["Python"], experience="5 years")),
                         dict(has_jd=False, skills=[], experience=""))

    def test_explicit_experience_ranges(self):
        self.assertEqual(search.experience_bounds("3-5 years of experience"), (3, 5))
        self.assertEqual(search.experience_bounds("minimum 5 years"), (5, None))
        self.assertEqual(search.experience_bounds("5+ years"), (5, None))
        self.assertEqual(search.experience_bounds("up to 3 years"), (None, 3))
        self.assertEqual(search.experience_bounds("exactly 4 years"), (4, 4))

    def test_ambiguous_experience_is_not_guessed(self):
        for source in ("3 years Java and 5 years SQL", "5 years preferred", "6 months", "over 3 years", "7-2 years"):
            self.assertIsNone(search.experience_bounds(source), source)


class ExperienceEditorTests(unittest.IsolatedAsyncioTestCase):
    def field(self, initial):
        state = {"value": str(initial)}
        async def fill(value):
            state["value"] = value
        return SimpleNamespace(fill=AsyncMock(side_effect=fill), press=AsyncMock(),
                               input_value=AsyncMock(side_effect=lambda: state["value"]))

    async def check_range(self, source, expected):
        lower, upper = self.field(9), self.field(12)
        update = SimpleNamespace(click=AsyncMock())
        with patch.object(search, "experience_controls", AsyncMock(return_value=(lower, upper, update))), \
             contextlib.redirect_stdout(io.StringIO()):
            await search.apply_experience(None, source)
        self.assertEqual((await lower.input_value(), await upper.input_value()), expected)
        update.click.assert_awaited_once()

    async def test_minimum_uses_30_plus_and_clicks_update(self):
        await self.check_range("5+ years", ("5", "30"))

    async def test_range_sets_both_screenshot_fields(self):
        await self.check_range("3-7 years", ("3", "7"))

    async def test_maximum_uses_zero_lower_bound(self):
        await self.check_range("up to 4 years", ("0", "4"))

    async def test_missing_experience_leaves_editor_untouched(self):
        with patch.object(search, "experience_controls", AsyncMock()) as controls:
            await search.apply_experience(None, "")
        controls.assert_not_awaited()

    async def test_out_of_range_requirement_is_not_silently_clamped(self):
        with patch.object(search, "experience_controls", AsyncMock()) as controls:
            with self.assertRaises(RuntimeError):
                await search.apply_experience(None, "35+ years")
        controls.assert_not_awaited()

    async def test_changed_field_value_does_not_click_update(self):
        lower, upper = self.field(0), self.field(30)
        lower.input_value = AsyncMock(return_value="0")
        update = SimpleNamespace(click=AsyncMock())
        with patch.object(search, "experience_controls", AsyncMock(return_value=(lower, upper, update))):
            with self.assertRaises(RuntimeError):
                await search.apply_experience(None, "5+ years")
        update.click.assert_not_awaited()

class WorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def run_flow(self, has_jd=True, history=None, profiles=None, advanced_error=False, search_only=False, advance=True, full_location=None):
        advanced_enabled = advance
        events = []
        job = dict(code="TEST", title="Python Developer", cleaned_title="Python Developer",
                   country="Canada", location="Toronto, Ontario, Canada", business_unit="Canada",
                   location_variants=["Toronto, Ontario, Canada", "Ontario, Canada", "Canada"],
                   raw_jd="Needs Python and 5+ years" if has_jd else "",
                   skills=["Python"], experience="5+ years")
        if full_location is not None:
            job["location_details"] = {"full_location": full_location}
        args = SimpleNamespace(location=None, search_only=search_only, limit=3)
        history = history if history is not None else {}
        profiles = profiles if profiles is not None else {}

        async def advance(*_):
            events.append("advanced")
            if advanced_error:
                raise RuntimeError("Advanced UI unavailable")

        async def title(*_):
            events.append("title")

        async def collect(*_):
            events.append("collect")
            return ["shared", "advanced-only"] if events.count("collect") == 1 and has_jd and not advanced_error else ["shared", "normal-only"]

        async def candidates(api, args, profile_page, applicant_page, job, urls, store, persist):
            events.append("profiles")
            for url in urls:
                store[url] = {"status": "saved"}

        page = SimpleNamespace(goto=AsyncMock())
        api = SimpleNamespace(SEARCH_URL="https://example.invalid/search", apply_title=AsyncMock(side_effect=title),
                              apply_location=AsyncMock(), wait_for_search_results=AsyncMock(),
                              collect_profiles=AsyncMock(side_effect=collect), save_state=Mock())
        fn = process_job_function(AsyncMock(side_effect=candidates))
        with patch.dict(os.environ, {"Advance": str(advanced_enabled).lower()}), \
             patch.object(search, "open_advanced_search", AsyncMock(side_effect=advance)), \
             patch.object(search, "apply_jd_filters", AsyncMock()) as filters, \
             patch.object(search, "clear_search_filters", AsyncMock()), \
             patch.object(search, "wait_for_filter_step", AsyncMock()), \
             patch.object(search, "submit_search", AsyncMock()), contextlib.redirect_stdout(io.StringIO()):
            await fn(api, args, page, None, None, job, profiles, history, None, Mock())
        return events, history, profiles, api, filters

    async def test_advance_false_only_normal_and_ignores_previous_advanced_failure(self):
        history = {"TEST": {"advanced_search": {"status": "failed", "search_status": "failed"}}}
        events, history, _, api, filters = await self.run_flow(history=history, advance=False)
        self.assertNotIn("advanced", events)
        self.assertEqual(api.apply_title.await_count, 3)
        self.assertEqual(api.collect_profiles.await_count, 3)
        self.assertEqual(filters.await_count, 0)
        self.assertEqual(history["TEST"]["status"], "completed")
    async def test_advanced_searches_all_location_variants(self):
        events, history, _, api, _ = await self.run_flow()
        self.assertEqual(events.count("advanced"), 3)
        self.assertEqual([call.args[1] for call in api.apply_location.await_args_list[:3]],
                         ["Toronto, Ontario, Canada", "Ontario, Canada", "Canada"])
        self.assertEqual(history["TEST"]["advanced_search"]["filters"]["locations"],
                         ["Toronto, Ontario, Canada", "Ontario, Canada", "Canada"])
        self.assertEqual([call.args[1] for call in api.apply_location.await_args_list[3:]],
                         ["Toronto, Ontario, Canada", "Ontario, Canada", "Canada"])

    async def test_advanced_first_then_normal_merge_and_deduplicate(self):
        events, history, _, api, filters = await self.run_flow()
        self.assertLess(events.index("advanced"), events.index("title"))
        self.assertEqual(events[-1], "profiles")
        self.assertEqual(filters.await_count, 3)
        self.assertEqual(api.apply_title.await_count, 6)
        self.assertEqual(api.apply_location.await_args_list[0].args[1], "Toronto, Ontario, Canada")
        self.assertEqual(history["TEST"]["profiles"], ["shared", "advanced-only", "normal-only"])
        self.assertEqual(history["TEST"]["status"], "completed")

    async def test_no_jd_retains_normal_title_location_search(self):
        events, history, _, _, filters = await self.run_flow(has_jd=False)
        self.assertNotIn("advanced", events)
        self.assertEqual(filters.await_count, 0)
        self.assertNotIn("advanced_search", history["TEST"])

    async def test_restart_reuses_finished_searches(self):
        _, history, profiles, _, _ = await self.run_flow()
        events, _, _, api, filters = await self.run_flow(history=history, profiles=profiles)
        self.assertNotIn("advanced", events)
        self.assertEqual(api.collect_profiles.await_count, 0)
        self.assertEqual(filters.await_count, 0)

    async def test_changed_filter_signature_forces_new_searches(self):
        _, history, profiles, _, _ = await self.run_flow()
        history["TEST"]["advanced_search"]["filters"]["skills"] = ["Old skill"]
        for record in history["TEST"]["location_searches"]:
            record["filters"] = {"title": "Old title", "version": 2}
        _, _, _, api, filters = await self.run_flow(history=history, profiles=profiles)
        self.assertEqual(api.collect_profiles.await_count, 6)
        self.assertEqual(filters.await_count, 3)
    async def test_advanced_failure_still_runs_normal_searches(self):
        events, history, _, api, _ = await self.run_flow(advanced_error=True)
        self.assertEqual(api.apply_title.await_count, 3)
        self.assertIn("profiles", events)
        self.assertEqual(history["TEST"]["advanced_search"]["search_status"], "failed")
        self.assertEqual(history["TEST"]["status"], "pending")

    async def test_search_only_merges_both_sources_without_applicant_processing(self):
        events, history, _, _, _ = await self.run_flow(search_only=True)
        self.assertNotIn("profiles", events)
        self.assertEqual(history["TEST"]["profiles"], ["shared", "advanced-only", "normal-only"])


if __name__ == "__main__":
    unittest.main()