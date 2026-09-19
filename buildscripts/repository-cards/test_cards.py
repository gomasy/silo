# Copyright (c) 2026 Feng Ruohang
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Regression checks for historical accuracy, contributor scope and SVG safety."""

import base64
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import URLError
import xml.etree.ElementTree as ET

import render
import update

NS = {'s': 'http://www.w3.org/2000/svg'}
PNG = base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a7mgAAAAASUVORK5CYII=')


def person(handle='Alice', group='reports', featured=False):
    return {'handle': handle, 'group': group, 'featured': featured,
            'what': 'A reviewed contribution', 'firstContribution': '2026-09-01'}


def history():
    return {'repository': 'pgsty/silo',
            'bootstrap': {'through': '2026-09-15', 'reconstructed': True},
            'points': [{'date': '2026-09-14', 'stars': 100}, {'date': '2026-09-15', 'stars': 105}]}


class HistoryTests(unittest.TestCase):
    def test_new_day_preserves_old_counts_and_unstars(self):
        before = history()
        after = update.update_history(before, '2026-09-16', 103)
        self.assertEqual(after['points'][:-1], before['points'])
        self.assertEqual(after['points'][-1], {'date': '2026-09-16', 'stars': 103})
        self.assertEqual(before, history())

    def test_same_day_rerun_replaces_instead_of_appending(self):
        first = update.update_history(history(), '2026-09-15', 107)
        self.assertEqual(len(first['points']), 2)
        self.assertEqual(first, update.update_history(first, '2026-09-15', 107))

    def test_missing_days_are_not_invented(self):
        result = update.update_history(history(), '2026-09-18', 106)
        self.assertEqual([p['date'] for p in result['points']], ['2026-09-14', '2026-09-15', '2026-09-18'])

    def test_rejects_wrong_repository_and_corrupt_history(self):
        cases = []
        wrong = history(); wrong['repository'] = 'someone/else'; cases.append(wrong)
        duplicate = history(); duplicate['points'].append(duplicate['points'][-1]); cases.append(duplicate)
        unordered = history(); unordered['points'].reverse(); cases.append(unordered)
        negative = history(); negative['points'][0]['stars'] = -1; cases.append(negative)
        future = history(); future['points'][-1]['date'] = '2026-09-20'; cases.append(future)
        for case in cases:
            with self.subTest(case=case), self.assertRaises(ValueError):
                update.update_history(case, '2026-09-16', 100)

    def test_first_run_has_no_fabricated_history(self):
        result = update.update_history(None, '2026-09-16', 10)
        self.assertFalse(result['bootstrap']['reconstructed'])
        self.assertEqual(result['points'], [{'date': '2026-09-16', 'stars': 10}])


class ContributorTests(unittest.TestCase):
    def test_retries_truncated_json_before_using_it(self):
        with patch('update.request', side_effect=[b'{"partial":', b'{"ok":true}']), patch('update.time.sleep'):
            self.assertEqual(update.GitHub('').get('repos/pgsty/silo'), {'ok': True})

    def test_paginates_past_one_full_page(self):
        class API(update.GitHub):
            def __init__(self): self.calls = []
            def get(self, path):
                self.calls.append(path)
                return list(range(100)) if 'page=1&' in path else [100]
        api = API()
        self.assertEqual(len(list(api.issues('pgsty/silo'))), 101)
        self.assertIn('state=all', api.calls[0])
        self.assertIn('page=2&', api.calls[1])

    def test_bots_deduplication_unmerged_work_and_reviewed_credit(self):
        def issue(login, kind='issue', user_type='User'):
            item = {'user': {'login': login, 'type': user_type, 'avatar_url': ''}, 'created_at': '2026-09-02T00:00:00Z'}
            if kind != 'issue': item['pull_request'] = {'merged_at': None if kind == 'open' else '2026-09-03T00:00:00Z'}
            return item
        class API:
            def issues(self, _repo):
                return [issue('alice'), issue('Bob', 'open'), issue('Bob', 'merged'),
                        issue('Carol', 'open'), issue('Copilot'), issue('robot', user_type='Bot')]
        curated = {'repositories': ['pgsty/silo', 'pgsty/mc'], 'bots': ['Copilot'],
                   'people': [person('Alice', featured=True), person('Reporter'), person('Copilot')]}
        result = update.collect_people(API(), curated)
        self.assertEqual({p['handle'] for p in result}, {'Alice', 'Bob', 'Carol', 'Reporter'})
        self.assertEqual(result[0]['handle'], 'Bob')
        self.assertEqual(result[1]['handle'], 'Carol')
        self.assertTrue(next(p for p in result if p['handle'] == 'Alice')['featured'])
        self.assertEqual(next(p for p in result if p['handle'] == 'Bob')['group'], 'code')
        self.assertFalse(next(p for p in result if p['handle'] == 'Carol')['featured'])

    def test_newer_reviewed_preview_survives_until_site_catches_up(self):
        remote = {'updated': '2026-09-16T03:00:00+00:00'}
        cached = {'updated': '2026-09-16T04:00:00+00:00'}
        self.assertIs(update.select_curated(remote, cached), cached)
        newer = {'updated': '2026-09-17T03:00:00+00:00'}
        self.assertIs(update.select_curated(newer, cached), newer)

    def test_avatar_failure_reuses_raster_cache(self):
        previous = {**person(), 'avatarDataUrl': update.raster_data_url(PNG)}
        with patch('update.request', side_effect=URLError('unavailable')):
            result = update.add_avatars(None, [{**person(), 'avatarUrl': 'https://avatars.githubusercontent.com/u/1'}], [previous])
        self.assertEqual(result[0]['avatarDataUrl'], previous['avatarDataUrl'])
        with self.assertRaises(ValueError): update.raster_data_url(b'<svg onload="bad()"/>')
        with self.assertRaises(ValueError): update.cached_avatar({'avatarDataUrl': 'data:image/svg+xml;base64,PHN2Zy8+'})

    def test_fetch_failure_leaves_published_assets_untouched(self):
        class API:
            def get(self, path):
                if path == 'repos/pgsty/silo': return {'full_name': 'pgsty/silo', 'stargazers_count': 106}
                raise URLError('roster unavailable')
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            original = json.dumps(history())
            (out / 'history.json').write_text(original)
            (out / 'contributors-light.svg').write_text('previous image')
            with self.assertRaises(URLError): update.refresh(out, API(), Path('.'))
            self.assertEqual((out / 'history.json').read_text(), original)
            self.assertEqual((out / 'contributors-light.svg').read_text(), 'previous image')


class RenderTests(unittest.TestCase):
    def test_real_emblem_generates_clean_xml(self):
        emblem = render.read_emblem(Path(__file__).resolve().parents[2] / '.github/silo.svg')
        svg = render.contributors('light', [person()], '2026-09-16', emblem)
        ET.fromstring(svg)
        self.assertTrue(all(line == line.rstrip() for line in svg.splitlines()))

    def test_all_avatars_fit_when_the_roster_grows(self):
        people = [{**person(f'person-{i}'), 'avatarDataUrl': update.raster_data_url(PNG)} for i in range(151)]
        for theme in ('light', 'dark'):
            root = ET.fromstring(render.contributors(theme, people, '2026-09-16', ''))
            images = root.findall('.//s:image', NS)
            self.assertEqual(len(images), 151)
            footer = float(root.attrib['height']) - 42
            self.assertTrue(all(float(i.attrib['y']) + float(i.attrib['height']) < footer for i in images))
            self.assertTrue(all(i.attrib['href'].startswith('data:image/png;base64,') for i in images))

    def test_untrusted_text_is_escaped(self):
        data = [{**person(), 'what': '<script>alert("x")</script> & contributions'}]
        svg = render.contributors('light', data, '2026-09-16', '')
        root = ET.fromstring(svg)
        self.assertEqual(root.findall('.//s:script', NS), [])
        self.assertIn('&lt;script&gt;', svg)

    def test_single_point_and_decreasing_star_history_render(self):
        for data in (update.update_history(None, '2026-09-16', 0), update.update_history(history(), '2026-09-16', 90)):
            for theme in ('light', 'dark'):
                svg = render.stars(theme, data, '2026-09-16', '')
                ET.fromstring(svg)
                self.assertNotIn('nan', svg.lower())
                self.assertNotIn('inf', svg.lower())


if __name__ == '__main__':
    unittest.main()
