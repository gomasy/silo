#!/usr/bin/env python3
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

"""Refresh the generated-asset checkout; publishing is handled by the workflow."""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
import json
from http.client import IncompleteRead
import os
from pathlib import Path
import re
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

import yaml

import render

REPOSITORY = 'pgsty/silo'
SOURCE = 'repos/pgsty/silo.pgsty.com/contents/data/home/contributors.yaml?ref=main'
GROUPS = ('code', 'proposed', 'reports')
HANDLE = re.compile(r'[A-Za-z0-9][A-Za-z0-9-]{0,38}\Z')


def request(url, token='', limit=8 * 1024 * 1024):
    headers = {'User-Agent': 'silo-repository-cards', 'Accept': 'application/vnd.github+json'}
    if urlparse(url).netloc == 'api.github.com':
        headers['X-GitHub-Api-Version'] = '2022-11-28'
        if token:
            headers['Authorization'] = f'Bearer {token}'
    for attempt in range(3):
        try:
            with urlopen(Request(url, headers=headers), timeout=25) as response:
                data = response.read(limit + 1)
                if len(data) > limit:
                    raise ValueError('Response exceeds the size limit')
                expected = response.headers.get('Content-Length')
                if expected is not None and len(data) != int(expected):
                    raise URLError('Incomplete response body')
                return data
        except HTTPError as exc:
            if exc.code < 500 or attempt == 2:
                raise
        except (URLError, TimeoutError, IncompleteRead):
            if attempt == 2:
                raise
        time.sleep(attempt + 1)


class GitHub:
    def __init__(self, token):
        self.token = token

    def get(self, path):
        for attempt in range(3):
            try:
                return json.loads(request('https://api.github.com/' + path, self.token))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                if attempt == 2:
                    raise ValueError(f'Incomplete or invalid GitHub JSON: {path}') from exc
                time.sleep(attempt + 1)

    def issues(self, repository):
        page = 1
        while True:
            batch = self.get(f'repos/{repository}/issues?state=all&per_page=100&page={page}&sort=created&direction=asc')
            if not isinstance(batch, list):
                raise ValueError(f'Invalid issues response for {repository}')
            yield from batch
            if len(batch) < 100:
                return
            page += 1


def curated_snapshot(data, revision):
    updated = str(data['updated'])
    datetime.fromisoformat(updated)
    repositories = [item['repo'] for item in data['repositories']]
    if not repositories or any(not re.fullmatch(r'pgsty/[A-Za-z0-9_.-]+', repo) for repo in repositories):
        raise ValueError('Invalid contributor repository scope')
    people = []
    for group in GROUPS:
        for entry in data[group]:
            if not HANDLE.fullmatch(entry['handle']):
                raise ValueError('Invalid GitHub contributor handle')
            people.append({
                'handle': entry['handle'], 'group': group,
                'featured': bool(entry.get('featured')), 'what': entry['what'],
                'firstContribution': str(entry.get('firstContribution', '9999-12-31')),
            })
    if not people or len({p['handle'].lower() for p in people}) != len(people):
        raise ValueError('Empty or duplicate contributor roster')
    return {'updated': updated, 'revision': revision, 'repositories': repositories,
            'bots': data.get('bots', ['Copilot', 'dependabot[bot]']), 'people': people}


def select_curated(remote, cached):
    # The initial, approved preview can contain reviewed credit not published by
    # the companion site yet. Keep that newer snapshot until the site catches up.
    if cached and datetime.fromisoformat(cached['updated']) > datetime.fromisoformat(remote['updated']):
        return cached
    return remote


def collect_people(api, curated):
    bots = {name.lower() for name in curated['bots']}
    people = {p['handle'].lower(): dict(p) for p in curated['people']
              if p['handle'].lower() not in bots and not p['handle'].lower().endswith('[bot]')}
    order = {p['handle'].lower(): index for index, p in enumerate(curated['people'])}
    for repository in curated['repositories']:
        print(f'Reading issue and PR authors: {repository}', flush=True)
        for issue in api.issues(repository):
            user = issue.get('user') or {}
            handle = user.get('login', '')
            key = handle.lower()
            if user.get('type') != 'User' or key in bots or key.endswith('[bot]'):
                continue
            if not HANDLE.fullmatch(handle):
                raise ValueError('Invalid issue author')
            pr = issue.get('pull_request')
            group = 'code' if pr and pr.get('merged_at') else 'proposed' if pr else 'reports'
            first = issue['created_at'][:10]
            date.fromisoformat(first)
            person = people.setdefault(key, {
                'handle': handle, 'group': group, 'featured': False,
                'what': 'Contributed an issue or pull request to SILO and related projects',
                'firstContribution': first,
            })
            person['avatarUrl'] = user.get('avatar_url', '')
            person['firstContribution'] = min(person['firstContribution'], first)
            if GROUPS.index(group) < GROUPS.index(person['group']):
                person['group'] = group
    if not people:
        raise ValueError('No human contributors were collected')
    return sorted(people.values(), key=lambda p: (
        GROUPS.index(p['group']), not p['featured'],
        order.get(p['handle'].lower(), len(order)), p['firstContribution'], p['handle'].lower()))


def raster_data_url(data):
    if data.startswith(b'\x89PNG\r\n\x1a\n'):
        mime = 'image/png'
    elif data.startswith(b'\xff\xd8\xff'):
        mime = 'image/jpeg'
    elif data.startswith((b'GIF87a', b'GIF89a')):
        mime = 'image/gif'
    elif data[:4] == b'RIFF' and data[8:12] == b'WEBP':
        mime = 'image/webp'
    else:
        raise ValueError('Avatar is not a raster image')
    return f'data:{mime};base64,' + base64.b64encode(data).decode('ascii')


def cached_avatar(person):
    value = person.get('avatarDataUrl', '')
    if not value:
        return ''
    prefix, encoded = value.split(',', 1)
    if prefix not in ('data:image/png;base64', 'data:image/jpeg;base64', 'data:image/gif;base64', 'data:image/webp;base64'):
        raise ValueError('Invalid cached avatar format')
    raw = base64.b64decode(encoded, validate=True)
    if len(raw) > 512 * 1024 or raster_data_url(raw) != value:
        raise ValueError('Invalid cached avatar')
    return value


def add_avatars(api, people, previous):
    cached = {p['handle'].lower(): cached_avatar(p) for p in previous}

    def update(person):
        person = dict(person)
        try:
            url = person.pop('avatarUrl', '') or api.get('users/' + person['handle'])['avatar_url']
            parsed = urlparse(url)
            if parsed.scheme != 'https' or parsed.netloc != 'avatars.githubusercontent.com':
                raise ValueError('Unexpected avatar host')
            data = request(url + ('&' if '?' in url else '?') + 's=96', limit=512 * 1024)
            person['avatarDataUrl'] = raster_data_url(data)
        except (HTTPError, URLError, TimeoutError, IncompleteRead, ValueError, KeyError) as exc:
            person.pop('avatarUrl', None)
            person['avatarDataUrl'] = cached.get(person['handle'].lower(), '')
            print(f'Avatar fallback for @{person["handle"]}: {type(exc).__name__}', file=sys.stderr)
        return person

    with ThreadPoolExecutor(max_workers=6) as pool:
        return list(pool.map(update, people))


def update_history(history, day, stars):
    date.fromisoformat(day)
    if type(stars) is not int or stars < 0:
        raise ValueError('Invalid repository star count')
    if history is None:
        history = {'repository': REPOSITORY, 'bootstrap': {'through': day, 'reconstructed': False}, 'points': []}
    if history['repository'] != REPOSITORY:
        raise ValueError('Star history belongs to a different repository')
    date.fromisoformat(history['bootstrap']['through'])
    dates = []
    for point in history['points']:
        date.fromisoformat(point['date'])
        if type(point['stars']) is not int or point['stars'] < 0:
            raise ValueError('Invalid historical star count')
        dates.append(point['date'])
    if dates != sorted(set(dates)) or any(d > day for d in dates):
        raise ValueError('History contains duplicate, unordered, or future dates')
    # Replace today's observation, preserve previous days, and allow unstars.
    points = [dict(p) for p in history['points'] if p['date'] != day]
    points.append({'date': day, 'stars': stars})
    return {**history, 'points': points}


def read_json(path, default=None):
    return json.loads(path.read_text()) if path.exists() else default


def refresh(output, api, source_root):
    day = datetime.now(timezone.utc).date().isoformat()
    metadata = api.get('repos/' + REPOSITORY)
    if metadata['full_name'].lower() != REPOSITORY:
        raise ValueError('Unexpected repository metadata')
    history = update_history(read_json(output / 'history.json'), day, metadata['stargazers_count'])
    source = api.get(SOURCE)
    reviewed = yaml.safe_load(base64.b64decode(source['content'], validate=False))
    curated = select_curated(curated_snapshot(reviewed, source['sha']), read_json(output / 'curated.json'))
    people = collect_people(api, curated)
    previous = read_json(output / 'contributors.json', {}).get('people', [])
    people = add_avatars(api, people, previous)
    emblem = render.read_emblem(source_root / '.github/silo.svg')
    payloads = {}
    for theme in ('light', 'dark'):
        payloads[f'contributors-{theme}.svg'] = render.contributors(theme, people, day, emblem) + '\n'
        payloads[f'star-history-{theme}.svg'] = render.stars(theme, history, day, emblem) + '\n'
    for svg in payloads.values():
        ET.fromstring(svg)
    for name, data in {
        'history.json': history,
        'curated.json': curated,
        'contributors.json': {'repository': REPOSITORY, 'updated': day, 'people': people},
    }.items():
        payloads[name] = json.dumps(data, indent=2, ensure_ascii=False) + '\n'
    payloads['README.md'] = f'''# SILO repository cards

Generated by [Repository Cards](https://github.com/pgsty/silo/actions/workflows/repository-cards.yml)
at 00:00 UTC daily (08:00 Asia/Shanghai). GitHub may queue scheduled runs.

Snapshot: {day}. {metadata['stargazers_count']:,} stars; {len(people)} community contributors.

- `contributors-light.svg` / `contributors-dark.svg`: human issue and PR authors across the SILO project scope, plus reviewed acknowledgements. Bots are excluded. Gold rings follow the reviewed companion-site roster; new authors are collected automatically.
- `star-history-light.svg` / `star-history-dark.svg`: initial history reconstructed from the then-current stargazers; later points are daily observed totals, including decreases. Missing days are not fabricated.
- `curated.json`: a cache of reviewed contributor credit from `pgsty/silo.pgsty.com/data/home/contributors.yaml`. The approved initial preview may be newer than the published site; a newer reviewed snapshot is retained until the site catches up.
- `contributors.json`: generated contributor data and embedded raster avatars. Failed avatar refreshes use the previous image, or an initial when no image is available.
- `history.json`: persistent daily totals. Keep this file when regenerating images.

The SVGs are self-contained. Source and instructions live on the default branch;
this branch contains generated assets only. Do not merge it into `main`.
'''
    # Collect and validate everything before touching the publication checkout.
    output.mkdir(parents=True, exist_ok=True)
    for filename, text in payloads.items():
        (output / filename).write_text(text)
    print(f'{day}: {len(people)} contributors; {metadata["stargazers_count"]:,} stars; {len(history["points"])} history points')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    configured = os.environ.get('GITHUB_REPOSITORY', REPOSITORY)
    if configured.lower() != REPOSITORY:
        raise SystemExit('This workflow is scoped to pgsty/silo')
    refresh(args.output, GitHub(os.environ.get('GH_TOKEN', '')), Path(__file__).resolve().parents[2])


if __name__ == '__main__':
    main()
