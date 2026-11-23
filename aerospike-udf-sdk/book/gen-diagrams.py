#!/usr/bin/env python3
"""The book's diagrams. The CRDT tutorial's are two-lane timelines, one lane per data centre, writes as
dots, shipments as arrows between the lanes, the record's state in a box after each event; the overview
chapters' are boxes and arrows (the module contract, a record UDF call, a stream UDF's partials) and one
lifecycle timeline. Hand-placed, written as inline SVG (`{{#include diagrams/<name>.svg}}`) so that
`currentColor` picks up the book's theme. Each SVG is one `<svg>` open tag and no blank line inside, or the
Markdown parser ends the HTML block early.

    python3 gen-diagrams.py          # writes src/crdt/diagrams/*.svg and src/diagrams/*.svg
"""

import os

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src')

DC1 = '#2f80ed'
DC2 = '#e08a1e'
GOOD = '#2fa66b'
BAD = '#d9534f'
MONO = 'var(--mono-font, ui-monospace, monospace)'

STYLE = f"""
.lane {{ stroke: currentColor; stroke-opacity: .35; stroke-width: 1.5; }}
.lanelabel {{ font-size: 13px; font-weight: 600; fill: currentColor; }}
.lbl {{ font-size: 12px; fill: currentColor; }}
.lbl.b {{ font-weight: 600; }}
.mono {{ font-family: {MONO}; font-size: 11.5px; fill: currentColor; }}
.box {{ fill: none; stroke: currentColor; stroke-opacity: .45; rx: 4; }}
.box.good {{ stroke: {GOOD}; stroke-opacity: 1; stroke-width: 1.5; }}
.box.bad {{ stroke: {BAD}; stroke-opacity: 1; stroke-width: 1.5; }}
.ship {{ fill: none; stroke: currentColor; stroke-opacity: .7; stroke-width: 1.4; marker-end: url(#arrow); }}
.ship.dashed {{ stroke-dasharray: 5 4; }}
.cap {{ font-size: 12.5px; fill: currentColor; fill-opacity: .85; }}
.time {{ font-size: 11px; fill: currentColor; fill-opacity: .6; }}
.tick {{ stroke: currentColor; stroke-opacity: .3; stroke-dasharray: 3 3; }}
.bar {{ rx: 3; }}
.barlbl {{ font-size: 12px; fill: #141414; }}
.region {{ fill: currentColor; fill-opacity: .04; stroke: currentColor; stroke-opacity: .3; stroke-dasharray: 4 3; rx: 6; }}
.node {{ fill: none; stroke: currentColor; stroke-opacity: .55; stroke-width: 1.2; rx: 5; }}
.node.good {{ stroke: {GOOD}; stroke-opacity: 1; stroke-width: 1.5; }}
.node.bad {{ stroke: {BAD}; stroke-opacity: 1; stroke-width: 1.5; }}
.cell {{ fill: currentColor; fill-opacity: .07; stroke: currentColor; stroke-opacity: .5; }}
.ship.bad {{ stroke: {BAD}; stroke-opacity: 1; marker-end: url(#arrow-bad); }}
.slice {{ fill: currentColor; fill-opacity: .25; }}
"""


class Svg:
    def __init__(self, width, height, title):
        self.w = width
        self.h = height
        self.title = title
        self.parts = []

    def add(self, s):
        self.parts.append(s)

    # --- lanes and events --------------------------------------------------------------------------------

    def lane(self, y, label, color, x0=96, x1=None):
        x1 = x1 or self.w - 24
        self.add(f'<line class="lane" x1="{x0}" y1="{y}" x2="{x1}" y2="{y}"/>')
        self.add(f'<text class="lanelabel" x="{x0 - 10}" y="{y + 4}" text-anchor="end" fill="{color}" style="fill:{color}">{label}</text>')

    def time_arrow(self, y, x1=None, label='time'):
        x1 = x1 or self.w - 24
        self.add(f'<text class="time" x="{x1}" y="{y + 16}" text-anchor="end">{label} →</text>')

    def write(self, x, y, color, label, side='left', bold=True):
        """A write: a dot on the lane, its label to the left (or right) of it."""
        self.add(f'<circle cx="{x}" cy="{y}" r="5.5" fill="{color}"/>')
        cls = 'lbl b' if bold else 'lbl'
        if side == 'left':
            self.add(f'<text class="{cls}" x="{x - 10}" y="{y + 4}" text-anchor="end">{label}</text>')
        else:
            self.add(f'<text class="{cls}" x="{x + 10}" y="{y + 4}">{label}</text>')

    def merge(self, x, y, color, label=None, side='right', bad=False):
        """A merge: a diamond where a shipment arrives."""
        c = BAD if bad else GOOD
        self.add(f'<path d="M{x},{y - 7} L{x + 7},{y} L{x},{y + 7} L{x - 7},{y} Z" fill="{c}"/>')
        if label:
            if side == 'right':
                self.add(f'<text class="lbl" x="{x + 11}" y="{y + 4}">{label}</text>')
            elif side == 'above':
                self.add(f'<text class="lbl" x="{x}" y="{y - 12}" text-anchor="middle">{label}</text>')
            elif side == 'below':
                self.add(f'<text class="lbl" x="{x}" y="{y + 18}" text-anchor="middle">{label}</text>')
            else:
                self.add(f'<text class="lbl" x="{x - 11}" y="{y + 4}" text-anchor="end">{label}</text>')

    def ship(self, x1, y1, x2, y2, dashed=False, label=None):
        """XDR ships a write from one lane to the other; the slope is the lag."""
        dy = 1 if y2 > y1 else -1
        cls = 'ship dashed' if dashed else 'ship'
        self.add(f'<line class="{cls}" x1="{x1}" y1="{y1 + 8 * dy}" x2="{x2}" y2="{y2 - 9 * dy}"/>')
        if label:
            mx = (x1 + x2) / 2
            my = (y1 + y2) / 2
            self.add(f'<text class="time" x="{mx + 8}" y="{my + 4}">{label}</text>')

    def state(self, x, y, lines, kind=None, anchor='middle'):
        """The record after an event: monospace lines in a rounded box centred on x (or starting at x)."""
        if isinstance(lines, str):
            lines = [lines]
        width = max(len(l) for l in lines) * 6.9 + 14
        height = len(lines) * 15 + 8
        if anchor == 'middle':
            bx = x - width / 2
        elif anchor == 'start':
            bx = x
        else:
            bx = x - width
        cls = 'box' + (f' {kind}' if kind else '')
        self.add(f'<rect class="{cls}" x="{bx:.1f}" y="{y}" width="{width:.1f}" height="{height}"/>')
        for i, l in enumerate(lines):
            self.add(f'<text class="mono" x="{bx + 7:.1f}" y="{y + 15 + i * 15}" xml:space="preserve">{esc(l)}</text>')
        return bx, bx + width

    def caption(self, y, text, x=None):
        x = self.w / 2 if x is None else x
        self.add(f'<text class="cap" x="{x}" y="{y}" text-anchor="middle">{esc(text)}</text>')

    def text(self, x, y, text, cls='lbl', anchor='start'):
        self.add(f'<text class="{cls}" x="{x}" y="{y}" text-anchor="{anchor}">{esc(text)}</text>')

    def vline(self, x, y1, y2):
        self.add(f'<line class="tick" x1="{x}" y1="{y1}" x2="{x}" y2="{y2}"/>')

    def bar(self, x1, x2, y, color, label, h=16, opacity=1.0):
        self.add(f'<rect class="bar" x="{x1}" y="{y - h / 2}" width="{x2 - x1}" height="{h}" fill="{color}" fill-opacity="{opacity}"/>')
        self.add(f'<text class="barlbl" x="{x1 + 6}" y="{y + 4}">{esc(label)}</text>')

    # --- boxes and arrows --------------------------------------------------------------------------------

    def region(self, x, y, w, h, label=None, color=None):
        """A dashed area: the host, the guest, a node."""
        self.add(f'<rect class="region" x="{x}" y="{y}" width="{w}" height="{h}"/>')
        if label:
            style = f' style="fill:{color}"' if color else ''
            self.add(f'<text class="lbl b" x="{x + 10}" y="{y + 17}"{style}>{esc(label)}</text>')

    def node(self, x, y, w, h, lines, kind=None, mono=False):
        """A box with centred lines of text; x, y is its top-left corner."""
        if isinstance(lines, str):
            lines = [lines]
        cls = 'node' + (f' {kind}' if kind else '')
        self.add(f'<rect class="{cls}" x="{x}" y="{y}" width="{w}" height="{h}"/>')
        lh = 15
        y0 = y + h / 2 - (len(lines) - 1) * lh / 2 + 4.5
        tcls = 'mono' if mono else 'lbl'
        for i, l in enumerate(lines):
            self.add(f'<text class="{tcls}" x="{x + w / 2}" y="{y0 + i * lh:.1f}" text-anchor="middle">{esc(l)}</text>')

    def cells(self, x, y, texts, widths, h=24):
        """Adjacent cells with monospace text: the sixteen-byte value."""
        for t, w in zip(texts, widths):
            self.add(f'<rect class="cell" x="{x}" y="{y}" width="{w}" height="{h}"/>')
            self.add(f'<text class="mono" x="{x + w / 2}" y="{y + h / 2 + 4}" text-anchor="middle">{esc(t)}</text>')
            x += w

    def arrow(self, x1, y1, x2, y2, label=None, dashed=False, bad=False, via=None, label_dy=-6):
        """An arrow between boxes; `via` bends it through a control point."""
        cls = 'ship' + (' dashed' if dashed else '') + (' bad' if bad else '')
        if via:
            self.add(f'<path class="{cls}" d="M{x1},{y1} Q{via[0]},{via[1]} {x2},{y2}"/>')
            mx, my = (x1 + 2 * via[0] + x2) / 4, (y1 + 2 * via[1] + y2) / 4
        else:
            self.add(f'<line class="{cls}" x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}"/>')
            mx, my = (x1 + x2) / 2, (y1 + y2) / 2
        if label:
            self.add(f'<text class="time" x="{mx}" y="{my + label_dy}" text-anchor="middle">{esc(label)}</text>')

    def render(self):
        body = '\n'.join(self.parts)
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.w} {self.h}" '
            f'role="img" aria-label="{esc(self.title)}" '
            f'style="max-width:{self.w}px;width:100%;height:auto;display:block;margin:1.2em auto;font-family:inherit">\n'
            f'<title>{esc(self.title)}</title>\n'
            f'<style>{STYLE}</style>\n'
            '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
            '<path d="M0,0 L10,5 L0,10 Z" fill="currentColor" fill-opacity=".75"/></marker>'
            '<marker id="arrow-bad" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
            f'<path d="M0,0 L10,5 L0,10 Z" fill="{BAD}"/></marker></defs>\n'
            f'{body}\n</svg>'
        )


def esc(s):
    return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


# ---------------------------------------------------------------------------------------------- diagrams

def intro_plain_int():
    s = Svg(720, 300, 'A plain integer cannot be merged')
    y1, y2 = 96, 214
    s.lane(y1, 'dc1', DC1)
    s.lane(y2, 'dc2', DC2)
    s.time_arrow(y2)
    # both start at 3
    s.state(150, 42, 'n: 3')
    s.state(150, 240, 'n: 3')
    s.write(150, y1, DC1, 'starts at 3', side='left', bold=False)
    s.write(150, y2, DC2, 'starts at 3', side='left', bold=False)
    # concurrent writes
    s.write(290, y1, DC1, 'n += 2')
    s.state(290, 42, 'n: 5')
    s.write(290, y2, DC2, 'n += 5')
    s.state(290, 240, 'n: 8')
    # crossing shipments
    s.ship(290, y1, 470, y2)
    s.ship(290, y2, 470, y1)
    s.merge(470, y1, None, 'pick a side', bad=True)
    s.merge(470, y2, None, 'pick a side', bad=True)
    s.state(470, 42, 'n: 5  or  n: 8', kind='bad')
    s.state(470, 240, 'n: 5  or  n: 8', kind='bad')
    s.caption(288, 'Both sides added to 3, and the answer is 10. Neither 5 nor 8 is it, and no rule over the two numbers recovers it.')
    return s


def intro_gcounter():
    s = Svg(720, 300, 'One count per site merges by the maximum')
    y1, y2 = 96, 214
    s.lane(y1, 'dc1', DC1)
    s.lane(y2, 'dc2', DC2)
    s.time_arrow(y2)
    s.state(150, 42, '{dc1: 3}')
    s.state(150, 240, '{dc1: 3}')
    s.write(150, y1, DC1, 'starts at 3', side='left', bold=False)
    s.write(150, y2, DC2, 'starts at 3', side='left', bold=False)
    s.write(290, y1, DC1, 'increment(dc1, 2)')
    s.state(290, 42, '{dc1: 5}')
    s.write(290, y2, DC2, 'increment(dc2, 5)')
    s.state(290, 240, '{dc1: 3, dc2: 5}')
    s.ship(290, y1, 470, y2)
    s.ship(290, y2, 470, y1)
    s.merge(470, y1, None, 'max per site')
    s.merge(470, y2, None, 'max per site')
    s.state(470, 42, '{dc1: 5, dc2: 5} = 10', kind='good')
    s.state(470, 240, '{dc1: 5, dc2: 5} = 10', kind='good')
    s.caption(288, 'Each site adds only to its own entry. The merge keeps the larger entry per site, and both sides read 10.')
    return s


def intro_merge_loop():
    s = Svg(720, 320, 'The merge runs on both sides and stops when it changes nothing')
    y1, y2 = 96, 214
    s.lane(y1, 'dc1', DC1)
    s.lane(y2, 'dc2', DC2)
    s.time_arrow(y2)
    s.write(170, y1, DC1, 'a write')
    s.state(170, 42, 'W')
    s.ship(170, y1, 330, y2, label='XDR ships W')
    s.merge(330, y2, None, 'xdr-merge: merge(local, W) = R')
    s.state(330, 240, 'R', kind='good')
    s.ship(330, y2, 500, y1, dashed=True, label='R ships back')
    s.merge(500, y1, None, 'merge(W, R) = R, same bytes', side='right')
    s.state(500, 42, 'R  (unchanged: no write, lost-conflict)', kind='good')
    s.text(500, 140, 'nothing ships: the loop ends here', cls='time', anchor='middle')
    s.caption(292, 'Every incoming XDR write to an existing record runs the handler. A merge that reproduces the stored bytes')
    s.caption(309, 'is elided and reported to the source as a success, which is what stops the two clusters bouncing writes.')
    return s


def twopset_race():
    s = Svg(720, 340, 'A removal wins the race, and is forever')
    y1, y2 = 100, 220
    s.lane(y1, 'dc1', DC1)
    s.lane(y2, 'dc2', DC2)
    s.time_arrow(y2)
    s.state(100, 36, ['added   [news]', 'removed []'], anchor='start')
    s.state(100, 246, ['added   [news]', 'removed []'], anchor='start')
    s.write(290, y1, DC1, 'add(art)')
    s.state(290, 36, ['added   [art, news]', 'removed []'])
    s.write(290, y2, DC2, 'remove(news)')
    s.state(290, 246, ['added   [news]', 'removed [news]'])
    s.ship(290, y1, 470, y2)
    s.ship(290, y2, 470, y1)
    s.merge(470, y1, None, 'union of each', side='above')
    s.merge(470, y2, None, 'union of each', side='below')
    s.state(470, 36, ['added   [art, news]', 'removed [news] → [art]'], kind='good')
    s.state(470, 246, ['added   [art, news]', 'removed [news] → [art]'], kind='good')
    s.write(600, y1, DC1, 'add(news)')
    s.state(640, 36, ['unchanged', '→ [art]'], kind='bad')
    s.ship(600, y1, 665, y2, dashed=True)
    s.merge(665, y2, None, None)
    s.caption(314, 'Both sides start holding news. dc1 adds art while dc2 removes news: the unions keep both changes. The later')
    s.caption(331, 'add(news) finds news already in added and still in removed, so it changes nothing, on either side.')
    return s


def lwwset_race():
    s = Svg(720, 340, 'A later stamp brings a removed tag back')
    y1, y2 = 100, 220
    s.lane(y1, 'dc1', DC1)
    s.lane(y2, 'dc2', DC2)
    s.time_arrow(y2)
    s.state(100, 36, ['+ {news:100}', '- {}'], anchor='start')
    s.state(100, 246, ['+ {news:100}', '- {}'], anchor='start')
    s.write(290, y1, DC1, 'add(art) @200')
    s.state(290, 36, ['+ {art:200, news:100}', '- {}'])
    s.write(290, y2, DC2, 'remove(news) @200')
    s.state(290, 246, ['+ {news:100}', '- {news:200}'])
    s.ship(290, y1, 470, y2)
    s.ship(290, y2, 470, y1)
    s.merge(470, y1, None, 'max per tag', side='above')
    s.merge(470, y2, None, 'max per tag', side='below')
    s.state(470, 36, ['+ {art:200, news:100}', '- {news:200}  → [art]'], kind='good')
    s.state(470, 246, ['+ {art:200, news:100}', '- {news:200}  → [art]'], kind='good')
    s.write(600, y1, DC1, 'add(news) @400')
    s.state(640, 36, ['news 400 > 200', '→ [art, news]'], kind='good')
    s.ship(600, y1, 665, y2)
    s.merge(665, y2, None, None)
    s.caption(314, 'The same race as step 3, with stamps (+ is addedAt, - is removedAt). The re-add at 400 outranks the removal')
    s.caption(331, 'at 200 on both sides, because the member test compares stamps instead of looking for a tombstone.')
    return s


def lwwset_skew():
    s = Svg(720, 342, "When dc2's clock runs behind, the later write loses")
    y1, y2 = 100, 220
    s.lane(y1, 'dc1', DC1)
    s.lane(y2, 'dc2', DC2)
    s.time_arrow(y2, label='real time')
    for x, t in [(200, '400'), (360, '500'), (520, '600')]:
        s.vline(x, 60, 262)
        s.text(x, 56, f't = {t}', cls='time', anchor='middle')
    s.write(200, y1, DC1, 'add(news)')
    s.state(200, 66, 'stamp 400', kind='good')
    s.ship(200, y1, 300, y2)
    s.merge(300, y2, None, None)
    s.write(360, y2, DC2, 'remove(news)', side='right')
    s.state(360, 246, ['dc2 clock reads 300', 'stamp 300'], kind='bad')
    s.ship(360, y2, 520, y1)
    s.merge(520, y1, None, 'max per tag', side='above')
    s.state(500, 128, ['+ {news:400}', '- {news:300} → news stays'], kind='bad', anchor='start')
    s.text(590, 265, 'the removal happened later and lost', cls='time', anchor='middle')
    s.caption(316, 'dc2 removes news a hundred milliseconds after dc1 added it back, but its clock is a second slow, so it stamps 300.')
    s.caption(333, '"Last writer wins" means last by the stamp. Step 5 replaces the clock with a version the application raises.')
    return s


def device_map_horizon():
    s = Svg(720, 300, 'The purge age must outlast an outage plus the replication lag')
    x0, xo, xl = 120, 400, 480   # removal; outage ends; dc2's copy arrives / dc2 sees the removal
    xs, xg = 350, 600            # a purge age that is too short; one that is long enough
    top, bottom = 50, 238
    s.text(x0, 36, 'dc1 removes C, leaves a marker', cls='lbl b', anchor='start')
    s.vline(x0, 44, bottom)
    s.vline(xl, 44, bottom)
    s.text(xl, 36, "dc2's live copy of C arrives", cls='lbl b', anchor='middle')
    s.bar(x0, xo, 78, DC2, 'link down: dc2 has not heard', opacity=.85)
    s.bar(xo, xl, 78, DC2, 'lag', opacity=.5)
    s.text(x0 - 8, 82, 'between the clusters', cls='lbl', anchor='end')
    s.bar(x0, xs, 128, BAD, 'marker kept for a short purge age', opacity=.85)
    s.text(xs + 8, 132, 'purged: C comes back', cls='lbl b', anchor='start')
    s.text(x0 - 8, 132, 'too short', cls='lbl', anchor='end')
    s.bar(x0, xg, 178, GOOD, 'marker kept for a purge age longer than outage + lag', opacity=.85)
    s.text(xg + 8, 182, 'marker wins', cls='lbl b', anchor='start')
    s.text(x0 - 8, 182, 'long enough', cls='lbl', anchor='end')
    s.time_arrow(bottom - 16, label='time')
    s.add(f'<line class="lane" x1="96" y1="{bottom - 16}" x2="{s.w - 24}" y2="{bottom - 16}"/>')
    s.caption(272, 'A marker purged before the other side has seen the removal lets that side resurrect the entry. The version floor')
    s.caption(289, 'limits the damage to devices re-added in that window, but the purge age is the real guard.')
    return s


# ---------------------------------------------------------------------------- the overview chapters

def module_contract():
    s = Svg(720, 346, 'Every read is one sixteen-byte value; bytes and elements cross only when the guest asks')
    s.region(24, 44, 176, 246, 'host: the record', DC1)
    s.region(504, 44, 192, 246, 'guest: the module', DC2)
    s.text(390, 60, 'one value, sixteen bytes', cls='lbl b', anchor='middle')
    s.cells(298, 72, ['kind', 'len', 'payload'], [40, 32, 112], h=20)
    rows = [
        (122, 'balance: 42', ['int', '', '42'], 'bin("balance")', ['the integer, in place'], False),
        (182, 'name: "alice"', ['str', '5', 'handle, offset'], 'bin("name")', ['bytes into a buffer', 'on request'], True),
        (242, 'tags: [a, b, c]', ['list', '3', 'handle, offset'], 'bin("tags")', ['walked with a cursor', '128 per import'], True),
    ]
    for y, bin_, cell, read, guest, lazy in rows:
        s.node(36, y - 17, 150, 34, bin_, mono=True)
        s.arrow(186, y, 296, y, label=read, label_dy=-8)
        s.cells(298, y - 12, cell, [40, 32, 112])
        s.arrow(482, y, 512, y, dashed=lazy)
        s.node(514, y - 17, 170, 34, guest)
    s.arrow(504, 274, 202, 274, label='handed back by position: a bin, the result, a collection being built', label_dy=15)
    s.caption(318, 'The host keeps the record and, for the call, the sources a handle points into. A scalar arrives whole; a string')
    s.caption(335, 'or collection arrives as a handle, and nothing is decoded until the guest asks. A value read can go straight back.')
    return s


def record_call():
    s = Svg(720, 262, 'A record UDF reads, stages, commits and answers; without the commit nothing is written')
    s.text(26, 50, 'a client executes module.fn(args) on a key; the server opens the record, takes its lock and calls the export')
    y, h, w = 84, 54, 148
    xs = [26, 196, 366, 546]
    s.node(xs[0], y, w, h, ['1 read', 'bins, metadata, args'])
    s.node(xs[1], y, w, h, ['2 stage', 'set_*, bin_remove'])
    s.node(xs[2], y, w, h, ['3 commit', 'rec_update / rec_create'], kind='good')
    s.node(xs[3], y, w, h, ['4 answer', 'result_* or fail'])
    for a, b in zip(xs, xs[1:]):
        s.arrow(a + w, y + h / 2, b, y + h / 2)
    s.text(xs[2] + w / 2, 160, 'the record changes only here', cls='lbl b', anchor='middle')
    s.arrow(xs[1] + w / 2, y + h, xs[3] + w / 2, y + h, bad=True, via=(xs[2] + w / 2, y + h + 110),
            label='return without a commit: the staged writes are dropped, the call looks right and changed nothing', label_dy=16)
    s.caption(244, 'Reads borrow from the record until the call ends; a value given to set_*, result_* or a builder moves to the host.')
    return s


def stream_partials():
    s = Svg(720, 350, 'One instance per query thread, one partial per thread, merged by the client')
    s.region(24, 44, 492, 240, 'one node: the query runs on its query threads', DC1)
    ys = [104, 164, 224]
    for i, y in enumerate(ys):
        s.text(40, y + 4, f'thread {i + 1}', cls='lbl b')
        for k in range(5):
            s.add(f'<rect class="slice" x="{104 + k * 18}" y="{y - 7}" width="12" height="14" rx="2"/>')
        s.arrow(196, y, 212, y)
        s.node(214, y - 16, 150, 32, ['one instance, kept', 'topk_record per record'])
        s.arrow(364, y, 380, y)
        s.node(382, y - 16, 56, 32, ['topk_end', 'once'])
        s.arrow(438, y, 454, y)
        s.node(456, y - 12, 48, 24, 'partial', kind='good')
        s.arrow(504, y, 566, 160, dashed=True)
    s.node(566, 130, 130, 60, ['the client', 'merges three partials', 'per node'], kind='good')
    s.text(104, 252, 'partition streams, one after another', cls='time')
    s.text(270, 272, 'state lives in the module\'s memory: static mut, package variables, statics', cls='time', anchor='middle')
    s.caption(308, "Lua's reduce ran on the client for you. Here the end function returns a partial that is cheap to merge, and a")
    s.caption(325, 'query over a node is a handful of partials rather than one per partition.')
    return s


def module_lifecycle():
    s = Svg(720, 334, 'A module runs in the interpreter until its ahead-of-time build is ready, and loads compiled after a restart')
    x_put, x_ready, x_restart, x_end = 120, 340, 540, 696
    y_bg, y1, y2 = 96, 160, 236
    s.lane(y1, 'any module', DC1)
    s.lane(y2, 'simd128', DC2)
    s.time_arrow(y2)
    for x, t in [(x_put, 'udf-put'), (x_ready, 'build ready'), (x_restart, 'restart')]:
        s.vline(x, 60, 262)
        s.text(x, 54, t, cls='lbl b', anchor='middle')
    s.text(x_put + 8, y_bg - 16, 'validated and instantiated; the build is cached under the hash of the bytes', cls='time')
    s.bar(x_put, x_ready, y_bg, GOOD, 'wamrc, on a background thread', opacity=.45)
    s.text(x_ready + 8, y_bg + 4, 'aot-compile-threads bounds the wait', cls='time')
    s.bar(x_put, x_ready, y1, DC2, 'interpreter, about Lua speed', opacity=.6)
    s.bar(x_ready, x_restart, y1, GOOD, 'compiled: native speed', opacity=.85)
    s.bar(x_restart, x_end, y1, GOOD, 'loads compiled', opacity=.85)
    s.bar(x_put, x_ready, y2, BAD, 'calls wait for the build', opacity=.6)
    s.bar(x_ready, x_restart, y2, GOOD, 'compiled: native speed', opacity=.85)
    s.bar(x_restart, x_end, y2, GOOD, 'loads compiled', opacity=.85)
    s.text(x_ready, 280, 'wamr: <module> is now running ahead-of-time compiled', cls='mono', anchor='middle')
    s.caption(304, 'A module that uses simd128 cannot run in the interpreter, so its first calls wait for the build instead of running')
    s.caption(321, 'slow. After a restart the compiled form is loaded from the cache and nothing is compiled again.')
    return s


DIAGRAMS = {
    'crdt/diagrams/intro-plain-int': intro_plain_int,
    'crdt/diagrams/intro-gcounter': intro_gcounter,
    'crdt/diagrams/intro-merge-loop': intro_merge_loop,
    'crdt/diagrams/twopset-race': twopset_race,
    'crdt/diagrams/lwwset-race': lwwset_race,
    'crdt/diagrams/lwwset-skew': lwwset_skew,
    'crdt/diagrams/device-map-horizon': device_map_horizon,
    'diagrams/module-contract': module_contract,
    'diagrams/record-call': record_call,
    'diagrams/stream-partials': stream_partials,
    'diagrams/module-lifecycle': module_lifecycle,
}


if __name__ == '__main__':
    for name, fn in DIAGRAMS.items():
        path = os.path.join(SRC, f'{name}.svg')
        os.makedirs(os.path.dirname(path), exist_ok=True)
        svg = fn().render()
        with open(path, 'w', encoding='utf-8') as f:
            f.write(svg)
            f.write('\n')
        print(f'{name}.svg: {len(svg)} bytes')
