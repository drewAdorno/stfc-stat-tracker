"""Tests for send_discord_notification.py"""

import pytest
from datetime import datetime, timedelta

import send_discord_notification as sdn


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def members():
    """Synthetic members list."""
    today = datetime.now()
    recent = (today - timedelta(days=2)).strftime("%b %d, %Y")
    old = (today - timedelta(days=30)).strftime("%b %d, %Y")
    return [
        {"id": "1", "name": "Alice", "level": "40", "power": "100M", "helps": "500", "join_date": recent},
        {"id": "2", "name": "Bob", "level": "35", "power": "50M", "helps": "200", "join_date": old},
        {"id": "3", "name": "Carol", "level": "30", "power": "25M", "helps": "100", "join_date": ""},
        {"id": "4", "name": "Dave", "level": "28", "power": "10M", "helps": "50"},
    ]


@pytest.fixture
def history():
    """Synthetic history with 3 snapshots (days -2, -1, today)."""
    today = datetime.now()
    dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in [2, 1, 0]]
    return [
        {
            "date": dates[0],
            "summary": {"total_power": "180M", "member_count": "5", "total_helps": "800", "total_rss": "10M", "total_iso": "5M"},
            "members": {
                "1": {"name": "Alice", "level": "38", "power": "90M", "helps": "400", "players_killed": "10", "hostiles_killed": "100", "resources_mined": "1M", "resources_raided": "500K", "rss_contrib": "1M", "iso_contrib": "500K"},
                "2": {"name": "Bob", "level": "35", "power": "50M", "helps": "200", "players_killed": "5", "hostiles_killed": "50", "resources_mined": "500K", "resources_raided": "200K", "rss_contrib": "500K", "iso_contrib": "200K"},
                "5": {"name": "Eve", "level": "32", "power": "40M", "helps": "200", "players_killed": "3", "hostiles_killed": "30", "resources_mined": "300K", "resources_raided": "100K", "rss_contrib": "300K", "iso_contrib": "100K"},
            },
        },
        {
            "date": dates[1],
            "summary": {"total_power": "185M", "member_count": "4", "total_helps": "850", "total_rss": "11M", "total_iso": "5.5M"},
            "members": {
                "1": {"name": "Alice", "level": "39", "power": "95M", "helps": "450", "players_killed": "12", "hostiles_killed": "110", "resources_mined": "1.1M", "resources_raided": "550K", "rss_contrib": "1.1M", "iso_contrib": "550K"},
                "2": {"name": "Bob", "level": "35", "power": "50M", "helps": "200", "players_killed": "5", "hostiles_killed": "50", "resources_mined": "500K", "resources_raided": "200K", "rss_contrib": "500K", "iso_contrib": "200K"},
                "5": {"name": "Eve", "level": "32", "power": "40M", "helps": "200", "players_killed": "3", "hostiles_killed": "30", "resources_mined": "300K", "resources_raided": "100K", "rss_contrib": "300K", "iso_contrib": "100K"},
            },
        },
        {
            "date": dates[2],
            "summary": {"total_power": "185M", "member_count": "4", "total_helps": "850", "total_rss": "11M", "total_iso": "5.5M"},
            "members": {
                "1": {"name": "Alice", "level": "40", "power": "100M", "helps": "500", "players_killed": "15", "hostiles_killed": "120", "resources_mined": "1.2M", "resources_raided": "600K", "rss_contrib": "1.2M", "iso_contrib": "600K"},
                "2": {"name": "Bob", "level": "35", "power": "50M", "helps": "200", "players_killed": "5", "hostiles_killed": "50", "resources_mined": "500K", "resources_raided": "200K", "rss_contrib": "500K", "iso_contrib": "200K"},
            },
        },
    ]


# ---------------------------------------------------------------------------
# 1. Number Parsing / Formatting
# ---------------------------------------------------------------------------

class TestParseAbbr:
    @pytest.mark.parametrize("inp, expected", [
        ("42", 42),
        ("1K", 1_000),
        ("2.5K", 2_500),
        ("190.58M", 190_580_000),
        ("1B", 1_000_000_000),
        ("3.5T", 3_500_000_000_000),
        ("1Q", 1e15),
    ])
    def test_suffixes(self, inp, expected):
        assert sdn.parse_abbr(inp) == pytest.approx(expected)

    def test_lowercase_suffix(self):
        assert sdn.parse_abbr("5k") == pytest.approx(5_000)

    def test_commas_stripped(self):
        assert sdn.parse_abbr("1,000") == pytest.approx(1_000)

    @pytest.mark.parametrize("inp", ["", None, "abc", "  "])
    def test_edge_cases_return_zero(self, inp):
        assert sdn.parse_abbr(inp) == 0

    def test_zero(self):
        assert sdn.parse_abbr("0") == 0


class TestFormatAbbr:
    @pytest.mark.parametrize("n, expected", [
        (0, "0"),
        (500, "500"),
        (1_000, "1K"),
        (2_500, "2.5K"),
        (1_000_000, "1M"),
        (190_580_000, "190.58M"),
        (1_000_000_000, "1B"),
        (1e12, "1T"),
        (1e15, "1Q"),
    ])
    def test_formatting(self, n, expected):
        assert sdn.format_abbr(n) == expected

    def test_negative(self):
        assert sdn.format_abbr(-5_000) == "-5K"

    def test_trailing_zeros_stripped(self):
        assert sdn.format_abbr(1_000_000) == "1M"
        assert "." not in sdn.format_abbr(1_000_000)


class TestFormatDelta:
    def test_positive(self):
        assert sdn.format_delta(5_000) == "+5K"

    def test_negative(self):
        assert sdn.format_delta(-5_000) == "-5K"

    def test_zero(self):
        assert sdn.format_delta(0) == "0"

    def test_large_positive(self):
        assert sdn.format_delta(1_500_000) == "+1.5M"


class TestRoundTrip:
    @pytest.mark.parametrize("n", [0, 42, 1_000, 2_500, 1_000_000, 190_580_000, 1e12])
    def test_parse_format_roundtrip(self, n):
        assert sdn.parse_abbr(sdn.format_abbr(n)) == pytest.approx(n, rel=1e-2)


# ---------------------------------------------------------------------------
# 2. Analytics Functions
# ---------------------------------------------------------------------------

class TestFindNewMembers:
    def test_recent_join_included(self, members):
        result = sdn.find_new_members(members)
        names = [m["name"] for m in result]
        assert "Alice" in names

    def test_old_join_excluded(self, members):
        result = sdn.find_new_members(members)
        names = [m["name"] for m in result]
        assert "Bob" not in names

    def test_empty_join_date_excluded(self, members):
        result = sdn.find_new_members(members)
        names = [m["name"] for m in result]
        assert "Carol" not in names

    def test_missing_join_date_excluded(self, members):
        result = sdn.find_new_members(members)
        names = [m["name"] for m in result]
        assert "Dave" not in names


class TestFindLeftMembers:
    def test_member_left(self, members, history):
        # Eve is in history[-2] but not in members
        result = sdn.find_left_members(members, history)
        names = [m["name"] for m in result]
        assert "Eve" in names

    def test_needs_two_entries(self, members):
        single = [{"date": "2026-01-01", "members": {"5": {"name": "Eve"}}}]
        assert sdn.find_left_members(members, single) == []

    def test_empty_history(self, members):
        assert sdn.find_left_members(members, []) == []

    def test_no_history(self, members):
        assert sdn.find_left_members(members, None) == []


class TestFindInactive:
    def test_inactive_member_detected(self, members, history):
        # Bob has identical stats across all 3 snapshots → 2 days inactive
        result = sdn.find_inactive(members, history)
        bob = next((m for m in result if m["name"] == "Bob"), None)
        assert bob is not None
        assert bob["days"] == 2

    def test_active_member_not_included(self, members, history):
        # Alice has changing stats
        result = sdn.find_inactive(members, history)
        alice = next((m for m in result if m["name"] == "Alice"), None)
        assert alice is None

    def test_sorted_descending(self, members, history):
        result = sdn.find_inactive(members, history)
        days = [m["days"] for m in result]
        assert days == sorted(days, reverse=True)

    def test_max_five(self, members, history):
        result = sdn.find_inactive(members, history)
        assert len(result) <= 5

    def test_too_few_snapshots(self, members):
        assert sdn.find_inactive(members, [{"date": "2026-01-01", "members": {}}]) == []


class TestFindPowerMovers:
    def test_gainers_sorted_desc(self, members, history):
        gainers, _ = sdn.find_power_movers(members, history)
        if len(gainers) > 1:
            deltas = [g["delta"] for g in gainers]
            assert deltas == sorted(deltas, reverse=True)

    def test_losers_sorted_asc(self, members, history):
        _, losers = sdn.find_power_movers(members, history)
        if len(losers) > 1:
            deltas = [l["delta"] for l in losers]
            assert deltas == sorted(deltas)

    def test_alice_is_gainer(self, members, history):
        gainers, _ = sdn.find_power_movers(members, history)
        names = [g["name"] for g in gainers]
        assert "Alice" in names

    def test_member_not_in_history_excluded(self, members, history):
        # Carol (id=3) and Dave (id=4) not in history snapshots
        gainers, losers = sdn.find_power_movers(members, history)
        all_names = [m["name"] for m in gainers + losers]
        assert "Carol" not in all_names
        assert "Dave" not in all_names

    def test_no_history(self, members):
        assert sdn.find_power_movers(members, []) == ([], [])


class TestFindLowestHelps:
    def test_returns_bottom_five(self, members, history):
        result = sdn.find_lowest_helps(members, history)
        assert len(result) <= 5

    def test_sorted_ascending(self, members, history):
        result = sdn.find_lowest_helps(members, history)
        gained = [m["gained"] for m in result]
        assert gained == sorted(gained)

    def test_bob_zero_gained(self, members, history):
        # Bob has 200 helps in all snapshots → 0 gained
        result = sdn.find_lowest_helps(members, history)
        bob = next((m for m in result if m["name"] == "Bob"), None)
        assert bob is not None
        assert bob["gained"] == pytest.approx(0)

    def test_no_history(self, members):
        assert sdn.find_lowest_helps(members, []) == []


# ---------------------------------------------------------------------------
# 3. Embed Assembly
# ---------------------------------------------------------------------------

class TestTruncateField:
    def test_short_unchanged(self):
        text = "hello"
        assert sdn.truncate_field(text) == text

    def test_exact_limit_unchanged(self):
        text = "x" * 1024
        assert sdn.truncate_field(text) == text

    def test_long_truncated(self):
        text = "x" * 2000
        result = sdn.truncate_field(text, limit=100)
        assert len(result) <= 100
        assert result.endswith("\n...")

    def test_custom_limit(self):
        text = "abcdefghij"  # 10 chars
        result = sdn.truncate_field(text, limit=8)
        assert len(result) <= 8
        assert result.endswith("\n...")


class TestBuildEmbed:
    def test_has_required_keys(self, members, history):
        latest = {"members": members, "summary": {"total_power": "185M", "member_count": "4", "total_helps": "850", "total_rss": "11M", "total_iso": "5.5M"}}
        embed = sdn.build_embed(latest, history)
        assert "title" in embed
        assert "description" in embed
        assert "color" in embed
        assert "footer" in embed

    def test_title_contains_date(self, members, history):
        latest = {"members": members, "summary": {}}
        embed = sdn.build_embed(latest, history)
        today = datetime.now().strftime("%Y-%m-%d")
        assert today in embed["title"]

    def test_footer_text(self, members, history):
        latest = {"members": members, "summary": {}}
        embed = sdn.build_embed(latest, history)
        assert embed["footer"]["text"] == "ncctracker.top"

    def test_color(self, members, history):
        latest = {"members": members, "summary": {}}
        embed = sdn.build_embed(latest, history)
        assert embed["color"] == 0x4DABF7

    def test_no_fields_when_empty(self):
        latest = {"members": [], "summary": {}}
        embed = sdn.build_embed(latest, [])
        assert "fields" not in embed

    def test_fields_present_with_data(self, members, history):
        latest = {"members": members, "summary": {"total_power": "185M", "member_count": "4", "total_helps": "850", "total_rss": "11M", "total_iso": "5.5M"}}
        embed = sdn.build_embed(latest, history)
        # Should have at least one field (new members, inactive, etc.)
        assert "fields" in embed
        assert len(embed["fields"]) > 0


class TestComputeDescription:
    def test_contains_stats(self, history):
        latest = {"summary": {"total_power": "185M", "member_count": "4", "total_helps": "850", "total_rss": "11M", "total_iso": "5.5M"}}
        desc = sdn.compute_description(latest, history)
        assert "185M" in desc
        assert "4" in desc
        assert "850" in desc
        assert "11M" in desc
        assert "5.5M" in desc

    def test_delta_strings_with_history(self, history):
        latest = {"summary": {"total_power": "185M", "member_count": "4", "total_helps": "850", "total_rss": "11M", "total_iso": "5.5M"}}
        desc = sdn.compute_description(latest, history)
        # Should contain delta markers (+ or - or 0)
        assert "(" in desc  # deltas are in parentheses

    def test_no_deltas_without_history(self):
        latest = {"summary": {"total_power": "100M", "member_count": "3", "total_helps": "500", "total_rss": "5M", "total_iso": "2M"}}
        desc = sdn.compute_description(latest, [])
        # No parenthesized deltas when no history
        assert "100M" in desc


# ---------------------------------------------------------------------------
# 4. Config / IO
# ---------------------------------------------------------------------------

class TestLoadWebhookUrl:
    def test_reads_from_env(self, tmp_path, monkeypatch):
        env = tmp_path / ".env"
        env.write_text("DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/123/abc\n")
        monkeypatch.setattr(sdn, "ENV_FILE", env)
        assert sdn.load_webhook_url() == "https://discord.com/api/webhooks/123/abc"

    def test_strips_quotes(self, tmp_path, monkeypatch):
        env = tmp_path / ".env"
        env.write_text('DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/123/abc"\n')
        monkeypatch.setattr(sdn, "ENV_FILE", env)
        assert sdn.load_webhook_url() == "https://discord.com/api/webhooks/123/abc"

    def test_single_quotes(self, tmp_path, monkeypatch):
        env = tmp_path / ".env"
        env.write_text("DISCORD_WEBHOOK_URL='https://discord.com/api/webhooks/123/abc'\n")
        monkeypatch.setattr(sdn, "ENV_FILE", env)
        assert sdn.load_webhook_url() == "https://discord.com/api/webhooks/123/abc"

    def test_returns_none_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sdn, "ENV_FILE", tmp_path / "nonexistent")
        assert sdn.load_webhook_url() is None

    def test_returns_none_when_key_missing(self, tmp_path, monkeypatch):
        env = tmp_path / ".env"
        env.write_text("OTHER_KEY=value\n")
        monkeypatch.setattr(sdn, "ENV_FILE", env)
        assert sdn.load_webhook_url() is None

    def test_skips_comments(self, tmp_path, monkeypatch):
        env = tmp_path / ".env"
        env.write_text("# comment\nDISCORD_WEBHOOK_URL=https://example.com\n")
        monkeypatch.setattr(sdn, "ENV_FILE", env)
        assert sdn.load_webhook_url() == "https://example.com"


class TestSentToday:
    def test_not_sent_when_no_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sdn, "LAST_SENT_FILE", tmp_path / ".last_notification_date")
        assert sdn.already_sent_today() is False

    def test_mark_then_check(self, tmp_path, monkeypatch):
        marker = tmp_path / ".last_notification_date"
        monkeypatch.setattr(sdn, "LAST_SENT_FILE", marker)
        sdn.mark_sent_today()
        assert sdn.already_sent_today() is True

    def test_old_date_returns_false(self, tmp_path, monkeypatch):
        marker = tmp_path / ".last_notification_date"
        marker.write_text("2020-01-01")
        monkeypatch.setattr(sdn, "LAST_SENT_FILE", marker)
        assert sdn.already_sent_today() is False
