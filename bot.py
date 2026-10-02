import asyncio
import io
import json
import os
from datetime import datetime, timedelta, timezone, time
from pathlib import Path
from typing import Optional

import asyncpg
import discord
from discord import app_commands, ui
from discord.ext import commands

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from PIL import Image, ImageDraw, ImageFont

# ============================================================
# 設定
# ============================================================

JST = timezone(timedelta(hours=9))
SUBJECTS = ["国語", "数学", "社会", "理科", "英語", "情報", "塾", "運動", "睡眠", "仮眠", "その他"]

# Railway の環境変数に入れる
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# スクリーンタイム通知を送る固定チャンネルIDに変更する
SCREEN_TIME_CHANNEL_ID = int(os.getenv("SCREEN_TIME_CHANNEL_ID", "123456789012345678"))

# 既存 data.json がある場合の一度きり移行を許可
MIGRATE_JSON = os.getenv("MIGRATE_JSON", "true").lower() == "true"
DATA_FILE = Path("data.json")

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# DB接続プール。データの正本はPostgreSQL。
db_pool: Optional[asyncpg.Pool] = None

# Discord側の短時間の二重実行防止。DB側にも制約を入れるので二重対策。
data_lock = asyncio.Lock()
active_monitors = {}
active_pomodoros = {}
screen_time_scheduler_task = None

# ============================================================
# 時刻ヘルパー
# ============================================================

def now_jst() -> datetime:
    return datetime.now(JST)


def today_str() -> str:
    return now_jst().strftime("%Y-%m-%d")


def parse_hm(value: str) -> time:
    return datetime.strptime(value.strip(), "%H:%M").time()


def parse_bedtime_input(value: str, now: datetime) -> datetime:
    """深夜～早朝に前夜の就寝時刻を入力した場合は前日扱い。"""
    t = parse_hm(value)
    dt = now.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
    if now.hour < 12 and t > now.time():
        dt -= timedelta(days=1)
    return dt


def iso_or_none(value):
    return value.isoformat() if value else None


# ============================================================
# DB
# ============================================================

async def db_init():
    global db_pool

    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL が設定されてへん。RailwayのPostgreSQLをBotサービスに接続してな。"
        )

    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=30,
    )

    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                discord_id BIGINT PRIMARY KEY,
                study_rate INTEGER NOT NULL DEFAULT 3000,
                exercise_rate INTEGER NOT NULL DEFAULT 3000,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS active_sessions (
                discord_id BIGINT PRIMARY KEY REFERENCES users(discord_id) ON DELETE CASCADE,
                subject TEXT NOT NULL,
                material TEXT NOT NULL DEFAULT '',
                start_datetime TIMESTAMPTZ NOT NULL,
                channel_id BIGINT,
                warning_sent_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS records (
                id BIGSERIAL PRIMARY KEY,
                discord_id BIGINT NOT NULL REFERENCES users(discord_id) ON DELETE CASCADE,
                record_date DATE NOT NULL,
                type TEXT NOT NULL,
                subject TEXT NOT NULL,
                material TEXT NOT NULL DEFAULT '',
                minutes INTEGER NOT NULL CHECK (minutes >= 0),
                problems INTEGER NOT NULL DEFAULT 0 CHECK (problems >= 0),
                pages INTEGER NOT NULL DEFAULT 0 CHECK (pages >= 0),
                rate_after INTEGER NOT NULL DEFAULT 3000,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                start_datetime TIMESTAMPTZ,
                end_datetime TIMESTAMPTZ,
                source TEXT NOT NULL DEFAULT 'manual',
                source_key TEXT UNIQUE
            )
        """)

        await conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_screen_time_per_user_day
            ON records(discord_id, record_date)
            WHERE type = 'screen_time'
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS screen_time_status (
                discord_id BIGINT NOT NULL REFERENCES users(discord_id) ON DELETE CASCADE,
                target_date DATE NOT NULL,
                prompted_at TIMESTAMPTZ,
                auto_recorded_at TIMESTAMPTZ,
                missing_notified_at TIMESTAMPTZ,
                PRIMARY KEY(discord_id, target_date)
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        # 旧版で追加された可能性がある列を安全に補完
        await conn.execute(
            "ALTER TABLE records ADD COLUMN IF NOT EXISTS start_datetime TIMESTAMPTZ"
        )
        await conn.execute(
            "ALTER TABLE records ADD COLUMN IF NOT EXISTS end_datetime TIMESTAMPTZ"
        )
        await conn.execute(
            "ALTER TABLE records ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'manual'"
        )
        await conn.execute(
            "ALTER TABLE records ADD COLUMN IF NOT EXISTS source_key TEXT"
        )
        await conn.execute(
            "ALTER TABLE active_sessions ADD COLUMN IF NOT EXISTS warning_sent_at TIMESTAMPTZ"
        )
        await conn.execute(
            "ALTER TABLE screen_time_status ADD COLUMN IF NOT EXISTS missing_notified_at TIMESTAMPTZ"
        )

    await migrate_json_once()


async def ensure_user(conn, discord_id: int):
    await conn.execute("""
        INSERT INTO users(discord_id)
        VALUES($1)
        ON CONFLICT(discord_id) DO NOTHING
    """, discord_id)


async def get_user_row(conn, discord_id: int):
    await ensure_user(conn, discord_id)
    return await conn.fetchrow(
        "SELECT * FROM users WHERE discord_id=$1",
        discord_id
    )


async def get_records(discord_id: int, record_date: Optional[str] = None):
    async with db_pool.acquire() as conn:
        if record_date:
            return await conn.fetch("""
                SELECT * FROM records
                WHERE discord_id=$1 AND record_date=$2::date
                ORDER BY id
            """, discord_id, record_date)

        return await conn.fetch("""
            SELECT * FROM records
            WHERE discord_id=$1
            ORDER BY id
        """, discord_id)


async def insert_record(
    conn,
    *,
    discord_id: int,
    record_date: str,
    record_type: str,
    subject: str,
    material: str,
    minutes: int,
    problems: int = 0,
    pages: int = 0,
    rate_after: int = 3000,
    timestamp: Optional[datetime] = None,
    start_datetime: Optional[datetime] = None,
    end_datetime: Optional[datetime] = None,
    source: str = "manual",
    source_key: Optional[str] = None,
):
    try:
        return await conn.fetchrow("""
            INSERT INTO records(
                discord_id,
                record_date,
                type,
                subject,
                material,
                minutes,
                problems,
                pages,
                rate_after,
                timestamp,
                start_datetime,
                end_datetime,
                source,
                source_key
            )
            VALUES(
                $1,$2::date,$3,$4,$5,$6,$7,$8,$9,$10,
                $11,$12,$13,$14
            )
            RETURNING *
        """,
            discord_id,
            record_date,
            record_type,
            subject,
            material,
            minutes,
            problems,
            pages,
            rate_after,
            timestamp or now_jst(),
            start_datetime,
            end_datetime,
            source,
            source_key
        )
    except asyncpg.UniqueViolationError:
        return None


# ============================================================
# 旧 data.json → PostgreSQL 一度きり移行
# ============================================================

async def migrate_json_once():
    if not MIGRATE_JSON or not DATA_FILE.exists():
        return

    async with db_pool.acquire() as conn:
        done = await conn.fetchval(
            "SELECT value FROM bot_meta WHERE key='json_migration_done'"
        )

        if done == "1":
            return

        try:
            raw = json.loads(
                DATA_FILE.read_text(encoding="utf-8")
            )
        except Exception as e:
            print(f"[MIGRATION] data.json 読み込み失敗: {e}")
            return

        users = raw.get("users", {})
        migrated = 0

        async with conn.transaction():
            for uid_text, old_user in users.items():
                try:
                    uid = int(uid_text)
                except ValueError:
                    continue

                await conn.execute("""
                    INSERT INTO users(
                        discord_id,
                        study_rate,
                        exercise_rate
                    )
                    VALUES($1,$2,$3)
                    ON CONFLICT(discord_id) DO UPDATE SET
                        study_rate=EXCLUDED.study_rate,
                        exercise_rate=EXCLUDED.exercise_rate,
                        updated_at=NOW()
                """,
                    uid,
                    int(old_user.get("study_rate", 3000)),
                    int(old_user.get("exercise_rate", 3000))
                )

                active = old_user.get("active_session")

                if active:
                    try:
                        start_dt = datetime.fromisoformat(
                            active["start_time"]
                        )

                        await conn.execute("""
                            INSERT INTO active_sessions(
                                discord_id,
                                subject,
                                material,
                                start_datetime
                            )
                            VALUES($1,$2,$3,$4)
                            ON CONFLICT(discord_id) DO NOTHING
                        """,
                            uid,
                            active.get("subject", "その他"),
                            active.get("material", ""),
                            start_dt
                        )
                    except Exception as e:
                        print(
                            f"[MIGRATION] active session {uid}: {e}"
                        )

                for r in old_user.get("records", []):
                    try:
                        rid = int(r.get("id"))
                        rdate = r.get("date") or today_str()
                        rtype = r.get("type", "study")
                        subject = r.get("subject", "その他")
                        minutes = max(
                            0,
                            int(r.get("minutes", 0))
                        )

                        timestamp = (
                            datetime.fromisoformat(r["timestamp"])
                            if r.get("timestamp")
                            else now_jst()
                        )

                        start_dt = (
                            datetime.fromisoformat(r["bedtime"])
                            if r.get("bedtime")
                            else None
                        )

                        end_dt = (
                            datetime.fromisoformat(r["wake_time"])
                            if r.get("wake_time")
                            else None
                        )

                        if not start_dt and r.get("start_datetime"):
                            start_dt = datetime.fromisoformat(
                                r["start_datetime"]
                            )

                        if not end_dt and r.get("end_datetime"):
                            end_dt = datetime.fromisoformat(
                                r["end_datetime"]
                            )

                        await conn.execute("""
                            INSERT INTO records(
                                id,
                                discord_id,
                                record_date,
                                type,
                                subject,
                                material,
                                minutes,
                                problems,
                                pages,
                                rate_after,
                                timestamp,
                                start_datetime,
                                end_datetime,
                                source,
                                source_key
                            )
                            VALUES(
                                $1,$2,$3::date,$4,$5,$6,$7,$8,$9,$10,
                                $11,$12,$13,$14,$15
                            )
                            ON CONFLICT(id) DO NOTHING
                        """,
                            rid,
                            uid,
                            rdate,
                            rtype,
                            subject,
                            r.get("material", ""),
                            minutes,
                            max(
                                0,
                                int(r.get("problems", 0))
                            ),
                            max(
                                0,
                                int(r.get("pages", 0))
                            ),
                            int(
                                r.get(
                                    "rate_after",
                                    old_user.get("study_rate", 3000)
                                )
                            ),
                            timestamp,
                            start_dt,
                            end_dt,
                            r.get("source", "legacy"),
                            f"legacy:{uid}:{rid}"
                        )

                        migrated += 1

                    except Exception as e:
                        print(
                            f"[MIGRATION] record {uid}/{r.get('id')}: {e}"
                        )

            await conn.execute("""
                SELECT setval(
                    pg_get_serial_sequence('records','id'),
                    COALESCE(
                        (SELECT MAX(id) FROM records),
                        1
                    ),
                    true
                )
            """)

            await conn.execute("""
                INSERT INTO bot_meta(key,value)
                VALUES('json_migration_done','1')
                ON CONFLICT(key)
                DO UPDATE SET value='1'
            """)

        print(f"[MIGRATION] 完了: {migrated} 件")


# ============================================================
# 統計画像
# ============================================================

def _find_font(size=10, bold=False):
    """Railway/Linuxでも日本語を描けるフォントを探す。"""

    candidates = [
        (
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"
            if bold
            else
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
        ),
        (
            "/usr/share/fonts/opentype/noto/NotoSansCJKJP-Bold.otf"
            if bold
            else
            "/usr/share/fonts/opentype/noto/NotoSansCJKJP-Regular.otf"
        ),
        (
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc"
            if bold
            else
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc"
        ),
        (
            "/System/Library/Fonts/ヒラギノ角ゴシック W6.ttc"
            if bold
            else
            "/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc"
        ),
    ]

    for path in candidates:
        if Path(path).exists():
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                pass

    try:
        return ImageFont.truetype(
            "DejaVuSans-Bold.ttf"
            if bold
            else
            "DejaVuSans.ttf",
            size
        )
    except Exception:
        return ImageFont.load_default()


def _find_matplotlib_font(bold=False):
    """Matplotlib用の日本語フォントを探す。"""

    candidates = [
        (
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"
            if bold
            else
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
        ),
        (
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc"
            if bold
            else
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc"
        ),
    ]

    for path in candidates:
        if Path(path).exists():
            return path

    return None


# レート帯はここを変えるだけで好きなランク制度に変更できる。
# (ランク名, 下限, 上限, 表示色)

RATE_RANKS = [
    ("BRONZE", 0, 999, "#CD7F32"),
    ("SILVER", 1000, 1999, "#BFC7D5"),
    ("GOLD", 2000, 2999, "#FFD34D"),
    ("PLATINUM", 3000, 3999, "#4DD9FF"),
    ("DIAMOND", 4000, 4999, "#8C7CFF"),
    ("MASTER", 5000, 999999, "#FF4FA3"),
]


def get_rate_rank(rate: int):
    rate = max(0, int(rate))

    for name, lower, upper, color in RATE_RANKS:
        if lower <= rate <= upper:
            return name, lower, upper, color

    return RATE_RANKS[-1]


def _fmt_minutes(minutes: int) -> str:
    minutes = max(0, int(minutes))
    h, m = divmod(minutes, 60)

    if h and m:
        return f"{h}時間{m}分"

    if h:
        return f"{h}時間"

    return f"{m}分"


def _fmt_rate(rate: int) -> str:
    return f"{int(rate):,}"


def generate_stats_image(
    user_name: str,
    records: list,
    current_rate: int
) -> io.BytesIO:

    """
    /stats 用のレートメインカード。

    レイアウト:
      ・左上: ランク色のレートゲージ + 現在レート + 次ランクまで
      ・左上中央: Peak Rate
      ・左下: 睡眠/勉強/仮眠/スクリーンタイムの平均・合計・最大・最小
      ・右半分: レート変動履歴
    """

    records = list(records or [])
    current_rate = int(current_rate or 0)

    # --------------------------------------------------------
    # レート履歴
    # --------------------------------------------------------

    rate_points = []

    for r in sorted(records, key=lambda x: x["id"]):
        try:
            rate = int(r["rate_after"])
        except (KeyError, TypeError, ValueError):
            continue

        # スクリーンタイム等はレート履歴に入れない。
        if r.get("type") == "screen_time":
            continue

        # 勉強系の記録を中心に履歴化。
        if (
            r.get("subject")
            not in ["睡眠", "仮眠", "運動"]
            and r.get("type") != "break"
        ):
            rate_points.append(
                (r["id"], rate)
            )

    if not rate_points:
        rate_points = [(0, current_rate)]

    elif rate_points[-1][1] != current_rate:
        rate_points.append(
            (
                rate_points[-1][0] + 1,
                current_rate
            )
        )

    peak_rate = max(
        [current_rate] +
        [p[1] for p in rate_points]
    )

    rank_name, rank_min, rank_max, rank_color = get_rate_rank(
        current_rate
    )

    next_rank = None
    next_rank_rate = None

    rank_index = next(
        (
            i
            for i, x in enumerate(RATE_RANKS)
            if x[0] == rank_name
        ),
        len(RATE_RANKS) - 1
    )

    if rank_index + 1 < len(RATE_RANKS):
        next_rank = RATE_RANKS[rank_index + 1][0]
        next_rank_rate = RATE_RANKS[rank_index + 1][1]

    if next_rank_rate is None:
        gauge_ratio = 1.0
        to_next_text = "MAX RANK"
    else:
        gauge_ratio = min(
            1.0,
            max(
                0.0,
                (
                    current_rate - rank_min
                ) / max(
                    1,
                    next_rank_rate - rank_min
                )
            )
        )

        to_next_text = (
            f"あと "
            f"{_fmt_rate(max(0, next_rank_rate - current_rate))}"
            f" → {next_rank}"
        )

    # --------------------------------------------------------
    # 4種類の時間統計
    #
    # 「平均/最大/最小」は、
    # その種類の記録が存在する日の1日合計で計算。
    # --------------------------------------------------------

    daily = {
        "勉強時間": {},
        "睡眠時間": {},
        "仮眠時間": {},
        "スクリーンタイム": {},
    }

    for r in records:
        try:
            mins = max(
                0,
                int(r["minutes"])
            )
        except (KeyError, TypeError, ValueError):
            continue

        date = str(
            r.get("record_date") or ""
        )

        if not date:
            continue

        if r.get("type") == "screen_time":
            key = "スクリーンタイム"

        elif r.get("subject") == "睡眠":
            key = "睡眠時間"

        elif r.get("subject") == "仮眠":
            key = "仮眠時間"

        elif (
            r.get("subject") not in ["運動"]
            and r.get("type") != "break"
        ):
            key = "勉強時間"

        else:
            continue

        daily[key][date] = (
            daily[key].get(date, 0) +
            mins
        )

    time_stats = {}

    for key, per_day in daily.items():
        values = list(per_day.values())

        if values:
            time_stats[key] = {
                "avg": round(
                    sum(values) / len(values)
                ),
                "total": sum(values),
                "max": max(values),
                "min": min(values),
            }

        else:
            time_stats[key] = {
                "avg": 0,
                "total": 0,
                "max": 0,
                "min": 0,
            }

    # --------------------------------------------------------
    # キャンバス
    # --------------------------------------------------------

    width, height = 1600, 900

    bg = "#0A0B0D"
    panel = "#111317"
    panel2 = "#0E1013"
    white = "#F4F6F8"
    muted = "#8C939D"
    faint = "#454B54"
    accent = rank_color

    base = Image.new(
        "RGB",
        (width, height),
        bg
    )

    draw = ImageDraw.Draw(base)

    # 背景: 1枚目のような控えめな幾何学模様

    draw.polygon(
        [(0, 0), (560, 0), (420, 900), (0, 900)],
        fill="#0D0F12"
    )

    draw.polygon(
        [
            (560, 0),
            (980, 0),
            (790, 900),
            (420, 900)
        ],
        fill="#0B0D10"
    )

    draw.line(
        [(560, 0), (420, 900)],
        fill="#252A30",
        width=2
    )

    draw.line(
        [(980, 0), (790, 900)],
        fill="#1D2228",
        width=2
    )

    draw.line(
        [(0, 720), (740, 0)],
        fill="#1B2026",
        width=2
    )

    # 角に細いフレーム

    draw.rectangle(
        (22, 22, width - 22, height - 22),
        outline="#242930",
        width=2
    )

    # フォント

    f_title = _find_font(42, True)
    f_subtitle = _find_font(16, False)
    f_section = _find_font(24, True)
    f_metric = _find_font(62, True)
    f_peak = _find_font(46, True)
    f_big_label = _find_font(19, True)
    f_table_head = _find_font(18, True)
    f_table_label = _find_font(22, True)
    f_table_value = _find_font(19, True)
    f_small = _find_font(15, False)
    f_small_bold = _find_font(16, True)
    f_rank = _find_font(22, True)

    # --------------------------------------------------------
    # ヘッダー
    # --------------------------------------------------------

    draw.text(
        (55, 43),
        "勉強記録Bot",
        fill=white,
        font=f_title
    )

    draw.text(
        (58, 93),
        "STUDY / RATE STATISTICS",
        fill=muted,
        font=f_subtitle
    )

    draw.text(
        (1060, 48),
        "PERSONAL PERFORMANCE",
        fill=muted,
        font=f_subtitle
    )

    draw.text(
        (1060, 75),
        user_name[:24],
        fill=white,
        font=f_section
    )

    draw.line(
        (55, 130, 1545, 130),
        fill="#2A2F36",
        width=2
    )

    # --------------------------------------------------------
    # 左上: レートゲージ
    # --------------------------------------------------------

    gauge_box = (
        55,
        160,
        650,
        490
    )

    draw.rounded_rectangle(
        gauge_box,
        radius=16,
        fill=panel,
        outline="#252A31",
        width=2
    )

    draw.text(
        (85, 185),
        "RATE",
        fill=muted,
        font=f_big_label
    )

    draw.text(
        (85, 220),
        rank_name,
        fill=accent,
        font=f_rank
    )

    # ゲージ

    cx, cy = 270, 340
    outer = 112
    width_arc = 25

    bbox = (
        cx - outer,
        cy - outer,
        cx + outer,
        cy + outer
    )

    draw.arc(
        bbox,
        start=135,
        end=405,
        fill="#30353C",
        width=width_arc
    )

    if gauge_ratio > 0:
        draw.arc(
            bbox,
            start=135,
            end=135 + int(270 * gauge_ratio),
            fill=accent,
            width=width_arc
        )

    inner = (
        outer -
        width_arc -
        9
    )

    draw.ellipse(
        (
            cx - inner,
            cy - inner,
            cx + inner,
            cy + inner
        ),
        fill="#0B0D10"
    )

    rate_text = _fmt_rate(current_rate)

    rate_bbox = draw.textbbox(
        (0, 0),
        rate_text,
        font=f_metric
    )

    draw.text(
        (
            cx -
            (rate_bbox[2] - rate_bbox[0]) / 2,
            cy - 45
        ),
        rate_text,
        fill=white,
        font=f_metric
    )

    draw.text(
        (
            cx - 46,
            cy + 24
        ),
        "RATE",
        fill=muted,
        font=f_small_bold
    )

    # 次ランク情報

    draw.text(
        (430, 240),
        "PEAK RATE",
        fill=muted,
        font=f_big_label
    )

    draw.text(
        (430, 270),
        _fmt_rate(peak_rate),
        fill=white,
        font=f_peak
    )

    draw.text(
        (430, 340),
        "NEXT RANK",
        fill=muted,
        font=f_big_label
    )

    draw.text(
        (430, 370),
        to_next_text,
        fill=accent,
        font=f_small_bold
    )

    draw.text(
        (430, 418),
        "現在ランク",
        fill=muted,
        font=f_small
    )

    draw.text(
        (430, 442),
        rank_name,
        fill=white,
        font=f_section
    )

    # --------------------------------------------------------
    # 左下: 時間統計表（線なし）
    # --------------------------------------------------------

    table_box = (
        55,
        520,
        770,
        845
    )

    draw.rounded_rectangle(
        table_box,
        radius=16,
        fill=panel2,
        outline="#252A31",
        width=2
    )

    draw.text(
        (85, 545),
        "TIME STATISTICS",
        fill=white,
        font=f_section
    )

    draw.text(
        (85, 578),
        "睡眠・勉強・仮眠・スクリーンタイム",
        fill=muted,
        font=f_small
    )

    col_x = [
        365,
        475,
        585,
        695
    ]

    headers = [
        "平均",
        "合計",
        "最大",
        "最小"
    ]

    for x, h in zip(col_x, headers):
        draw.text(
            (x, 624),
            h,
            fill=muted,
            font=f_table_head,
            anchor="mm"
        )

    rows = [
        "睡眠時間",
        "勉強時間",
        "仮眠時間",
        "スクリーンタイム"
    ]

    row_y = [
        674,
        719,
        764,
        809
    ]

    row_accent = [
        "#A7B6FF",
        "#FFFFFF",
        "#C58BFF",
        "#55D9FF"
    ]

    for label, y, row_color in zip(
        rows,
        row_y,
        row_accent
    ):
        draw.text(
            (85, y),
            label,
            fill=row_color,
            font=f_table_label,
            anchor="lm"
        )

        st = time_stats[label]

        vals = [
            st["avg"],
            st["total"],
            st["max"],
            st["min"]
        ]

        for x, val in zip(
            col_x,
            vals
        ):
            draw.text(
                (x, y),
                _fmt_minutes(val),
                fill=white,
                font=f_table_value,
                anchor="mm"
            )

    # --------------------------------------------------------
    # 右半分: レート変動履歴
    # --------------------------------------------------------

    graph_box = (
        850,
        160,
        1545,
        845
    )

    draw.rounded_rectangle(
        graph_box,
        radius=16,
        fill=panel,
        outline="#252A31",
        width=2
    )

    draw.text(
        (885, 188),
        "RATE HISTORY",
        fill=white,
        font=f_section
    )

    draw.text(
        (885, 225),
        "レート変動履歴",
        fill=muted,
        font=f_small
    )

    draw.text(
        (1320, 191),
        f"PEAK  {_fmt_rate(peak_rate)}",
        fill=accent,
        font=f_small_bold
    )

    draw.text(
        (1320, 225),
        f"NOW  {_fmt_rate(current_rate)}",
        fill=white,
        font=f_small_bold
    )

    # グラフをMatplotlibで作る

    graph_w = 650
    graph_h = 515

    fig = plt.figure(
        figsize=(
            graph_w / 100,
            graph_h / 100
        ),
        dpi=100
    )

    ax = fig.add_axes(
        [
            0.10,
            0.11,
            0.86,
            0.78
        ]
    )

    fig.patch.set_facecolor(panel)
    ax.set_facecolor(panel)

    xs = list(
        range(
            1,
            len(rate_points) + 1
        )
    )

    ys = [
        p[1]
        for p in rate_points
    ]

    mpl_font_path = _find_matplotlib_font(False)

    mpl_font = (
        matplotlib.font_manager.FontProperties(
            fname=mpl_font_path
        )
        if mpl_font_path
        else None
    )

    ax.plot(
        xs,
        ys,
        color=accent,
        linewidth=3.0,
        marker="o",
        markersize=4.5,
        markerfacecolor=white,
        markeredgecolor=accent,
        markeredgewidth=1.2
    )

    ax.fill_between(
        xs,
        ys,
        [min(ys)] * len(ys),
        color=accent,
        alpha=0.07
    )

    # ピークライン

    ax.axhline(
        peak_rate,
        color="#FFFFFF",
        alpha=0.18,
        linewidth=1.0,
        linestyle="--"
    )

    # ランク境界を薄く表示

    for _, lower, _, color in RATE_RANKS:
        if min(ys) <= lower <= max(ys):
            ax.axhline(
                lower,
                color=color,
                alpha=0.10,
                linewidth=1.0
            )

    lo = min(ys)
    hi = max(ys)

    pad = max(
        100,
        int(
            (hi - lo) * 0.16
        )
    )

    ax.set_ylim(
        lo - pad,
        hi + pad
    )

    ax.set_xlim(
        1,
        max(2, len(xs))
    )

    ax.set_xlabel(
        "記録回数",
        color=muted,
        fontsize=10,
        labelpad=8,
        fontproperties=mpl_font
    )

    ax.set_ylabel(
        "RATE",
        color=muted,
        fontsize=10,
        labelpad=8,
        fontproperties=mpl_font
    )

    ax.tick_params(
        colors="#C6CBD2",
        labelsize=9
    )

    ax.grid(
        True,
        color="#2A2F36",
        linestyle="--",
        linewidth=0.55,
        alpha=0.8
    )

    for spine in ax.spines.values():
        spine.set_color("#343A42")
        spine.set_linewidth(1.0)

    # x軸ラベルは最大8点程度に抑える

    if len(xs) > 8:
        step = max(
            1,
            len(xs) // 7
        )

        tick_x = list(
            range(
                1,
                len(xs) + 1,
                step
            )
        )

        if tick_x[-1] != len(xs):
            tick_x.append(len(xs))

        ax.set_xticks(tick_x)

    fig_buf = io.BytesIO()

    fig.savefig(
        fig_buf,
        format="png",
        dpi=100,
        facecolor=panel,
        edgecolor="none"
    )

    plt.close(fig)

    fig_buf.seek(0)

    graph_img = Image.open(
        fig_buf
    ).convert("RGB")

    base.paste(
        graph_img,
        (885, 285)
    )

    # 現在レートをグラフ上部にも補強表示

    draw.text(
        (1230, 255),
        "CURRENT",
        fill=muted,
        font=f_small_bold
    )

    draw.text(
        (1320, 247),
        _fmt_rate(current_rate),
        fill=accent,
        font=f_peak
    )

    out = io.BytesIO()

    base.save(
        out,
        format="PNG",
        optimize=True
    )

    out.seek(0)

    return out


# ============================================================
# 共通の記録・セッション処理
# ============================================================

async def cancel_monitor(user_id: int):
    uid = str(user_id)

    task = active_monitors.pop(
        uid,
        None
    )

    if task and not task.done():
        task.cancel()


async def create_normal_session(
    user_id: int,
    subject: str,
    material: str,
    channel_id: int
):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await ensure_user(
                conn,
                user_id
            )

            exists = await conn.fetchval(
                "SELECT 1 FROM active_sessions WHERE discord_id=$1",
                user_id
            )

            if exists:
                return False

            await conn.execute("""
                INSERT INTO active_sessions(
                    discord_id,
                    subject,
                    material,
                    start_datetime,
                    channel_id
                )
                VALUES($1,$2,$3,$4,$5)
            """,
                user_id,
                subject,
                material,
                now_jst(),
                channel_id
            )

    return True


async def monitor_session(
    user_id: int,
    channel_id: int,
    start_datetime: Optional[datetime] = None
):
    try:
        async with db_pool.acquire() as conn:
            active = await conn.fetchrow(
                "SELECT * FROM active_sessions WHERE discord_id=$1",
                user_id
            )

        if not active:
            return

        start_dt = (
            start_datetime
            or active["start_datetime"]
        )

        elapsed = (
            now_jst() - start_dt
        ).total_seconds()

        warning_sent = (
            active["warning_sent_at"]
            is not None
        )

        # Bot再起動後でも、
        # 開始からの実時間を基準にする。

        if not warning_sent:
            remaining_to_180 = max(
                0,
                int(
                    180 * 60 -
                    elapsed
                )
            )

            if remaining_to_180:
                await asyncio.sleep(
                    remaining_to_180
                )

            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    active = await conn.fetchrow(
                        """
                        SELECT *
                        FROM active_sessions
                        WHERE discord_id=$1
                        FOR UPDATE
                        """,
                        user_id
                    )

                    if not active:
                        return

                    if active["warning_sent_at"] is None:
                        await conn.execute(
                            """
                            UPDATE active_sessions
                            SET warning_sent_at=$1
                            WHERE discord_id=$2
                            """,
                            now_jst(),
                            user_id
                        )

                        should_warn = True

                        channel_id = (
                            active["channel_id"]
                            or channel_id
                        )

                    else:
                        should_warn = False

            if should_warn:
                channel = bot.get_channel(
                    channel_id
                )

                if channel:
                    try:
                        await channel.send(
                            f"<@{user_id}> "
                            f"勉強開始から "
                            f"**180分（3時間）** "
                            f"経ったで！まだ続けてるか？",
                            view=LongSessionView(
                                user_id
                            )
                        )
                    except discord.DiscordException as e:
                        print(
                            f"[MONITOR] "
                            f"180分通知失敗: {e}"
                        )

        else:
            channel_id = (
                active["channel_id"]
                or channel_id
            )

        # 240分までの残り時間を実時間から再計算。

        elapsed = (
            now_jst() - start_dt
        ).total_seconds()

        remaining_to_240 = max(
            0,
            int(
                240 * 60 -
                elapsed
            )
        )

        if remaining_to_240:
            await asyncio.sleep(
                remaining_to_240
            )

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                active = await conn.fetchrow(
                    """
                    SELECT *
                    FROM active_sessions
                    WHERE discord_id=$1
                    FOR UPDATE
                    """,
                    user_id
                )

                if not active:
                    return

                await conn.execute(
                    """
                    DELETE FROM active_sessions
                    WHERE discord_id=$1
                    """,
                    user_id
                )

        channel = bot.get_channel(
            channel_id
        )

        if channel:
            try:
                await channel.send(
                    f"<@{user_id}> "
                    f"🚨 開始から **240分（4時間）** "
                    f"経過したから、自動でタイマーを止めたで。"
                    f"必要なら /add で手動記録してな。"
                )
            except discord.DiscordException as e:
                print(
                    f"[MONITOR] "
                    f"240分通知失敗: {e}"
                )

    except asyncio.CancelledError:
        return

    finally:
        active_monitors.pop(
            str(user_id),
            None
        )


async def resume_active_monitors():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                discord_id,
                channel_id,
                start_datetime
            FROM active_sessions
            WHERE subject <> '睡眠'
            """
        )

    for row in rows:
        uid = row["discord_id"]

        if (
            str(uid) in active_monitors
            and not active_monitors[str(uid)].done()
        ):
            continue

        task = asyncio.create_task(
            monitor_session(
                uid,
                row["channel_id"] or 0,
                row["start_datetime"]
            )
        )

        active_monitors[str(uid)] = task


# ============================================================
# モーダル
# ============================================================

class SleepStartModal(
    ui.Modal,
    title="就寝時刻を入力"
):
    bedtime = ui.TextInput(
        label="就寝時刻",
        placeholder="23:30",
        required=True,
        max_length=5
    )

    def __init__(
        self,
        user_id: int,
        material: str = ""
    ):
        super().__init__()

        self.user_id = user_id
        self.material = material

    async def on_submit(
        self,
        interaction: discord.Interaction
    ):
        try:
            parse_hm(
                self.bedtime.value
            )

            bedtime = parse_bedtime_input(
                self.bedtime.value,
                now_jst()
            )

        except ValueError:
            await interaction.response.send_message(
                "就寝時刻は HH:MM 形式（例: 23:30）で入力してな。",
                ephemeral=True
            )
            return

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await ensure_user(
                    conn,
                    self.user_id
                )

                if await conn.fetchval(
                    """
                    SELECT 1
                    FROM active_sessions
                    WHERE discord_id=$1
                    """,
                    self.user_id
                ):
                    await interaction.response.send_message(
                        "すでに別の記録を開始中やで。",
                        ephemeral=True
                    )
                    return

                await conn.execute(
                    """
                    INSERT INTO active_sessions(
                        discord_id,
                        subject,
                        material,
                        start_datetime,
                        channel_id
                    )
                    VALUES(
                        $1,
                        '睡眠',
                        $2,
                        $3,
                        $4
                    )
                    """,
                    self.user_id,
                    self.material,
                    bedtime,
                    interaction.channel_id
                )

        await interaction.response.send_message(
            f"就寝時刻を "
            f"**{bedtime.strftime('%Y-%m-%d %H:%M')}** "
            f"として開始したで！\n"
            f"起きたら `/end` して起床時刻を入力してな。"
        )


class SleepEndModal(
    ui.Modal,
    title="起床時刻を入力"
):
    wake_time = ui.TextInput(
        label="起床時刻",
        placeholder="07:00",
        required=True,
        max_length=5
    )

    def __init__(self, user_id: int):
        super().__init__()
        self.user_id = user_id

    async def on_submit(
        self,
        interaction: discord.Interaction
    ):
        try:
            wt = parse_hm(
                self.wake_time.value
            )
        except ValueError:
            await interaction.response.send_message(
                "起床時刻は HH:MM 形式（例: 07:00）で入力してな。",
                ephemeral=True
            )
            return

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                active = await conn.fetchrow(
                    """
                    SELECT *
                    FROM active_sessions
                    WHERE discord_id=$1
                    FOR UPDATE
                    """,
                    self.user_id
                )

                if (
                    not active
                    or active["subject"] != "睡眠"
                ):
                    await interaction.response.send_message(
                        "睡眠の開始記録が見つからんかったわ。",
                        ephemeral=True
                    )
                    return

                bedtime = active[
                    "start_datetime"
                ]

                wake = bedtime.replace(
                    hour=wt.hour,
                    minute=wt.minute,
                    second=0,
                    microsecond=0
                )

                if wake <= bedtime:
                    wake += timedelta(days=1)

                minutes = int(
                    (
                        wake - bedtime
                    ).total_seconds() // 60
                )

                if minutes <= 0 or minutes > 1440:
                    await interaction.response.send_message(
                        "睡眠時間が不正やで。就寝・起床時刻を確認してな。",
                        ephemeral=True
                    )
                    return

                user = await get_user_row(
                    conn,
                    self.user_id
                )

                row = await insert_record(
                    conn,
                    discord_id=self.user_id,
                    record_date=bedtime.strftime(
                        "%Y-%m-%d"
                    ),
                    record_type="睡眠",
                    subject="睡眠",
                    material="",
                    minutes=minutes,
                    rate_after=user["study_rate"],
                    timestamp=now_jst(),
                    start_datetime=bedtime,
                    end_datetime=wake,
                    source="session",
                    source_key=(
                        f"sleep-session:"
                        f"{self.user_id}:"
                        f"{bedtime.isoformat()}"
                    ),
                )

                if row is None:
                    await interaction.response.send_message(
                        "この睡眠記録はすでに保存済みやで。",
                        ephemeral=True
                    )
                    return

                await conn.execute(
                    """
                    DELETE FROM active_sessions
                    WHERE discord_id=$1
                    """,
                    self.user_id
                )

                record_id = row["id"]

        await cancel_monitor(
            self.user_id
        )

        h, m = divmod(
            minutes,
            60
        )

        await interaction.response.send_message(
            f"睡眠を記録したで！"
            f"（記録番号: **#{record_id}**）\n"
            f"就寝: **{bedtime.strftime('%m/%d %H:%M')}**\n"
            f"起床: **{wake.strftime('%m/%d %H:%M')}**\n"
            f"睡眠時間: **{h}時間{m}分**"
        )


class SleepAddModal(
    ui.Modal,
    title="睡眠を追加"
):
    bedtime = ui.TextInput(
        label="就寝時刻",
        placeholder="23:30",
        required=True,
        max_length=5
    )

    wake_time = ui.TextInput(
        label="起床時刻",
        placeholder="07:00",
        required=True,
        max_length=5
    )

    def __init__(self, user_id: int):
        super().__init__()
        self.user_id = user_id

    async def on_submit(
        self,
        interaction: discord.Interaction
    ):
        try:
            parse_hm(
                self.bedtime.value
            )

            wt = parse_hm(
                self.wake_time.value
            )

            bedtime = parse_bedtime_input(
                self.bedtime.value,
                now_jst()
            )

            wake = bedtime.replace(
                hour=wt.hour,
                minute=wt.minute,
                second=0,
                microsecond=0
            )

            if wake <= bedtime:
                wake += timedelta(days=1)

            minutes = int(
                (
                    wake - bedtime
                ).total_seconds() // 60
            )

            if minutes <= 0 or minutes > 1440:
                raise ValueError

        except ValueError:
            await interaction.response.send_message(
                "時刻はHH:MM形式で、睡眠時間は24時間以内にしてな。",
                ephemeral=True
            )
            return

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                user = await get_user_row(
                    conn,
                    self.user_id
                )

                row = await insert_record(
                    conn,
                    discord_id=self.user_id,
                    record_date=bedtime.strftime(
                        "%Y-%m-%d"
                    ),
                    record_type="睡眠",
                    subject="睡眠",
                    material="",
                    minutes=minutes,
                    rate_after=user["study_rate"],
                    timestamp=now_jst(),
                    start_datetime=bedtime,
                    end_datetime=wake,
                    source="manual",
                    source_key=(
                        f"sleep-manual:"
                        f"{self.user_id}:"
                        f"{bedtime.isoformat()}:"
                        f"{wake.isoformat()}"
                    ),
                )

                if row is None:
                    await interaction.response.send_message(
                        "同じ睡眠記録がすでに登録されてるで。",
                        ephemeral=True
                    )
                    return

                record_id = row["id"]

        h, m = divmod(
            minutes,
            60
        )

        await interaction.response.send_message(
            f"睡眠を追加したで！"
            f"（記録番号: **#{record_id}**）\n"
            f"就寝: **{bedtime.strftime('%m/%d %H:%M')}** / "
            f"起床: **{wake.strftime('%m/%d %H:%M')}**\n"
            f"睡眠時間: **{h}時間{m}分**"
        )


class LongSessionModal(
    ui.Modal,
    title="記録を修正して終了"
):
    minutes_input = ui.TextInput(
        label="時間（分）",
        placeholder="180",
        required=True,
        max_length=4,
        default="180"
    )

    pages_input = ui.TextInput(
        label="ページ数（任意）",
        placeholder="0",
        required=False,
        max_length=4,
        default="0"
    )

    problems_input = ui.TextInput(
        label="問題数（任意）",
        placeholder="0",
        required=False,
        max_length=4,
        default="0"
    )

    def __init__(self, user_id: int):
        super().__init__()
        self.user_id = user_id

    async def on_submit(
        self,
        interaction: discord.Interaction
    ):
        try:
            minutes = int(
                self.minutes_input.value
            )

            pages = int(
                self.pages_input.value or "0"
            )

            problems = int(
                self.problems_input.value or "0"
            )

            if (
                minutes < 0
                or pages < 0
                or problems < 0
                or minutes > 240
            ):
                raise ValueError

        except ValueError:
            await interaction.response.send_message(
                "時間は0〜240分、ページ数と問題数は0以上で入力してな。",
                ephemeral=True
            )
            return

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                active = await conn.fetchrow(
                    """
                    SELECT *
                    FROM active_sessions
                    WHERE discord_id=$1
                    FOR UPDATE
                    """,
                    self.user_id
                )

                if not active:
                    await interaction.response.send_message(
                        "セッションが見つからんかったわ。",
                        ephemeral=True
                    )
                    return

                user = await get_user_row(
                    conn,
                    self.user_id
                )

                if active["subject"] in [
                    "睡眠",
                    "仮眠",
                    "運動",
                    "塾"
                ]:
                    pages = 0
                    problems = 0

                row = await insert_record(
                    conn,
                    discord_id=self.user_id,
                    record_date=(
                        active["start_datetime"]
                        .astimezone(JST)
                        .strftime("%Y-%m-%d")
                    ),
                    record_type=(
                        "study"
                        if active["subject"]
                        not in [
                            "運動",
                            "睡眠",
                            "仮眠",
                            "塾"
                        ]
                        else active["subject"]
                    ),
                    subject=active["subject"],
                    material=active["material"],
                    minutes=minutes,
                    pages=pages,
                    problems=problems,
                    rate_after=user["study_rate"],
                    timestamp=now_jst(),
                    start_datetime=active["start_datetime"],
                    end_datetime=now_jst(),
                    source="session_correction",
                    source_key=(
                        f"corrected-session:"
                        f"{self.user_id}:"
                        f"{active['start_datetime'].isoformat()}"
                    ),
                )

                if row is None:
                    await interaction.response.send_message(
                        "このセッションはすでに記録済みやで。",
                        ephemeral=True
                    )
                    return

                await conn.execute(
                    """
                    DELETE FROM active_sessions
                    WHERE discord_id=$1
                    """,
                    self.user_id
                )

                record_id = row["id"]
                subject = active["subject"]

        await cancel_monitor(
            self.user_id
        )

        await interaction.response.send_message(
            f"修正して記録したで！"
            f"（記録番号: **#{record_id}**）\n"
            f"教科: **{subject}**\n"
            f"時間: **{minutes}分** / "
            f"ページ: **{pages}** / "
            f"問題: **{problems}**"
        )


class LongSessionView(ui.View):
    def __init__(self, user_id: int):
        super().__init__(
            timeout=3600
        )
        self.user_id = user_id

    @ui.button(
        label="続ける",
        style=discord.ButtonStyle.green
    )
    async def continue_btn(
        self,
        interaction: discord.Interaction,
        button: ui.Button
    ):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "あなたのもんと違うで。",
                ephemeral=True
            )
            return

        for item in self.children:
            item.disabled = True

        await interaction.response.edit_message(
            content=(
                f"<@{self.user_id}> "
                f"了解や！そのまま頑張ってな。"
                f"（最大240分で強制終了やで）"
            ),
            view=self
        )

    @ui.button(
        label="終わる（時間を修正）",
        style=discord.ButtonStyle.primary
    )
    async def end_btn(
        self,
        interaction: discord.Interaction,
        button: ui.Button
    ):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "あなたのもんと違うで。",
                ephemeral=True
            )
            return

        for item in self.children:
            item.disabled = True

        await interaction.response.send_modal(
            LongSessionModal(
                self.user_id
            )
        )


class ScreenTimeModal(
    ui.Modal,
    title="スクリーンタイム入力"
):
    minutes = ui.TextInput(
        label="スクリーンタイム（分）",
        placeholder="120",
        required=True,
        max_length=4
    )

    def __init__(
        self,
        user_id: int,
        target_date: str
    ):
        super().__init__()

        self.user_id = user_id
        self.target_date = target_date

    async def on_submit(
        self,
        interaction: discord.Interaction
    ):
        now = now_jst()

        # 20:30〜23:59 = 今日
        # 00:00〜00:15 = 前日分

        allowed = (
            now.time() >= time(20, 30)
            or now.time() <= time(0, 15)
        )

        if not allowed:
            await interaction.response.send_message(
                "スクリーンタイムの入力時間外やで。"
                "20:30〜0:15の間だけ入力できるで。",
                ephemeral=True
            )
            return

        try:
            minutes = int(
                self.minutes.value.strip()
            )

            if not 0 <= minutes <= 1440:
                raise ValueError

        except ValueError:
            await interaction.response.send_message(
                "スクリーンタイムは0〜1440分の数字で入力してな。",
                ephemeral=True
            )
            return

        # 古いボタンを押した場合の誤記録防止

        expected_date = (
            today_str()
            if now.time() >= time(20, 30)
            else
            (
                now - timedelta(days=1)
            ).strftime("%Y-%m-%d")
        )

        if self.target_date != expected_date:
            await interaction.response.send_message(
                "この入力ボタンは期限切れやで。"
                "最新の通知から入力してな。",
                ephemeral=True
            )
            return

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await ensure_user(
                    conn,
                    self.user_id
                )

                user = await get_user_row(
                    conn,
                    self.user_id
                )

                row = await insert_record(
                    conn,
                    discord_id=self.user_id,
                    record_date=self.target_date,
                    record_type="screen_time",
                    subject="スクリーンタイム",
                    material="本人入力",
                    minutes=minutes,
                    rate_after=user["study_rate"],
                    timestamp=now,
                    source="screen_manual",
                    source_key=(
                        f"screen:"
                        f"{self.user_id}:"
                        f"{self.target_date}"
                    ),
                )

                if row is None:
                    await interaction.response.send_message(
                        "その日のスクリーンタイムはもう記録済みやで。",
                        ephemeral=True
                    )
                    return

                await conn.execute(
                    """
                    INSERT INTO screen_time_status(
                        discord_id,
                        target_date,
                        prompted_at
                    )
                    VALUES($1,$2::date,$3)
                    ON CONFLICT(
                        discord_id,
                        target_date
                    )
                    DO UPDATE SET
                        prompted_at =
                        COALESCE(
                            screen_time_status.prompted_at,
                            EXCLUDED.prompted_at
                        )
                    """,
                    self.user_id,
                    self.target_date,
                    now
                )

                record_id = row["id"]

        await interaction.response.send_message(
            f"スクリーンタイムを **{minutes}分** "
            f"で記録したで！（#{record_id}）",
            ephemeral=True
        )


class ScreenTimeView(ui.View):
    def __init__(
        self,
        user_id: int,
        target_date: str
    ):
        super().__init__(
            timeout=5 * 60 * 60
        )

        self.user_id = user_id
        self.target_date = target_date

    @ui.button(
        label="スクリーンタイムを入力",
        style=discord.ButtonStyle.primary
    )
    async def input_btn(
        self,
        interaction: discord.Interaction,
        button: ui.Button
    ):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "あなた宛ての入力ボタンやないで。",
                ephemeral=True
            )
            return

        await interaction.response.send_modal(
            ScreenTimeModal(
                self.user_id,
                self.target_date
            )
        )


# ============================================================
# スクリーンタイム自動計算
# ============================================================

async def calculate_auto_screen_time(
    conn,
    user_id: int,
    target_date: str
):
    """
    起床(D) → 次の就寝(D以降)
    を活動可能時間とする。

    睡眠そのものは活動時間の外なので
    二重に引かない。

    起床/次の就寝が無ければ
    計算不能として None を返す。
    """

    target = datetime.strptime(
        target_date,
        "%Y-%m-%d"
    ).date()

    sleep_rows = await conn.fetch(
        """
        SELECT
            start_datetime,
            end_datetime
        FROM records
        WHERE discord_id=$1
          AND type='睡眠'
        ORDER BY start_datetime
        """,
        user_id
    )

    wake_candidates = [
        r["end_datetime"]
        for r in sleep_rows
        if (
            r["end_datetime"]
            and
            r["end_datetime"]
                .astimezone(JST)
                .date()
            == target
        )
    ]

    wake_time = (
        max(wake_candidates)
        if wake_candidates
        else None
    )

    if wake_time is None:
        return None

    next_bedtimes = [
        r["start_datetime"]
        for r in sleep_rows
        if (
            r["start_datetime"]
            and
            r["start_datetime"] > wake_time
        )
    ]

    bedtime = (
        min(next_bedtimes)
        if next_bedtimes
        else None
    )

    if (
        bedtime is None
        or bedtime <= wake_time
    ):
        return None

    available = int(
        (
            bedtime - wake_time
        ).total_seconds() // 60
    )

    if (
        available < 0
        or available > 1440
    ):
        return None

    recorded = await conn.fetchval(
        """
        SELECT COALESCE(SUM(minutes),0)
        FROM records
        WHERE discord_id=$1
          AND record_date=$2::date
          AND type NOT IN (
              'sleep',
              '睡眠',
              'screen_time'
          )
          AND subject <> '睡眠'
        """,
        user_id,
        target_date
    )

    recorded = int(
        recorded or 0
    )

    return (
        max(
            0,
            available - recorded
        ),
        available,
        recorded
    )


async def auto_record_screen_time(
    target_date: str
):
    async with db_pool.acquire() as conn:
        users = await conn.fetch(
            """
            SELECT discord_id
            FROM users
            ORDER BY discord_id
            """
        )

        for u in users:
            uid = u["discord_id"]

            async with conn.transaction():
                # 行ロックで同時実行を防ぐ

                await conn.execute(
                    """
                    SELECT discord_id
                    FROM users
                    WHERE discord_id=$1
                    FOR UPDATE
                    """,
                    uid
                )

                exists = await conn.fetchval(
                    """
                    SELECT 1
                    FROM records
                    WHERE discord_id=$1
                      AND record_date=$2::date
                      AND type='screen_time'
                    """,
                    uid,
                    target_date
                )

                if exists:
                    await conn.execute(
                        """
                        INSERT INTO screen_time_status(
                            discord_id,
                            target_date,
                            auto_recorded_at
                        )
                        VALUES($1,$2::date,$3)
                        ON CONFLICT(
                            discord_id,
                            target_date
                        )
                        DO UPDATE SET
                            auto_recorded_at =
                            COALESCE(
                                screen_time_status.auto_recorded_at,
                                EXCLUDED.auto_recorded_at
                            )
                        """,
                        uid,
                        target_date,
                        now_jst()
                    )

                    continue

                result = await calculate_auto_screen_time(
                    conn,
                    uid,
                    target_date
                )

                if result is None:
                    # 無理に1440分などで記録しない
                    continue

                minutes, available, recorded = result

                user = await get_user_row(
                    conn,
                    uid
                )

                row = await insert_record(
                    conn,
                    discord_id=uid,
                    record_date=target_date,
                    record_type="screen_time",
                    subject="スクリーンタイム",
                    material=(
                        f"自動計算"
                        f"（活動可能{available}分"
                        f" - 記録{recorded}分）"
                    ),
                    minutes=minutes,
                    rate_after=user["study_rate"],
                    timestamp=now_jst(),
                    source="screen_auto",
                    source_key=(
                        f"screen:"
                        f"{uid}:"
                        f"{target_date}"
                    ),
                )

                if row:
                    await conn.execute(
                        """
                        INSERT INTO screen_time_status(
                            discord_id,
                            target_date,
                            auto_recorded_at
                        )
                        VALUES($1,$2::date,$3)
                        ON CONFLICT(
                            discord_id,
                            target_date
                        )
                        DO UPDATE SET
                            auto_recorded_at =
                            EXCLUDED.auto_recorded_at
                        """,
                        uid,
                        target_date,
                        now_jst()
                    )

    # 計算できなかったユーザーへ通知

    await notify_screen_time_missing_sleep(
        target_date
    )


async def notify_screen_time_missing_sleep(
    target_date: str
):
    channel = bot.get_channel(
        SCREEN_TIME_CHANNEL_ID
    )

    if not channel:
        return

    async with db_pool.acquire() as conn:
        users = await conn.fetch(
            """
            SELECT discord_id
            FROM users
            ORDER BY discord_id
            """
        )

        for u in users:
            uid = u["discord_id"]

            exists = await conn.fetchval(
                """
                SELECT 1
                FROM records
                WHERE discord_id=$1
                  AND record_date=$2::date
                  AND type='screen_time'
                """,
                uid,
                target_date
            )

            if exists:
                continue

            result = await calculate_auto_screen_time(
                conn,
                uid,
                target_date
            )

            if result is None:
                status = await conn.fetchrow(
                    """
                    SELECT missing_notified_at
                    FROM screen_time_status
                    WHERE discord_id=$1
                      AND target_date=$2::date
                    """,
                    uid,
                    target_date
                )

                if (
                    status
                    and
                    status["missing_notified_at"]
                ):
                    continue

                try:
                    await channel.send(
                        f"<@{uid}> ⚠️ "
                        f"{target_date} のスクリーンタイムを"
                        f"自動計算できんかった。"
                        f"起床時刻と次の就寝時刻の記録が"
                        f"不足してる可能性があるで。",
                        allowed_mentions=discord.AllowedMentions(
                            users=True,
                            roles=False,
                            everyone=False
                        )
                    )

                    await conn.execute(
                        """
                        INSERT INTO screen_time_status(
                            discord_id,
                            target_date,
                            missing_notified_at
                        )
                        VALUES($1,$2::date,$3)
                        ON CONFLICT(
                            discord_id,
                            target_date
                        )
                        DO UPDATE SET
                            missing_notified_at =
                            COALESCE(
                                screen_time_status.missing_notified_at,
                                EXCLUDED.missing_notified_at
                            )
                        """,
                        uid,
                        target_date,
                        now_jst()
                    )

                except discord.DiscordException:
                    pass


async def send_screen_time_prompts(
    target_date: str
):
    channel = bot.get_channel(
        SCREEN_TIME_CHANNEL_ID
    )

    if channel is None:
        try:
            channel = await bot.fetch_channel(
                SCREEN_TIME_CHANNEL_ID
            )
        except Exception as e:
            print(
                f"[SCREEN] "
                f"通知先チャンネル取得失敗: {e}"
            )
            return

    async with db_pool.acquire() as conn:
        users = await conn.fetch(
            """
            SELECT discord_id
            FROM users
            ORDER BY discord_id
            """
        )

    allowed_mentions = discord.AllowedMentions(
        users=True,
        roles=False,
        everyone=False
    )

    for u in users:
        uid = u["discord_id"]

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    SELECT discord_id
                    FROM users
                    WHERE discord_id=$1
                    FOR UPDATE
                    """,
                    uid
                )

                already = await conn.fetchval(
                    """
                    SELECT 1
                    FROM records
                    WHERE discord_id=$1
                      AND record_date=$2::date
                      AND type='screen_time'
                    """,
                    uid,
                    target_date
                )

                status = await conn.fetchrow(
                    """
                    SELECT *
                    FROM screen_time_status
                    WHERE discord_id=$1
                      AND target_date=$2::date
                    FOR UPDATE
                    """,
                    uid,
                    target_date
                )

                if (
                    already
                    or
                    (
                        status
                        and
                        status["prompted_at"]
                    )
                ):
                    continue

                await conn.execute(
                    """
                    INSERT INTO screen_time_status(
                        discord_id,
                        target_date,
                        prompted_at
                    )
                    VALUES($1,$2::date,$3)
                    ON CONFLICT(
                        discord_id,
                        target_date
                    )
                    DO UPDATE SET
                        prompted_at=EXCLUDED.prompted_at
                    """,
                    uid,
                    target_date,
                    now_jst()
                )

        try:
            await channel.send(
                f"<@{uid}> "
                f"今日のスクリーンタイムを入力してな！\n"
                f"入力できるのは "
                f"**20:30〜0:15** の間だけやで。",
                view=ScreenTimeView(
                    uid,
                    target_date
                ),
                allowed_mentions=allowed_mentions
            )

        except discord.DiscordException as e:
            print(
                f"[SCREEN] "
                f"{uid} 通知失敗: {e}"
            )


async def screen_time_scheduler():
    while not bot.is_closed():
        try:
            now = now_jst()

            if now.time() >= time(20, 30):
                await send_screen_time_prompts(
                    now.strftime("%Y-%m-%d")
                )

            elif now.time() >= time(0, 15):
                target = (
                    now - timedelta(days=1)
                ).strftime("%Y-%m-%d")

                await auto_record_screen_time(
                    target
                )

        except asyncio.CancelledError:
            return

        except Exception as e:
            print(
                f"[SCREEN SCHEDULER] "
                f"{type(e).__name__}: {e}"
            )

        await asyncio.sleep(20)


# ============================================================
# ポモドーロ
# ============================================================

class PomodoroModal(
    ui.Modal,
    title="ポモドーロ記録"
):
    subject = ui.TextInput(
        label="やった教科",
        placeholder="国語・数学など",
        required=True,
        max_length=20
    )

    pages = ui.TextInput(
        label="ページ数（任意）",
        placeholder="0",
        required=False,
        max_length=5,
        default="0"
    )

    problems = ui.TextInput(
        label="問題数（任意）",
        placeholder="0",
        required=False,
        max_length=5,
        default="0"
    )

    def __init__(
        self,
        user_id: int,
        is_continue: bool,
        round_key: str
    ):
        super().__init__()

        self.user_id = user_id
        self.is_continue = is_continue
        self.round_key = round_key

    async def on_submit(
        self,
        interaction: discord.Interaction
    ):
        if (
            self.subject.value not in SUBJECTS
            or
            self.subject.value in [
                "睡眠",
                "仮眠",
                "運動",
                "塾"
            ]
        ):
            await interaction.response.send_message(
                "ポモドーロでは勉強科目を選んでな。",
                ephemeral=True
            )
            return

        try:
            pages = int(
                self.pages.value or "0"
            )

            problems = int(
                self.problems.value or "0"
            )

            if (
                pages < 0
                or problems < 0
            ):
                raise ValueError

        except ValueError:
            await interaction.response.send_message(
                "問題数とページ数は0以上の数字で入力してな。",
                ephemeral=True
            )
            return

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                user = await get_user_row(
                    conn,
                    self.user_id
                )

                row = await insert_record(
                    conn,
                    discord_id=self.user_id,
                    record_date=today_str(),
                    record_type="study",
                    subject=self.subject.value,
                    material="ポモドーロ",
                    minutes=25,
                    pages=pages,
                    problems=problems,
                    rate_after=user["study_rate"],
                    timestamp=now_jst(),
                    source="pomodoro",
                    source_key=(
                        f"pomodoro:"
                        f"{self.round_key}"
                    ),
                )

                if row is None:
                    await interaction.response.send_message(
                        "このポモドーロはすでに記録済みやで。"
                        "二重登録は防いどいた。",
                        ephemeral=True
                    )
                    return

                record_id = row["id"]

        state = active_pomodoros.get(
            str(self.user_id)
        )

        if state:
            state["rounds"].append(
                {
                    "subject": self.subject.value,
                    "problems": problems,
                    "pages": pages,
                    "id": record_id
                }
            )

        if self.is_continue:
            await interaction.response.send_message(
                f"記録したで！（#{record_id}）\n"
                f"5分休憩に入るで。"
                f"休憩後にまた通知するわ。",
                ephemeral=True
            )

            task = asyncio.create_task(
                pomodoro_break_and_restart(
                    self.user_id,
                    interaction.channel_id
                )
            )

            if state:
                state["task"] = task

        else:
            rounds = (
                state["rounds"]
                if state
                else []
            )

            summary = "\n".join(
                f"#{r['id']} "
                f"{r['subject']} "
                f"問題{r['problems']} / "
                f"P{r['pages']}"
                for r in rounds
            ) or (
                f"#{record_id} "
                f"{self.subject.value} "
                f"問題{problems} / "
                f"P{pages}"
            )

            embed = discord.Embed(
                title="ポモドーロ終了",
                description=summary,
                color=0x57F287
            )

            embed.set_footer(
                text=(
                    f"合計 "
                    f"{len(rounds) if rounds else 1}"
                    f" ポモドーロ"
                )
            )

            await interaction.response.send_message(
                embed=embed,
                ephemeral=True
            )

            active_pomodoros.pop(
                str(self.user_id),
                None
            )


class PomodoroView(ui.View):
    def __init__(
        self,
        user_id: int,
        round_key: str
    ):
        super().__init__(
            timeout=300
        )

        self.user_id = user_id
        self.round_key = round_key

    @ui.button(
        label="記録して続ける",
        style=discord.ButtonStyle.green
    )
    async def continue_btn(
        self,
        interaction: discord.Interaction,
        button: ui.Button
    ):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "あなたのもんと違うで。",
                ephemeral=True
            )
            return

        await interaction.response.send_modal(
            PomodoroModal(
                self.user_id,
                True,
                self.round_key
            )
        )

    @ui.button(
        label="記録して終了",
        style=discord.ButtonStyle.red
    )
    async def end_btn(
        self,
        interaction: discord.Interaction,
        button: ui.Button
    ):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "あなたのもんと違うで。",
                ephemeral=True
            )
            return

        await interaction.response.send_modal(
            PomodoroModal(
                self.user_id,
                False,
                self.round_key
            )
        )


async def pomodoro_work(
    user_id: int,
    channel_id: int,
    round_key: str
):
    try:
        await asyncio.sleep(
            25 * 60
        )
    except asyncio.CancelledError:
        return

    channel = bot.get_channel(
        channel_id
    )

    if channel is None:
        return

    await channel.send(
        f"<@{user_id}> "
        f"25分終わったで！記録してな。",
        view=PomodoroView(
            user_id,
            round_key
        )
    )


async def pomodoro_break_and_restart(
    user_id: int,
    channel_id: int
):
    try:
        await asyncio.sleep(
            5 * 60
        )
    except asyncio.CancelledError:
        return

    state = active_pomodoros.get(
        str(user_id)
    )

    if not state:
        return

    channel = bot.get_channel(
        channel_id
    )

    if channel is None:
        return

    await channel.send(
        f"<@{user_id}> "
        f"休憩終わり！次の25分スタートやで。"
    )

    round_key = (
        f"{user_id}:"
        f"{now_jst().isoformat()}"
    )

    task = asyncio.create_task(
        pomodoro_work(
            user_id,
            channel_id,
            round_key
        )
    )

    state["task"] = task


# ============================================================
# 起動イベント
# ============================================================

@bot.event
async def on_ready():
    global screen_time_scheduler_task

    print(
        f"{bot.user} としてログインしたで！"
    )

    await resume_active_monitors()

    if (
        screen_time_scheduler_task is None
        or
        screen_time_scheduler_task.done()
    ):
        screen_time_scheduler_task = asyncio.create_task(
            screen_time_scheduler()
        )


@bot.event
async def setup_hook():
    await db_init()


@bot.command()
@commands.is_owner()
async def sync(ctx):
    try:
        synced = await bot.tree.sync()

        await ctx.send(
            f"スラッシュコマンド "
            f"{len(synced)}個 を同期したで！"
        )

    except Exception as e:
        await ctx.send(
            f"エラーや: {e}"
        )


# ============================================================
# コマンド
# ============================================================

@bot.tree.command(
    name="pomodoro",
    description="ポモドーロを開始する（25分）"
)
async def pomodoro(
    interaction: discord.Interaction
):
    uid = str(
        interaction.user.id
    )

    if uid in active_pomodoros:
        await interaction.response.send_message(
            "すでにポモドーロ中やで。",
            ephemeral=True
        )
        return

    round_key = (
        f"{interaction.user.id}:"
        f"{now_jst().isoformat()}"
    )

    task = asyncio.create_task(
        pomodoro_work(
            interaction.user.id,
            interaction.channel_id,
            round_key
        )
    )

    active_pomodoros[uid] = {
        "task": task,
        "rounds": [],
        "channel_id": interaction.channel_id
    }

    await interaction.response.send_message(
        f"{interaction.user.mention} "
        f"ポモドーロ開始！25分カウントするで。"
    )


@bot.tree.command(
    name="start",
    description="勉強・活動を開始する"
)
@app_commands.describe(
    教科="教科を選択",
    教材="教材名（任意）"
)
@app_commands.choices(
    教科=[
        app_commands.Choice(
            name=s,
            value=s
        )
        for s in SUBJECTS
    ]
)
async def start(
    interaction: discord.Interaction,
    教科: app_commands.Choice[str],
    教材: str = ""
):
    uid = interaction.user.id

    if 教科.value == "睡眠":
        async with db_pool.acquire() as conn:
            if await conn.fetchval(
                """
                SELECT 1
                FROM active_sessions
                WHERE discord_id=$1
                """,
                uid
            ):
                await interaction.response.send_message(
                    "すでに開始中やで。先に /end してな。",
                    ephemeral=True
                )
                return

        await interaction.response.send_modal(
            SleepStartModal(
                uid,
                教材
            )
        )

        return

    ok = await create_normal_session(
        uid,
        教科.value,
        教材,
        interaction.channel_id
    )

    if not ok:
        await interaction.response.send_message(
            "すでに開始中やで。先に /end してな。",
            ephemeral=True
        )
        return

    await cancel_monitor(uid)

    task = asyncio.create_task(
        monitor_session(
            uid,
            interaction.channel_id
        )
    )

    active_monitors[str(uid)] = task

    await interaction.response.send_message(
        f"開始したで！\n"
        f"教科: **{教科.value}**\n"
        f"教材: **{教材 or 'なし'}**\n"
        f"終了するときは /end やで。"
    )


@bot.tree.command(
    name="end",
    description="終了して記録する"
)
@app_commands.describe(
    ページ数="進めたページ数（任意・空欄でOK）",
    問題数="解いた問題数（任意・空欄でOK）"
)
async def end(
    interaction: discord.Interaction,
    ページ数: app_commands.Range[int, 0, None] = 0,
    問題数: app_commands.Range[int, 0, None] = 0
):
    uid = interaction.user.id

    async with db_pool.acquire() as conn:
        active = await conn.fetchrow(
            """
            SELECT *
            FROM active_sessions
            WHERE discord_id=$1
            """,
            uid
        )

    if not active:
        await interaction.response.send_message(
            "開始してないで。先に /start してな。",
            ephemeral=True
        )
        return

    if active["subject"] == "睡眠":
        await interaction.response.send_modal(
            SleepEndModal(uid)
        )
        return

    end_dt = now_jst()

    minutes = int(
        (
            end_dt -
            active["start_datetime"]
        ).total_seconds() // 60
    )

    if minutes > 240:
        await interaction.response.send_message(
            "時間が長すぎる（4時間超え）から、"
            "自動記録は止めたで。"
            "/add で手動入力してな！",
            ephemeral=True
        )

        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM active_sessions
                WHERE discord_id=$1
                """,
                uid
            )

        await cancel_monitor(uid)

        return

    minutes = max(
        1,
        minutes
    )

    if active["subject"] in [
        "運動",
        "睡眠",
        "仮眠",
        "塾"
    ]:
        ページ数 = 0
        問題数 = 0

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            locked = await conn.fetchrow(
                """
                SELECT *
                FROM active_sessions
                WHERE discord_id=$1
                FOR UPDATE
                """,
                uid
            )

            if not locked:
                await interaction.response.send_message(
                    "もう終了済みのセッションやで。",
                    ephemeral=True
                )
                return

            user = await get_user_row(
                conn,
                uid
            )

            row = await insert_record(
                conn,
                discord_id=uid,
                record_date=(
                    locked["start_datetime"]
                    .astimezone(JST)
                    .strftime("%Y-%m-%d")
                ),
                record_type=(
                    "study"
                    if locked["subject"]
                    not in [
                        "運動",
                        "睡眠",
                        "仮眠",
                        "塾"
                    ]
                    else locked["subject"]
                ),
                subject=locked["subject"],
                material=locked["material"],
                minutes=minutes,
                pages=ページ数,
                problems=問題数,
                rate_after=user["study_rate"],
                timestamp=end_dt,
                start_datetime=locked["start_datetime"],
                end_datetime=end_dt,
                source="session",
                source_key=(
                    f"session:"
                    f"{uid}:"
                    f"{locked['start_datetime'].isoformat()}"
                ),
            )

            if row is None:
                await interaction.response.send_message(
                    "このセッションはすでに記録済みやで。",
                    ephemeral=True
                )
                return

            await conn.execute(
                """
                DELETE FROM active_sessions
                WHERE discord_id=$1
                """,
                uid
            )

            record_id = row["id"]
            subject = locked["subject"]
            material = locked["material"]

    await cancel_monitor(uid)

    if subject == "塾":
        msg = (
            f"記録したで！"
            f"（記録番号: **#{record_id}**）\n"
            f"教科: **塾**\n"
            f"時間: **{minutes}分**"
        )
    else:
        msg = (
            f"記録したで！"
            f"（記録番号: **#{record_id}**）\n"
            f"教科: **{subject}**"
            f"（{material or 'なし'}）\n"
            f"時間: **{minutes}分** / "
            f"ページ数: **{ページ数}** / "
            f"問題数: **{問題数}**"
        )

    await interaction.response.send_message(
        msg
    )


@bot.tree.command(
    name="add",
    description="後から記録を追加する"
)
@app_commands.describe(
    分="分数（0以上）",
    教科="教科を選択",
    ページ数="ページ数（任意・空欄でOK）",
    問題数="問題数（任意・空欄でOK）"
)
@app_commands.choices(
    教科=[
        app_commands.Choice(
            name=s,
            value=s
        )
        for s in SUBJECTS
    ]
)
async def add(
    interaction: discord.Interaction,
    教科: app_commands.Choice[str],
    分: Optional[
        app_commands.Range[int, 0, None]
    ] = None,
    ページ数: app_commands.Range[int, 0, None] = 0,
    問題数: app_commands.Range[int, 0, None] = 0
):
    if 教科.value == "睡眠":
        await interaction.response.send_modal(
            SleepAddModal(
                interaction.user.id
            )
        )
        return

    if 分 is None:
        await interaction.response.send_message(
            "睡眠以外は分数を入力してな。",
            ephemeral=True
        )
        return

    if 教科.value in [
        "運動",
        "仮眠",
        "塾"
    ]:
        ページ数 = 0
        問題数 = 0

    async with db_pool.acquire() as conn:
        user = await get_user_row(
            conn,
            interaction.user.id
        )

        row = await insert_record(
            conn,
            discord_id=interaction.user.id,
            record_date=today_str(),
            record_type=(
                "study"
                if 教科.value
                not in [
                    "運動",
                    "仮眠",
                    "睡眠",
                    "塾"
                ]
                else 教科.value
            ),
            subject=教科.value,
            material="",
            minutes=分,
            pages=ページ数,
            problems=問題数,
            rate_after=user["study_rate"],
            timestamp=now_jst(),
            source="manual",
            source_key=(
                f"manual:"
                f"{interaction.user.id}:"
                f"{now_jst().isoformat()}:"
                f"{教科.value}:"
                f"{分}"
            ),
        )

        if row is None:
            await interaction.response.send_message(
                "同じ記録がすでに登録されてるで。",
                ephemeral=True
            )
            return

        record_id = row["id"]

    await interaction.response.send_message(
        f"追加したで！"
        f"（記録番号: **#{record_id}**）\n"
        f"教科: **{教科.value}**\n"
        f"時間: **{分}分** / "
        f"ページ数: **{ページ数}** / "
        f"問題数: **{問題数}**"
    )


@bot.tree.command(
    name="clear",
    description="指定した番号の記録を削除する"
)
@app_commands.describe(
    番号="削除したい記録番号"
)
async def clear(
    interaction: discord.Interaction,
    番号: int
):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT *
            FROM records
            WHERE id=$1
              AND discord_id=$2
            """,
            番号,
            interaction.user.id
        )

        if not row:
            await interaction.response.send_message(
                f"番号 **#{番号}** の記録は見つからんかった。",
                ephemeral=True
            )
            return

        await conn.execute(
            """
            DELETE FROM records
            WHERE id=$1
              AND discord_id=$2
            """,
            番号,
            interaction.user.id
        )

    await interaction.response.send_message(
        f"記録 **#{番号}** を削除したで。\n"
        f"（{row['subject']} {row['minutes']}分）"
    )


@bot.tree.command(
    name="today",
    description="今日の自分の記録を見る"
)
async def today(
    interaction: discord.Interaction
):
    async with db_pool.acquire() as conn:
        user = await get_user_row(
            conn,
            interaction.user.id
        )

        records = await conn.fetch(
            """
            SELECT *
            FROM records
            WHERE discord_id=$1
              AND record_date=$2::date
            ORDER BY id
            """,
            interaction.user.id,
            today_str()
        )

    embed = discord.Embed(
        title=f"今日の記録（{today_str()}）",
        color=0x5865F2
    )

    embed.set_author(
        name=interaction.user.display_name,
        icon_url=interaction.user.display_avatar.url
    )

    if not records:
        embed.description = (
            "今日はまだ記録がないで。"
        )

        await interaction.response.send_message(
            embed=embed
        )

        return

    total_study = 0
    total_problems = 0
    total_pages = 0
    total_exercise = 0
    total_sleep = 0
    total_nap = 0
    total_screen = 0

    lines = []

    for r in records:
        if r["type"] == "screen_time":
            lines.append(
                f"#{r['id']} "
                f"📱 スクリーンタイム "
                f"{r['minutes']}分"
            )

            total_screen += r["minutes"]

        elif r["subject"] == "運動":
            lines.append(
                f"#{r['id']} "
                f"🏃 運動 "
                f"{r['minutes']}分"
            )

            total_exercise += r["minutes"]

        elif r["subject"] == "睡眠":
            lines.append(
                f"#{r['id']} "
                f"😴 睡眠 "
                f"{r['minutes']}分"
            )

            total_sleep += r["minutes"]

        elif r["subject"] == "仮眠":
            lines.append(
                f"#{r['id']} "
                f"😪 仮眠 "
                f"{r['minutes']}分"
            )

            total_nap += r["minutes"]

        elif r["subject"] == "塾":
            lines.append(
                f"#{r['id']} "
                f"🏫 塾 "
                f"{r['minutes']}分"
            )

            total_study += r["minutes"]

        else:
            lines.append(
                f"#{r['id']} "
                f"📖 {r['subject']} "
                f"{r['minutes']}分 / "
                f"P{r['pages']} / "
                f"問{r['problems']}"
            )

            total_study += r["minutes"]
            total_problems += r["problems"]
            total_pages += r["pages"]

    embed.description = "\n".join(
        lines
    )

    embed.add_field(
        name="勉強時間",
        value=f"**{total_study}分**",
        inline=True
    )

    embed.add_field(
        name="ページ数",
        value=f"**{total_pages}**",
        inline=True
    )

    embed.add_field(
        name="問題数",
        value=f"**{total_problems}**",
        inline=True
    )

    if total_exercise:
        embed.add_field(
            name="運動",
            value=f"**{total_exercise}分**",
            inline=True
        )

    if total_sleep:
        embed.add_field(
            name="睡眠",
            value=f"**{total_sleep}分**",
            inline=True
        )

    if total_nap:
        embed.add_field(
            name="仮眠",
            value=f"**{total_nap}分**",
            inline=True
        )

    if records:
        embed.add_field(
            name="スクリーンタイム",
            value=f"**{total_screen}分**",
            inline=True
        )

    embed.add_field(
        name="勉強レート",
        value=f"**{user['study_rate']}**",
        inline=True
    )

    embed.add_field(
        name="運動レート",
        value=f"**{user['exercise_rate']}**",
        inline=True
    )

    embed.set_footer(
        text="番号を使って /clear できます"
    )

    await interaction.response.send_message(
        embed=embed
    )


@bot.tree.command(
    name="stats",
    description="自分のスタッツ・レート推移カードを出力する"
)
async def stats(
    interaction: discord.Interaction
):
    await interaction.response.defer()

    async with db_pool.acquire() as conn:
        user = await get_user_row(
            conn,
            interaction.user.id
        )

        records = await conn.fetch(
            """
            SELECT *
            FROM records
            WHERE discord_id=$1
            ORDER BY id
            """,
            interaction.user.id
        )

    loop = asyncio.get_running_loop()

    img_bytes = await loop.run_in_executor(
        None,
        generate_stats_image,
        interaction.user.display_name,
        records,
        user["study_rate"]
    )

    await interaction.followup.send(
        file=discord.File(
            fp=img_bytes,
            filename="stats.png"
        )
    )


# ============================================================
# 終了処理
# ============================================================

async def close_db():
    global db_pool

    if db_pool:
        await db_pool.close()
        db_pool = None


if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise RuntimeError(
            "DISCORD_TOKEN が設定されてへん"
        )

    bot.run(DISCORD_TOKEN)
