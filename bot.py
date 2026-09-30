import discord
from discord.ext import commands
from discord import app_commands
import os
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ======================
# 設定
# ======================
DATA_FILE = Path("data.json")
JST = timezone(timedelta(hours=9))

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ======================
# データ管理
# ======================
def load_data():
    if DATA_FILE.exists():
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"users": {}}

def save_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_user(data, user_id: str):
    if user_id not in data["users"]:
        data["users"][user_id] = {
            "active_session": None,
            "records": []
        }
    return data["users"][user_id]

def now_jst():
    return datetime.now(JST)

def today_str():
    return now_jst().strftime("%Y-%m-%d")

# ======================
# 起動時
# ======================
@bot.event
async def on_ready():
    print(f"{bot.user} としてログインしたで！")
    try:
        synced = await bot.tree.sync()
        print(f"スラッシュコマンド同期: {len(synced)}個")
    except Exception as e:
        print(e)

# ======================
# 勉強開始
# ======================
@bot.tree.command(name="start", description="勉強を開始する")
@app_commands.describe(教科="教科名", 教材="教材名")
async def start(interaction: discord.Interaction, 教科: str, 教材: str):
    data = load_data()
    user = get_user(data, str(interaction.user.id))

    if user["active_session"] is not None:
        await interaction.response.send_message("すでに勉強中やで。先に `/end` してな。", ephemeral=True)
        return

    user["active_session"] = {
        "subject": 教科,
        "material": 教材,
        "start_time": now_jst().isoformat()
    }
    save_data(data)

    await interaction.response.send_message(
        f"勉強開始したで！\n教科: **{教科}**\n教材: **{教材}**\n終了するときは `/end` やで。"
    )

# ======================
# 勉強終了
# ======================
@bot.tree.command(name="end", description="勉強を終了して記録する")
@app_commands.describe(問題数="解いた問題数", ページ数="進めたページ数")
async def end(interaction: discord.Interaction, 問題数: int, ページ数: int):
    data = load_data()
    user = get_user(data, str(interaction.user.id))

    if user["active_session"] is None:
        await interaction.response.send_message("勉強開始してないで。先に `/start` してな。", ephemeral=True)
        return

    session = user["active_session"]
    start_time = datetime.fromisoformat(session["start_time"])
    end_time = now_jst()
    minutes = int((end_time - start_time).total_seconds() // 60)

    if minutes < 1:
        minutes = 1  # 最低1分扱い

    record = {
        "date": today_str(),
        "type": "study",
        "subject": session["subject"],
        "material": session["material"],
        "minutes": minutes,
        "problems": 問題数,
        "pages": ページ数,
        "timestamp": end_time.isoformat()
    }
    user["records"].append(record)
    user["active_session"] = None
    save_data(data)

    await interaction.response.send_message(
        f"記録したで！\n"
        f"教科: **{session['subject']}**（{session['material']}）\n"
        f"時間: **{minutes}分**\n"
        f"問題数: **{問題数}**\n"
        f"ページ数: **{ページ数}**"
    )

# ======================
# 手入力で勉強追加
# ======================
@bot.tree.command(name="add", description="後から勉強記録を追加する")
@app_commands.describe(分="勉強した分数", 問題数="解いた問題数", ページ数="進めたページ数", 教科="教科名")
async def add(interaction: discord.Interaction, 分: int, 問題数: int, ページ数: int, 教科: str):
    data = load_data()
    user = get_user(data, str(interaction.user.id))

    record = {
        "date": today_str(),
        "type": "study",
        "subject": 教科,
        "material": "",
        "minutes": 分,
        "problems": 問題数,
        "pages": ページ数,
        "timestamp": now_jst().isoformat()
    }
    user["records"].append(record)
    save_data(data)

    await interaction.response.send_message(
        f"手入力で記録したで！\n"
        f"教科: **{教科}**\n"
        f"時間: **{分}分**\n"
        f"問題数: **{問題数}**\n"
        f"ページ数: **{ページ数}**"
    )

# ======================
# 運動記録（レート用）
# ======================
@bot.tree.command(name="exercise", description="運動時間を記録する（レートに影響）")
@app_commands.describe(分="運動した分数")
async def exercise(interaction: discord.Interaction, 分: int):
    data = load_data()
    user = get_user(data, str(interaction.user.id))

    record = {
        "date": today_str(),
        "type": "exercise",
        "minutes": 分,
        "timestamp": now_jst().isoformat()
    }
    user["records"].append(record)
    save_data(data)

    await interaction.response.send_message(f"運動 **{分}分** 記録したで！（レートに反映される）")

# ======================
# その他の時間（記録のみ）
# ======================
@bot.tree.command(name="break", description="勉強以外の時間を記録する（レートには影響しない）")
@app_commands.describe(種類="飯・風呂・家事など", 分="分数")
async def break_time(interaction: discord.Interaction, 種類: str, 分: int):
    data = load_data()
    user = get_user(data, str(interaction.user.id))

    record = {
        "date": today_str(),
        "type": "break",
        "category": 種類,
        "minutes": 分,
        "timestamp": now_jst().isoformat()
    }
    user["records"].append(record)
    save_data(data)

    await interaction.response.send_message(f"**{種類}** を **{分}分** 記録したで。（レートには影響せん）")

# ======================
# 今日の記録確認
# ======================
@bot.tree.command(name="today", description="今日の自分の記録を見る")
async def today(interaction: discord.Interaction):
    data = load_data()
    user = get_user(data, str(interaction.user.id))
    today = today_str()

    today_records = [r for r in user["records"] if r.get("date") == today]

    if not today_records:
        await interaction.response.send_message("今日はまだ記録がないで。", ephemeral=True)
        return

    lines = []
    total_study = 0
    total_exercise = 0

    for r in today_records:
        if r["type"] == "study":
            lines.append(f"📖 {r['subject']}  {r['minutes']}分  問題{r['problems']}  ページ{r['pages']}")
            total_study += r["minutes"]
        elif r["type"] == "exercise":
            lines.append(f"🏃 運動  {r['minutes']}分")
            total_exercise += r["minutes"]
        elif r["type"] == "break":
            lines.append(f"☕ {r['category']}  {r['minutes']}分")

    text = "\n".join(lines)
    text += f"\n\n合計勉強時間: **{total_study}分**"
    if total_exercise > 0:
        text += f"\n合計運動時間: **{total_exercise}分**"

    await interaction.response.send_message(f"**今日の記録**\n{text}")

# ======================
# 今の状態確認（stats）
# ======================
@bot.tree.command(name="stats", description="今勉強中かどうかを確認する")
async def stats(interaction: discord.Interaction):
    data = load_data()
    user = get_user(data, str(interaction.user.id))

    if user["active_session"] is None:
        await interaction.response.send_message("今は勉強してないで。", ephemeral=True)
    else:
        s = user["active_session"]
        start_time = datetime.fromisoformat(s["start_time"])
        elapsed = int((now_jst() - start_time).total_seconds() // 60)
        await interaction.response.send_message(
            f"今勉強中やで！\n"
            f"教科: **{s['subject']}**（{s['material']}）\n"
            f"経過時間: 約**{elapsed}分**"
        )

# ======================
# 起動
# ======================
bot.run(os.environ["DISCORD_TOKEN"])
