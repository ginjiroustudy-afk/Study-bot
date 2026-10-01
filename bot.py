import discord
from discord.ext import commands
from discord import app_commands, ui
import os
import json
import asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ======================
# 設定
# ======================
DATA_FILE = Path("/data/data.json")
JST = timezone(timedelta(hours=9))

SUBJECTS = ["国語", "数学", "社会", "理科", "英語", "情報", "運動", "睡眠", "仮眠", "その他"]

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ポモドーロ用の一時データ（再起動で消える）
active_pomodoros = {}  # user_id: {"task": ..., "rounds": [], "channel_id": ...}

# ======================
# データ管理
# ======================
def load_data():
    if DATA_FILE.exists():
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"users": {}, "next_id": 1}

def save_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_user(data, user_id: str):
    if user_id not in data["users"]:
        data["users"][user_id] = {
            "active_session": None,
            "records": [],
            "study_rate": 3000,
            "exercise_rate": 3000
        }
    user = data["users"][user_id]
    if "study_rate" not in user:
        user["study_rate"] = 3000
    if "exercise_rate" not in user:
        user["exercise_rate"] = 3000
    return user

def now_jst():
    return datetime.now(JST)

def today_str():
    return now_jst().strftime("%Y-%m-%d")

def get_next_id(data):
    nid = data.get("next_id", 1)
    data["next_id"] = nid + 1
    return nid

# ======================
# ポモドーロ用UI
# ======================
class PomodoroModal(ui.Modal, title="ポモドーロ記録"):
    subject = ui.TextInput(label="やった教科", placeholder="国語・数学など", required=True, max_length=20)
    problems = ui.TextInput(label="問題数", placeholder="0", required=True, max_length=5)
    pages = ui.TextInput(label="ページ数", placeholder="0", required=True, max_length=5)

    def __init__(self, user_id: int, is_continue: bool):
        super().__init__()
        self.user_id = user_id
        self.is_continue = is_continue

    async def on_submit(self, interaction: discord.Interaction):
        try:
            problems = int(self.problems.value)
            pages = int(self.pages.value)
        except ValueError:
            await interaction.response.send_message("問題数とページ数は数字で入力してな。", ephemeral=True)
            return

        data = load_data()
        user = get_user(data, str(self.user_id))
        record_id = get_next_id(data)

        record = {
            "id": record_id,
            "date": today_str(),
            "type": "study",
            "subject": self.subject.value,
            "material": "ポモドーロ",
            "minutes": 25,
            "problems": problems,
            "pages": pages,
            "timestamp": now_jst().isoformat()
        }
        user["records"].append(record)

        # ポモドーロのラウンドにも追加
        if str(self.user_id) in active_pomodoros:
            active_pomodoros[str(self.user_id)]["rounds"].append({
                "subject": self.subject.value,
                "problems": problems,
                "pages": pages,
                "id": record_id
            })

        save_data(data)

        if self.is_continue:
            await interaction.response.send_message(
                f"記録したで！（#{record_id}）\n5分休憩に入るで。休憩後にまた通知するわ。",
                ephemeral=True
            )
            # 5分休憩 → 次の25分
            asyncio.create_task(pomodoro_break_and_restart(self.user_id, interaction.channel_id))
        else:
            # 終了 → 一覧表示
            rounds = active_pomodoros.get(str(self.user_id), {}).get("rounds", [])
            lines = [f"#{r['id']} {r['subject']}  問題{r['problems']} / P{r['pages']}" for r in rounds]
            summary = "\n".join(lines) if lines else "記録なし"

            embed = discord.Embed(title="ポモドーロ終了", description=summary, color=0x57F287)
            embed.set_footer(text=f"合計 {len(rounds)} ポモドーロ")
            await interaction.response.send_message(embed=embed, ephemeral=True)

            # 終了処理
            if str(self.user_id) in active_pomodoros:
                del active_pomodoros[str(self.user_id)]

class PomodoroView(ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=300)
        self.user_id = user_id

    @ui.button(label="記録して続ける", style=discord.ButtonStyle.green)
    async def continue_btn(self, interaction: discord.Interaction, button: ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("あなたのもんと違うで。", ephemeral=True)
            return
        modal = PomodoroModal(self.user_id, is_continue=True)
        await interaction.response.send_modal(modal)

    @ui.button(label="記録して終了", style=discord.ButtonStyle.red)
    async def end_btn(self, interaction: discord.Interaction, button: ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("あなたのもんと違うで。", ephemeral=True)
            return
        modal = PomodoroModal(self.user_id, is_continue=False)
        await interaction.response.send_modal(modal)

# ======================
# ポモドーロのタイマー処理
# ======================
async def pomodoro_work(user_id: int, channel_id: int):
    await asyncio.sleep(25 * 60)  # 25分

    channel = bot.get_channel(channel_id)
    if channel is None:
        return

    view = PomodoroView(user_id)
    await channel.send(
        f"<@{user_id}> 25分終わったで！記録してな。",
        view=view
    )

async def pomodoro_break_and_restart(user_id: int, channel_id: int):
    await asyncio.sleep(5 * 60)  # 5分休憩

    channel = bot.get_channel(channel_id)
    if channel is None:
        return

    await channel.send(f"<@{user_id}> 休憩終わり！次の25分スタートやで。")
    # 次の25分を開始
    task = asyncio.create_task(pomodoro_work(user_id, channel_id))
    if str(user_id) in active_pomodoros:
        active_pomodoros[str(user_id)]["task"] = task

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
# ポモドーロ開始
# ======================
@bot.tree.command(name="pomodoro", description="ポモドーロを開始する（25分）")
async def pomodoro(interaction: discord.Interaction):
    uid = str(interaction.user.id)

    if uid in active_pomodoros:
        await interaction.response.send_message("すでにポモドーロ中やで。", ephemeral=True)
        return

    await interaction.response.send_message(
        f"{interaction.user.mention} ポモドーロ開始！25分カウントするで。"
    )

    task = asyncio.create_task(pomodoro_work(interaction.user.id, interaction.channel_id))
    active_pomodoros[uid] = {
        "task": task,
        "rounds": [],
        "channel_id": interaction.channel_id
    }

# ======================
# 勉強開始
# ======================
@bot.tree.command(name="start", description="勉強・活動を開始する")
@app_commands.describe(教科="教科を選択", 教材="教材名（任意）")
@app_commands.choices(教科=[app_commands.Choice(name=s, value=s) for s in SUBJECTS])
async def start(interaction: discord.Interaction, 教科: app_commands.Choice[str], 教材: str = ""):
    data = load_data()
    user = get_user(data, str(interaction.user.id))

    if user["active_session"] is not None:
        await interaction.response.send_message("すでに開始中やで。先に `/end` してな。", ephemeral=True)
        return

    user["active_session"] = {
        "subject": 教科.value,
        "material": 教材,
        "start_time": now_jst().isoformat()
    }
    save_data(data)

    await interaction.response.send_message(
        f"開始したで！\n教科: **{教科.value}**\n教材: **{教材 or 'なし'}**\n終了するときは `/end` やで。"
    )

# ======================
# 勉強終了
# ======================
@bot.tree.command(name="end", description="終了して記録する")
@app_commands.describe(問題数="解いた問題数（睡眠・仮眠・運動は0でOK）", ページ数="進めたページ数（睡眠・仮眠・運動は0でOK）")
async def end(interaction: discord.Interaction, 問題数: int = 0, ページ数: int = 0):
    data = load_data()
    user = get_user(data, str(interaction.user.id))

    if user["active_session"] is None:
        await interaction.response.send_message("開始してないで。先に `/start` してな。", ephemeral=True)
        return

    session = user["active_session"]
    start_time = datetime.fromisoformat(session["start_time"])
    end_time = now_jst()
    minutes = max(1, int((end_time - start_time).total_seconds() // 60))

    record_id = get_next_id(data)
    record = {
        "id": record_id,
        "date": today_str(),
        "type": "study" if session["subject"] not in ["運動", "睡眠", "仮眠"] else session["subject"],
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
        f"記録したで！（記録番号: **#{record_id}**）\n"
        f"教科: **{session['subject']}**（{session['material'] or 'なし'}）\n"
        f"時間: **{minutes}分**\n"
        f"問題数: **{問題数}** / ページ数: **{ページ数}**"
    )

# ======================
# 手入力で追加
# ======================
@bot.tree.command(name="add", description="後から記録を追加する")
@app_commands.describe(分="分数", 問題数="問題数", ページ数="ページ数", 教科="教科を選択")
@app_commands.choices(教科=[app_commands.Choice(name=s, value=s) for s in SUBJECTS])
async def add(interaction: discord.Interaction, 分: int, 教科: app_commands.Choice[str], 問題数: int = 0, ページ数: int = 0):
    data = load_data()
    user = get_user(data, str(interaction.user.id))

    record_id = get_next_id(data)
    record = {
        "id": record_id,
        "date": today_str(),
        "type": "study" if 教科.value not in ["運動", "睡眠", "仮眠"] else 教科.value,
        "subject": 教科.value,
        "material": "",
        "minutes": 分,
        "problems": 問題数,
        "pages": ページ数,
        "timestamp": now_jst().isoformat()
    }
    user["records"].append(record)
    save_data(data)

    await interaction.response.send_message(
        f"追加したで！（記録番号: **#{record_id}**）\n"
        f"教科: **{教科.value}**\n"
        f"時間: **{分}分** / 問題: **{問題数}** / ページ: **{ページ数}**"
    )

# ======================
# 運動記録
# ======================
@bot.tree.command(name="exercise", description="運動時間を記録する（運動レート用）")
@app_commands.describe(分="運動した分数")
async def exercise(interaction: discord.Interaction, 分: int):
    data = load_data()
    user = get_user(data, str(interaction.user.id))

    record_id = get_next_id(data)
    record = {
        "id": record_id,
        "date": today_str(),
        "type": "運動",
        "subject": "運動",
        "material": "",
        "minutes": 分,
        "problems": 0,
        "pages": 0,
        "timestamp": now_jst().isoformat()
    }
    user["records"].append(record)
    save_data(data)

    await interaction.response.send_message(f"運動 **{分}分** 記録したで！（番号: **#{record_id}**）")

# ======================
# その他の時間
# ======================
@bot.tree.command(name="break", description="勉強以外の時間を記録する（レート影響なし）")
@app_commands.describe(種類="飯・風呂・家事など", 分="分数")
async def break_time(interaction: discord.Interaction, 種類: str, 分: int):
    data = load_data()
    user = get_user(data, str(interaction.user.id))

    record_id = get_next_id(data)
    record = {
        "id": record_id,
        "date": today_str(),
        "type": "break",
        "subject": 種類,
        "category": 種類,
        "minutes": 分,
        "problems": 0,
        "pages": 0,
        "timestamp": now_jst().isoformat()
    }
    user["records"].append(record)
    save_data(data)

    await interaction.response.send_message(f"**{種類}** を **{分}分** 記録したで。（番号: **#{record_id}**・レート影響なし）")

# ======================
# 記録削除
# ======================
@bot.tree.command(name="clear", description="指定した番号の記録を削除する")
@app_commands.describe(番号="削除したい記録番号")
async def clear(interaction: discord.Interaction, 番号: int):
    data = load_data()
    user = get_user(data, str(interaction.user.id))

    for i, r in enumerate(user["records"]):
        if r.get("id") == 番号:
            deleted = user["records"].pop(i)
            save_data(data)
            await interaction.response.send_message(
                f"記録 **#{番号}** を削除したで。\n（{deleted.get('subject', '?')} {deleted.get('minutes', 0)}分）"
            )
            return

    await interaction.response.send_message(f"番号 **#{番号}** の記録は見つからんかった。", ephemeral=True)

# ======================
# 記録編集
# ======================
@bot.tree.command(name="edit", description="指定した番号の記録を編集する")
@app_commands.describe(番号="編集したい記録番号", 分="新しい分数", 問題数="新しい問題数", ページ数="新しいページ数")
async def edit(interaction: discord.Interaction, 番号: int, 分: int = None, 問題数: int = None, ページ数: int = None):
    data = load_data()
    user = get_user(data, str(interaction.user.id))

    for r in user["records"]:
        if r.get("id") == 番号:
            if 分 is not None:
                r["minutes"] = 分
            if 問題数 is not None:
                r["problems"] = 問題数
            if ページ数 is not None:
                r["pages"] = ページ数
            save_data(data)
            await interaction.response.send_message(
                f"記録 **#{番号}** を更新したで。\n"
                f"時間: **{r['minutes']}分** / 問題: **{r.get('problems', 0)}** / ページ: **{r.get('pages', 0)}**"
            )
            return

    await interaction.response.send_message(f"番号 **#{番号}** の記録は見つからんかった。", ephemeral=True)

# ======================
# 今日の記録
# ======================
@bot.tree.command(name="today", description="今日の自分の記録を見る")
async def today(interaction: discord.Interaction):
    data = load_data()
    user = get_user(data, str(interaction.user.id))
    today = today_str()

    today_records = [r for r in user["records"] if r.get("date") == today]

    embed = discord.Embed(title=f"今日の記録（{today}）", color=0x5865F2)
    embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)

    if not today_records:
        embed.description = "今日はまだ記録がないで。"
        await interaction.response.send_message(embed=embed)
        return

    total_study = 0
    total_problems = 0
    total_pages = 0
    total_exercise = 0
    total_sleep = 0
    total_nap = 0
    lines = []

    for r in sorted(today_records, key=lambda x: x.get("id", 0)):
        rid = r.get("id", "?")
        subj = r.get("subject", "?")
        mins = r.get("minutes", 0)

        if r.get("type") == "break":
            lines.append(f"`#{rid}` ☕ {subj}  {mins}分")
        elif subj == "運動":
            lines.append(f"`#{rid}` 🏃 運動  {mins}分")
            total_exercise += mins
        elif subj == "睡眠":
            lines.append(f"`#{rid}` 😴 睡眠  {mins}分")
            total_sleep += mins
        elif subj == "仮眠":
            lines.append(f"`#{rid}` 😪 仮眠  {mins}分")
            total_nap += mins
        else:
            lines.append(f"`#{rid}` 📖 {subj}  {mins}分 / 問{r.get('problems',0)} / P{r.get('pages',0)}")
            total_study += mins
            total_problems += r.get("problems", 0)
            total_pages += r.get("pages", 0)

    embed.description = "\n".join(lines) if lines else "記録なし"

    embed.add_field(name="勉強時間", value=f"**{total_study}分**", inline=True)
    embed.add_field(name="問題数", value=f"**{total_problems}**", inline=True)
    embed.add_field(name="ページ数", value=f"**{total_pages}**", inline=True)
    if total_exercise > 0:
        embed.add_field(name="運動", value=f"**{total_exercise}分**", inline=True)
    if total_sleep > 0:
        embed.add_field(name="睡眠", value=f"**{total_sleep}分**", inline=True)
    if total_nap > 0:
        embed.add_field(name="仮眠", value=f"**{total_nap}分**", inline=True)

    embed.add_field(name="勉強レート", value=f"**{user['study_rate']}**", inline=True)
    embed.add_field(name="運動レート", value=f"**{user['exercise_rate']}**", inline=True)
    embed.set_footer(text="番号を使って /clear や /edit ができます")

    await interaction.response.send_message(embed=embed)

# ======================
# stats
# ======================
@bot.tree.command(name="stats", description="自分のレートを確認する")
async def stats(interaction: discord.Interaction):
    data = load_data()
    user = get_user(data, str(interaction.user.id))

    embed = discord.Embed(title="レート", color=0x5865F2)
    embed.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
    embed.add_field(name="勉強レート", value=f"**{user['study_rate']}**", inline=True)
    embed.add_field(name="運動レート", value=f"**{user['exercise_rate']}**", inline=True)

    await interaction.response.send_message(embed=embed)

# ======================
# 起動
# ======================
bot.run(os.environ["DISCORD_TOKEN"])
