import os
import csv
from io import StringIO
from datetime import datetime, date
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, HTTPException, Depends, Query, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse, PlainTextResponse, RedirectResponse
from pydantic import BaseModel
from dotenv import load_dotenv

from sqlalchemy import (create_engine, String, DateTime, Date, Integer, ForeignKey,
                        UniqueConstraint, select, func)
from sqlalchemy.orm import declarative_base, Mapped, mapped_column, Session, sessionmaker, relationship

# --- LINE SDK (v3) ---
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3 import WebhookHandler
from linebot.v3.webhooks import MessageEvent, TextMessageContent, FollowEvent, PostbackEvent
from linebot.v3.messaging import MessagingApi, Configuration, ApiClient
from linebot.v3.messaging.models import (
    ReplyMessageRequest, PushMessageRequest, TextMessage,
    QuickReply, QuickReplyItem, PostbackAction
)

# ============= 基本設定 =============
load_dotenv()
JST = ZoneInfo(os.getenv("TZ", "Asia/Tokyo"))
ADMIN_KEY = os.getenv("ADMIN_KEY", "dev-admin-key")
CRON_TOKEN = os.getenv("CRON_TOKEN", "dev-cron-token")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
LINE_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")

if not LINE_CHANNEL_SECRET or not LINE_ACCESS_TOKEN:
    print("[warn] LINEの環境変数が未設定です（Step1未完了なら無視OK）")

# ============= FastAPI =============
app = FastAPI(title="LINE Habit MVP", version="0.1.0")

# ============= DB（SQLite） =============
Base = declarative_base()
engine = create_engine(
    "sqlite:///./app.db",
    connect_args={"check_same_thread": False}  # FastAPIのスレッドと相性用
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

class User(Base):
    __tablename__ = "users"
    user_id: Mapped[str] = mapped_column(String, primary_key=True)
    display_name: Mapped[str | None] = mapped_column(String, nullable=True)
    followed_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(JST))
    logs: Mapped[list["Log"]] = relationship("Log", back_populates="user")

class Log(Base):
    __tablename__ = "logs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String, ForeignKey("users.user_id"), index=True)
    date: Mapped[date] = mapped_column(Date, index=True)
    status: Mapped[str] = mapped_column(String)  # 'done' or 'skip'
    memo: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(JST))
    user: Mapped[User] = relationship("User", back_populates="logs")

    __table_args__ = (UniqueConstraint("user_id", "date", name="uq_user_date"),)

Base.metadata.create_all(bind=engine)

def upsert_daily_log(db: Session, user_id: str, d: date, status: str) -> str:
    """
    returns: 'created' | 'updated' | 'same'
    """
    row = db.execute(
        select(Log).where(Log.user_id == user_id, Log.date == d)
    ).scalar_one_or_none()
    if row:
        if row.status == status:
            return "same"
        row.status = status
        db.commit()
        return "updated"
    else:
        db.add(Log(user_id=user_id, date=d, status=status))
        db.commit()
        return "created"

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ============= LINE API クライアント / ハンドラ =============
handler = WebhookHandler(LINE_CHANNEL_SECRET)
configuration = Configuration(access_token=LINE_ACCESS_TOKEN)

def line_api() -> MessagingApi:
    """with ApiClient(...) を毎回書くのが面倒なのでヘルパー。"""
    api_client = ApiClient(configuration)
    return MessagingApi(api_client)

def today_jst() -> date:
    return datetime.now(JST).date()

WEEK = "月火水木金土日"

def make_checkin_message() -> TextMessage:
    """達成/未達のクイックリプライを付けた本文を作成"""
    qr = QuickReply(items=[
        QuickReplyItem(action=PostbackAction(label="達成", data="status=done")),
        QuickReplyItem(action=PostbackAction(label="未達", data="status=skip")),
    ])
    d = datetime.now(JST).date()
    w = WEEK[d.weekday()]
    return TextMessage(text=f"{d.isoformat()}（{w}）のチェックイン ✅\n達成/未達を選んでください。", quickReply=qr)

# ============= ルーティング =============
@app.get("/healthz")
def healthz():
    return {"ok": True, "now": datetime.now(JST).isoformat()}

@app.get("/")
def root():
    return RedirectResponse(url="/docs")

# --- Webhook（LINE -> 当アプリ） ---
@app.post("/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    sig = request.headers.get("X-Line-Signature")
    body = (await request.body()).decode("utf-8")
    try:
        # ここでは検証だけして…
        handler.handle  # 呼ぶのは後ろ
    except Exception:
        pass
    # ← 応答を先に返す
    resp = PlainTextResponse("OK")
    # ← 実処理はバックグラウンドへ（sig付きで）
    background_tasks.add_task(handler.handle, body, sig)
    return resp

# --- イベントハンドラ（Follow / Postback / Text） ---
@handler.add(FollowEvent)
def on_follow(event: FollowEvent):
    user_id = event.source.user_id
    with SessionLocal() as db:
        # 既に存在するならスキップ
        u = db.get(User, user_id)
        if not u:
            display_name = None
            try:
                with ApiClient(configuration) as api_client:
                    api = MessagingApi(api_client)
                    prof = api.get_profile(user_id)
                    display_name = prof.display_name
            except Exception:
                pass
            u = User(user_id=user_id, display_name=display_name)
            db.add(u)
            db.commit()
        # 挨拶＋使い方
        with ApiClient(configuration) as api_client:
            api = MessagingApi(api_client)
            api.push_message(
                PushMessageRequest(
                    to=user_id,
                    messages=[
                        TextMessage(text="友だち追加ありがとうございます！毎朝8時にチェックインを送ります。\n（メニューが届かない場合は『手動』で /cron を叩いてテスト中です）"),
                        make_checkin_message()
                    ]
                )
            )

@handler.add(PostbackEvent)
def on_postback(event: PostbackEvent):
    user_id = event.source.user_id
    data = event.postback.data or ""
    status = "done" if "status=done" in data else ("skip" if "status=skip" in data else None)
    if not status:
        return

    d = today_jst()
    with SessionLocal() as db:
        # 未登録なら保険で登録
        if not db.get(User, user_id):
            db.add(User(user_id=user_id))
            db.commit()

        result = upsert_daily_log(db, user_id, d, status)

    if result == "same":
        reply_text = f"本日分は既に「{'達成' if status=='done' else '未達'}」で記録済みです。"
    elif result == "updated":
        reply_text = f"本日分を「{'達成' if status=='done' else '未達'}」に更新しました。"
    else:  # created
        reply_text = f"本日分を「{'達成' if status=='done' else '未達'}」で記録しました。"

    with ApiClient(configuration) as api_client:
        api = MessagingApi(api_client)
        api.reply_message(
            ReplyMessageRequest(
                replyToken=event.reply_token,
                messages=[TextMessage(text=reply_text)]
            )
        )

@handler.add(MessageEvent, message=TextMessageContent)
def on_text(event: MessageEvent):
    user_id = event.source.user_id
    text = (event.message.text or "").strip().lower()
    if text in ("done", "達成", "y", "yes"):
        status = "done"
    elif text in ("skip", "未達", "n", "no"):
        status = "skip"
    else:
        with ApiClient(configuration) as api_client:
            api = MessagingApi(api_client)
            api.reply_message(
                ReplyMessageRequest(
                    replyToken=event.reply_token,
                    messages=[make_checkin_message()]
                )
            )
        return

    d = today_jst()
    with SessionLocal() as db:
        if not db.get(User, user_id):
            db.add(User(user_id=user_id))
            db.commit()
        result = upsert_daily_log(db, user_id, d, status)

    if result == "same":
        reply_text = f"本日分は既に「{'達成' if status=='done' else '未達'}」で記録済みです。"
    elif result == "updated":
        reply_text = f"本日分を「{'達成' if status=='done' else '未達'}」に更新しました。"
    else:
        reply_text = f"本日分を「{'達成' if status=='done' else '未達'}」で記録しました。"

    with ApiClient(configuration) as api_client:
        api = MessagingApi(api_client)
        api.reply_message(
            ReplyMessageRequest(
                replyToken=event.reply_token,
                messages=[TextMessage(text=reply_text)]
            )
        )

# --- 日次配信（GitHub Actions から叩く） ---
@app.post("/cron/daily")
def cron_daily(token: str = Query(...), db: Session = Depends(get_db)):
    if token != CRON_TOKEN:
        raise HTTPException(status_code=403, detail="invalid token")
    users = db.execute(select(User.user_id)).scalars().all()
    if not users:
        return {"pushed": 0}
    pushed = 0
    with ApiClient(configuration) as api_client:
        api = MessagingApi(api_client)
        msg = make_checkin_message()
        for uid in users:
            try:
                api.push_message(PushMessageRequest(to=uid, messages=[msg]))
                pushed += 1
            except Exception:
                # 個別失敗はスキップ（無効ユーザなど）
                pass
    return {"pushed": pushed}

# --- 管理用：当日達成率の簡易API ---
class TodayStats(BaseModel):
    date: date
    total_users: int
    responded: int
    done: int
    rate_percent: float

@app.get("/admin/today", response_model=TodayStats)
def admin_today(key: str = Query(...), db: Session = Depends(get_db)):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="forbidden")
    d = today_jst()
    total = db.execute(select(func.count(User.user_id))).scalar_one()
    # 回答者数
    responded = db.execute(select(func.count(Log.id)).where(Log.date == d)).scalar_one()
    done = db.execute(select(func.count(Log.id)).where(Log.date == d, Log.status == "done")).scalar_one()
    rate = (done / total * 100.0) if total else 0.0
    return TodayStats(date=d, total_users=total, responded=responded, done=done, rate_percent=round(rate, 1))

# --- 管理用：CSVエクスポート ---
@app.get("/admin/export")
def admin_export(key: str = Query(...), db: Session = Depends(get_db)):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="forbidden")

    rows = db.execute(
        select(Log.user_id, Log.date, Log.status, Log.created_at)
        .order_by(Log.date.desc(), Log.user_id)
    ).all()

    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(["user_id", "date", "status", "created_at"])
    for r in rows:
        writer.writerow([r.user_id, r.date.isoformat(), r.status, r.created_at.isoformat()])
    buf.seek(0)

    headers = {"Content-Disposition": 'attachment; filename="logs.csv"'}
    return StreamingResponse(iter([buf.read()]), media_type="text/csv", headers=headers)
