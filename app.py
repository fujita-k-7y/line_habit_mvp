import os
import csv
from io import StringIO
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import urllib.parse as urlparse
import hmac, hashlib, base64, logging
from typing import List, Tuple
import random, calendar

from fastapi import FastAPI, Request, HTTPException, Depends, Query, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse, PlainTextResponse, RedirectResponse
from pydantic import BaseModel
from dotenv import load_dotenv

from sqlalchemy import (create_engine, String, DateTime, Date, Integer, ForeignKey,
                        UniqueConstraint, select, func, case, literal)
from sqlalchemy.orm import declarative_base, Mapped, mapped_column, Session, sessionmaker, relationship

# --- LINE SDK (v3) ---
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3 import WebhookHandler
from linebot.v3.webhooks import MessageEvent, TextMessageContent, FollowEvent, PostbackEvent
from linebot.v3.messaging import MessagingApi, Configuration, ApiClient, ApiException, ErrorResponse
from linebot.v3.messaging.models import (
    ReplyMessageRequest, PushMessageRequest, TextMessage,
    QuickReply, QuickReplyItem, PostbackAction, FlexMessage, FlexContainer
)

# テーブルモデルおよびSeed API
import json
from pathlib import Path
from sqlalchemy import Boolean, CheckConstraint

# ============= 基本設定 =============
load_dotenv()
JST = ZoneInfo(os.getenv("TZ", "Asia/Tokyo"))
ADMIN_KEY = os.getenv("ADMIN_KEY", "dev-admin-key")
CRON_TOKEN = os.getenv("CRON_TOKEN", "dev-cron-token")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
LINE_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")

if not LINE_CHANNEL_SECRET or not LINE_ACCESS_TOKEN:
    print("[warn] LINEの環境変数が未設定です（Step1未完了なら無視OK）")


logger = logging.getLogger("app")

# ============= FastAPI =============
app = FastAPI(title="LINE Habit MVP", version="0.1.0")

# ============= DB（SQLite） =============
Base = declarative_base()
engine = create_engine(
    "sqlite:///./app.db",
    connect_args={"check_same_thread": False}  # FastAPIのスレッドと相性用
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


# 既存のimportsの近く（CheckConstraintは既にimport済み）
class CheckinEvent(Base):
    __tablename__ = "checkin_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String, index=True)
    behavior_id: Mapped[str] = mapped_column(String, ForeignKey("behaviors.id"), index=True)
    date: Mapped[date] = mapped_column(Date, index=True)  # JSTの“日付”
    result: Mapped[str] = mapped_column(String)           # 'did'|'didnt'|'pass'
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(JST))
    __table_args__ = (CheckConstraint("result in ('did','didnt','pass')", name="ck_event_result"),)


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

class Book(Base):
    __tablename__ = "books"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    title: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(JST))
    behaviors: Mapped[list["Behavior"]] = relationship("Behavior", back_populates="book", cascade="all, delete-orphan")

class Behavior(Base):
    __tablename__ = "behaviors"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    book_id: Mapped[str] = mapped_column(String, ForeignKey("books.id"), index=True)
    title: Mapped[str] = mapped_column(String, nullable=False)
    kind: Mapped[str] = mapped_column(String, nullable=False)  # 'action' or 'mind'
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(JST))
    book: Mapped[Book] = relationship("Book", back_populates="behaviors")

class UserBehavior(Base):
    __tablename__ = "user_behaviors"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String, index=True)
    behavior_id: Mapped[str] = mapped_column(String, ForeignKey("behaviors.id"), index=True)
    # schedule
    schedule_type: Mapped[str] = mapped_column(String, default="daily")  # 'daily'|'weekly'|'once'
    days_mask: Mapped[int] = mapped_column(Integer, default=127)         # 7bit: 月(1)<<0 .. 日(1)<<6, 127=毎日
    hour: Mapped[int] = mapped_column(Integer, default=21)               # 9 or 21（MVP）
    run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)  # once用
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(JST))
    __table_args__ = (
        UniqueConstraint("user_id", "behavior_id", name="uq_user_behavior"),
        CheckConstraint("schedule_type in ('daily','weekly','once')", name="ck_schedule_type"),
    )

class Checkin(Base):
    __tablename__ = "checkins"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String, index=True)
    behavior_id: Mapped[str] = mapped_column(String, ForeignKey("behaviors.id"), index=True)
    date: Mapped[date] = mapped_column(Date, index=True)
    result: Mapped[str] = mapped_column(String)  # 'did'|'didnt'|'pass'
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(JST))
    __table_args__ = (
        UniqueConstraint("user_id", "behavior_id", "date", name="uq_checkin"),
        CheckConstraint("result in ('did','didnt','pass')", name="ck_result"),
    )

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
# 既存の /webhook 定義を丸ごと差し替え
@app.api_route("/webhook", methods=["POST"], include_in_schema=True)
@app.api_route("/webhook/", methods=["POST"], include_in_schema=False)
async def webhook(request: Request, background_tasks: BackgroundTasks):
    signature = request.headers.get("X-Line-Signature", "")
    body = await request.body()

    # 署名が正しければ裏で処理する。正しくなくても 200 でACKだけは返す（Verifyを通すため）
    try:
        mac = hmac.new(LINE_CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256).digest()
        check = base64.b64encode(mac).decode("utf-8")
        if hmac.compare_digest(check, signature):
            background_tasks.add_task(handler.handle, body.decode("utf-8"), signature)
        else:
            logger.warning("Webhook signature invalid; acked 200 anyway")
    except Exception as e:
        logger.exception(f"Webhook error: {e}")

    return PlainTextResponse("OK")  # ← 常に200を返す

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
    with SessionLocal() as db:
        send_books_quickreply(user_id, db)

@handler.add(PostbackEvent)
def on_postback(event: PostbackEvent):
    user_id = event.source.user_id
    data = event.postback.data or ""
    # 既存の達成/未達（配信チェックイン）
    if "status=done" in data or "status=skip" in data:
        status = "done" if "status=done" in data else "skip"
        d = today_jst()
        with SessionLocal() as db:
            # 未登録保険
            if not db.get(User, user_id):
                db.add(User(user_id=user_id)); db.commit()
            result = upsert_daily_log(db, user_id, d, status)
        reply_text = "本日分を「達成」で記録しました。" if status=="done" else "本日分を「未達」で記録しました。"
        if result == "same":
            reply_text = reply_text.replace("で記録しました。", "は既に同じ内容で記録済みです。")
        elif result == "updated":
            reply_text = reply_text.replace("で記録しました。", "に更新しました。")
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(
                ReplyMessageRequest(replyToken=event.reply_token, messages=[TextMessage(text=reply_text)])
            )
        return

    # 新フロー：本選択・行動トグル
    params = parse_postback(data)
    op = params.get("op")
    if op == "pick_book":
        book_id = params.get("book")
        page = int(params.get("p", "0"))
        with SessionLocal() as db:
            if not db.get(User, user_id):
                db.add(User(user_id=user_id)); db.commit()
            send_behaviors_page(user_id, book_id, page, db)
        return
    if op == "toggle":
        book_id = params.get("book"); hid = params.get("hid"); page = int(params.get("p","0"))
        with SessionLocal() as db:
            if not db.get(User, user_id):
                db.add(User(user_id=user_id)); db.commit()
            ub = db.execute(select(UserBehavior).where(UserBehavior.user_id==user_id, UserBehavior.behavior_id==hid)).scalar_one_or_none()
            if ub:
                ub.enabled = not ub.enabled
            else:
                db.add(UserBehavior(user_id=user_id, behavior_id=hid, schedule_type="daily", hour=21, enabled=True))
            db.commit()
            send_behaviors_page(user_id, book_id, page, db)
        return
    if op == "finalize":
        book_id = params.get("book")
        with SessionLocal() as db:
            # 集計してメッセージ
            q = (select(Behavior.title)
                 .join(UserBehavior, UserBehavior.behavior_id==Behavior.id)
                 .where(UserBehavior.user_id==user_id, UserBehavior.enabled==True, Behavior.book_id==book_id))
            titles = db.execute(q).scalars().all()
        msg = "選択が完了しました。\n" + (("\n".join([f"・{t}" for t in titles])) if titles else "（選択なし）")
        msg += "\n\n※ 初期設定は「毎日21:00にリマインド」です。"
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(
                ReplyMessageRequest(replyToken=event.reply_token, messages=[TextMessage(text=msg)])
            )
        return

    # 3択（できた/できない/パス）の記録
    if op == "check":
        hid = params.get("hid")
        res = params.get("res")
        if res not in ("did", "didnt", "pass") or not hid:
            # 不正なデータは黙って捨てる
            with ApiClient(configuration) as api_client:
                MessagingApi(api_client).reply_message(
                    ReplyMessageRequest(replyToken=event.reply_token, messages=[TextMessage(text="入力を解釈できませんでした。")])
                )
            return
        d = today_jst()
        label_map = {"did": "できた", "didnt": "できない", "pass": "パス"}
        with SessionLocal() as db:
            if not db.get(User, user_id):
                db.add(User(user_id=user_id)); db.commit()
            beh = db.get(Behavior, hid)
            title = beh.title if beh else "（行動）"
            row = db.execute(select(Checkin).where(
                Checkin.user_id==user_id, Checkin.behavior_id==hid, Checkin.date==d
            )).scalar_one_or_none()
            if row:
                if row.result == res:
                    msg_core = "本日分は同じ結果で記録済みです（追記しました）。"
                else:
                    row.result = res
                    msg_core = "本日分の結果を更新し、追記しました。"
            else:
                db.add(Checkin(user_id=user_id, behavior_id=hid, date=d, result=res))
                msg_core = "本日分を記録し、追記しました。"
            # 追記イベントを1行保存
            db.add(CheckinEvent(user_id=user_id, behavior_id=hid, date=d, result=res))
            db.commit()
            # 今日その行動に対する累計回数を出す
            cnt_today = db.execute(
                select(func.count(CheckinEvent.id)).where(
                    CheckinEvent.user_id==user_id,
                    CheckinEvent.behavior_id==hid,
                    CheckinEvent.date==d,
                )
            ).scalar_one()
        reply_text = f"『{title}』に「{label_map[res]}」で応答しました。\n{msg_core}\n（本日 {cnt_today} 回目）"
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(
                ReplyMessageRequest(replyToken=event.reply_token, messages=[TextMessage(text=reply_text)])
            )
        return

@handler.add(MessageEvent, message=TextMessageContent)
def on_text(event: MessageEvent):
    user_id = event.source.user_id
    text = (event.message.text or "").strip().lower()
    # 本選択コマンド
    if text in ("本", "books", "book", "書籍"):
        with SessionLocal() as db:
            if not db.get(User, user_id):
                db.add(User(user_id=user_id)); db.commit()
            send_books_quickreply(user_id, db)
        return
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

def _should_send(u: UserBehavior, now: datetime) -> bool:
    h = getattr(u, "hour", 21)
    if u.schedule_type == "daily":
        return now.hour == h
    if u.schedule_type == "weekly":
        wd = now.weekday()  # Mon=0..Sun=6
        return ((u.days_mask >> wd) & 1) == 1 and now.hour == h
    if u.schedule_type == "once":
        return (u.run_at is not None) and abs((u.run_at - now).total_seconds()) <= 300
    return False

@app.post("/cron/dispatch_test")
def cron_dispatch_test(token: str = Query(...), when: str | None = Query(None), mode: str = Query("flex"), db: Session = Depends(get_db)):
    if token != CRON_TOKEN:
        raise HTTPException(status_code=403, detail="invalid token")
    if not when:
        now = datetime.now(JST)
    else:
        dt = datetime.fromisoformat(when)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=JST)  # naiveならJSTとして扱う
        now = dt.astimezone(JST)
    ubs = db.execute(select(UserBehavior).where(UserBehavior.enabled==True)).scalars().all()
    targets = [ub for ub in ubs if _should_send(ub, now)]
    pushed = 0
    # ユーザーごとにひとまとめ（1通=最大10バブル）
    by_user: dict[str, list[str]] = {}
    for ub in targets:
        by_user.setdefault(ub.user_id, []).append(ub.behavior_id)
    errors = 0
    with ApiClient(configuration) as api_client:
        api = MessagingApi(api_client)
        for uid, hids in by_user.items():
            # 行動ID -> (hid,title)
            items: List[Tuple[str, str]] = []
            for hid in hids:
                beh = db.get(Behavior, hid)
                items.append((hid, beh.title if beh else "（行動）"))
            # 10件ごとに分割して送信
            for pack in _chunks(items, 10):
                try:
                    if mode == "text":
                        txt = "今日の行動チェック\n" + "\n".join([f"・{t}" for _, t in pack])
                        api.push_message(PushMessageRequest(to=uid, messages=[TextMessage(text=txt)]))
                    else:
                        flex = build_flex_for_behaviors(pack)
                        api.push_message(PushMessageRequest(to=uid, messages=[flex]))
                    pushed += 1
                except ApiException as e:
                    errors += 1
                    logger.error(f"push failed: status={e.status} body={e.body}")
                except Exception as e:
                    errors += 1
                    logger.exception(f"push failed: {e}")
    return {"now": now.isoformat(), "candidates": len(ubs), "pushed": pushed, "errors": errors, "mode": mode}

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

# --- 管理用：Seed投入API ---
@app.post("/admin/seed")
def admin_seed(key: str = Query(...), db: Session = Depends(get_db)):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="forbidden")

    data_path = Path(__file__).parent / "data" / "books.json"
    if not data_path.exists():
        raise HTTPException(status_code=400, detail=f"seed file not found: {data_path}")

    payload = json.loads(data_path.read_text(encoding="utf-8"))
    books = payload.get("books", [])
    behaviors = payload.get("behaviors", [])

    # upsert: 既存→更新、未登録→追加
    b_up, h_up = 0, 0

    # Books
    for b in books:
        obj = db.get(Book, b["id"])
        if obj:
            if obj.title != b["title"]:
                obj.title = b["title"]
                b_up += 1
        else:
            db.add(Book(id=b["id"], title=b["title"]))
            b_up += 1

    # Behaviors
    for h in behaviors:
        obj = db.get(Behavior, h["id"])
        if obj:
            changed = False
            if obj.title != h["title"]:
                obj.title = h["title"]; changed = True
            if obj.book_id != h["book_id"]:
                obj.book_id = h["book_id"]; changed = True
            if obj.kind != h["kind"]:
                obj.kind = h["kind"]; changed = True
            if changed:
                h_up += 1
        else:
            db.add(Behavior(id=h["id"], book_id=h["book_id"], title=h["title"], kind=h["kind"]))
            h_up += 1

    db.commit()
    return {"books_upserted": b_up, "behaviors_upserted": h_up}

@app.get("/admin/user_behaviors")
def admin_user_behaviors(key: str = Query(...), db: Session = Depends(get_db)):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="forbidden")
    rows = db.execute(
        select(UserBehavior.user_id, UserBehavior.behavior_id, UserBehavior.enabled,
               UserBehavior.schedule_type, UserBehavior.hour)
    ).all()
    return {"items": [dict(user_id=r.user_id, behavior_id=r.behavior_id, enabled=r.enabled,
                           schedule_type=r.schedule_type, hour=r.hour) for r in rows]}

@app.get("/admin/books")
def admin_books(key: str = Query(...), db: Session = Depends(get_db)):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="forbidden")
    rows = db.execute(select(Book.id, Book.title)).all()
    return {"books": [dict(id=r.id, title=r.title) for r in rows]}

@app.post("/admin/simulate_day")
def admin_simulate_day(
    key: str = Query(...),
    d: str = Query(..., description="YYYY-MM-DD"),
    rate: float = Query(0.75, ge=0.0, le=1.0),
    pass_rate: float = Query(0.1, ge=0.0, le=1.0),
    times: int = Query(1, ge=1, le=60),
    db: Session = Depends(get_db)
):
    """有効な user_behaviors 全件に対し、その日のチェックインを一括生成（既存は上書き）"""
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="forbidden")
    day = date.fromisoformat(d)
    ubs = db.execute(select(UserBehavior).where(UserBehavior.enabled==True)).scalars().all()
    did, didnt, passed = 0, 0, 0
    for ub in ubs:
        last_res = None
        for _ in range(times):
            r = random.random()
            if r < rate:
                res = "did"; did += 1
            elif r < rate + pass_rate:
                res = "pass"; passed += 1
            else:
                res = "didnt"; didnt += 1
            # 追記イベント
            db.add(CheckinEvent(user_id=ub.user_id, behavior_id=ub.behavior_id, date=day, result=res))
            last_res = res
        # 日次サマリは最後の結果で更新
        row = db.execute(select(Checkin).where(
            Checkin.user_id==ub.user_id, Checkin.behavior_id==ub.behavior_id, Checkin.date==day
        )).scalar_one_or_none()
        if row:
            row.result = last_res
        else:
            db.add(Checkin(user_id=ub.user_id, behavior_id=ub.behavior_id, date=day, result=last_res))
    db.commit()
    return {"date": day.isoformat(), "targets": len(ubs), "events_per_target": times, "did": did, "didnt": didnt, "pass": passed}

@app.post("/admin/simulate_month")
def admin_simulate_month(
    key: str = Query(...),
    ym: str = Query(..., description="YYYY-MM"),
    rate: float = Query(0.75, ge=0.0, le=1.0),
    pass_rate: float = Query(0.1, ge=0.0, le=1.0),
    times: int = Query(1, ge=1, le=60),
    db: Session = Depends(get_db)
):
    """その月の全日について simulate_day を回す（高速デモ用）"""
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="forbidden")
    start, end = month_range(ym)
    total_targets = 0
    for i in range((end - start).days + 1):
        day = (start + timedelta(days=i)).isoformat()
        resp = admin_simulate_day(key=key, d=day, rate=rate, pass_rate=pass_rate, times=times, db=db)
        total_targets += resp["targets"]
    return {
        "ym": ym,
        "days": (end-start).days + 1,
        "avg_targets_per_day": total_targets/((end-start).days + 1),
        "events_per_target": times
    }

# “実績の絶対値”を確認できるデバッグAPI
@app.get("/admin/monthly_totals_debug")
def admin_monthly_totals_debug(key: str = Query(...), ym: str = Query(...), user_id: str | None = Query(None), db: Session = Depends(get_db)):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="forbidden")
    start, end = month_range(ym)
    base = select(
        func.count().label("events"),
        func.sum(case((CheckinEvent.result=="did",   1), else_=0)).label("did"),
        func.sum(case((CheckinEvent.result=="didnt", 1), else_=0)).label("didnt"),
        func.sum(case((CheckinEvent.result=="pass",  1), else_=0)).label("pass"),
    ).where(CheckinEvent.date>=start, CheckinEvent.date<=end)
    if user_id:
        base = base.where(CheckinEvent.user_id == user_id)
    row = db.execute(base).one()
    return {
        "ym": ym,
        "user_id": user_id,
        "events": int(row.events or 0),
        "did": int(row.did or 0),
        "didnt": int(row.didnt or 0),
        "pass": int(row.pass or 0),
    }


# --- postback解析 ---
def parse_postback(data: str) -> dict:
    try:
        return dict(urlparse.parse_qsl(data or ""))
    except Exception:
        return {}

# --- 送信ヘルパ ---
def send_text(user_id: str, text: str):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).push_message(
            PushMessageRequest(to=user_id, messages=[TextMessage(text=text)])
        )

def send_books_quickreply(user_id: str, db: Session):
    books = db.execute(select(Book).order_by(Book.title)).scalars().all()
    if not books:
        send_text(user_id, "（管理者へ）書籍マスタが空です。/admin/seed を実行してください。")
        return
    items = []
    for b in books[:13]:  # QuickReplyは最大13
        items.append(
            QuickReplyItem(action=PostbackAction(label=b.title[:20], data=f"op=pick_book&book={b.id}"))
        )
    qr = QuickReply(items=items)
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).push_message(
            PushMessageRequest(
                to=user_id,
                messages=[TextMessage(text="本を選んでください：", quickReply=qr)]
            )
        )

# --- 本・行動のUI ---
PAGE_SIZE = 5
def send_behaviors_page(user_id: str, book_id: str, page: int, db: Session):
    book = db.get(Book, book_id)
    if not book:
        send_text(user_id, "指定の本が見つかりません。")
        return
    beh_all = db.execute(select(Behavior).where(Behavior.book_id==book_id).order_by(Behavior.title)).scalars().all()
    if not beh_all:
        send_text(user_id, f"『{book.title}』の行動リストが未登録です。")
        return
    start = max(0, page*PAGE_SIZE); end = min(len(beh_all), start+PAGE_SIZE)
    slice_beh = beh_all[start:end]
    # 選択済み（enabled=1）のセット
    enabled_ids = set(
        db.execute(select(UserBehavior.behavior_id)
                   .where(UserBehavior.user_id==user_id,
                          UserBehavior.enabled==True)).scalars().all()
    )
    lines = [f"行動を選択（トグル）：", f"ページ {page+1}/{(len(beh_all)+PAGE_SIZE-1)//PAGE_SIZE}"]
    qritems = []
    for i, h in enumerate(slice_beh, start=1):
        mark = "☑" if h.id in enabled_ids else "☐"
        lines.append(f"{i}. {mark} {h.title}")
        qritems.append(
            QuickReplyItem(
                action=PostbackAction(
                    label=f"{mark} {h.title[:16]}",
                    data=f"op=toggle&book={book_id}&hid={h.id}&p={page}"
                )
            )
        )
    # ナビゲーション
    if end < len(beh_all):
        qritems.append(QuickReplyItem(action=PostbackAction(label="次へ ▶", data=f"op=pick_book&book={book_id}&p={page+1}")))
    if page > 0:
        qritems.append(QuickReplyItem(action=PostbackAction(label="◀ 前へ", data=f"op=pick_book&book={book_id}&p={page-1}")))
    qritems.append(QuickReplyItem(action=PostbackAction(label="決定", data=f"op=finalize&book={book_id}")))
    qr = QuickReply(items=qritems[:13])
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).push_message(
            PushMessageRequest(
                to=user_id,
                messages=[TextMessage(text="\n".join(lines), quickReply=qr)]
            )
        )

# Flex（カルーセル）を組み立てる（最大10バブル/通）
def build_flex_for_behaviors(items: List[Tuple[str, str]]) -> FlexMessage:
    """items: [(hid, title)] を Flex の Carousel JSON にしてから SDK で検証・生成"""
    bubbles = []
    for hid, title in items[:10]:
        bubbles.append({
            "type": "bubble",
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "md",
                "contents": [
                    {"type": "text", "text": title, "wrap": True, "weight": "bold"},
                    {
                        "type": "box",
                        "layout": "horizontal",
                        "spacing": "md",
                        "contents": [
                            {"type":"button","style":"primary","height":"sm",
                             "action":{"type":"postback","label":"できた","data":f"op=check&hid={hid}&res=did"}},
                            {"type":"button","style":"secondary","height":"sm",
                             "action":{"type":"postback","label":"できない","data":f"op=check&hid={hid}&res=didnt"}},
                            {"type":"button","style":"secondary","height":"sm",
                             "action":{"type":"postback","label":"パス","data":f"op=check&hid={hid}&res=pass"}}
                        ]
                    }
                ]
            }
        })
    contents_json = {"type": "carousel", "contents": bubbles}
    return FlexMessage(
        alt_text="今日の行動チェック",
        contents=FlexContainer.from_json(json.dumps(contents_json))
    )

def _chunks(seq: List, n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

# --- 月次サマリ(JSON & push) ---
def summarize_user_month(db: Session, user_id: str, ym: str):
    start, end = month_range(ym)
    # Behavior が削除/未Seedでもイベントは集計する（タイトル等はダミーに）
    title_co = func.coalesce(Behavior.title, literal("（削除済みの行動）"))
    kind_co  = func.coalesce(Behavior.kind,  literal("action"))
    q = (
        select(
            CheckinEvent.behavior_id,
            title_co.label("title"),
            kind_co.label("kind"),
            func.sum(case((CheckinEvent.result == "did",   1), else_=0)).label("did"),
            func.sum(case((CheckinEvent.result == "didnt", 1), else_=0)).label("didnt"),
            func.sum(case((CheckinEvent.result == "pass",  1), else_=0)).label("pass_cnt"),
            func.count().label("events"),
        )
        .select_from(CheckinEvent)
        .join(Behavior, Behavior.id == CheckinEvent.behavior_id, isouter=True)  # ← LEFT OUTER JOIN
        .where(
            CheckinEvent.user_id == user_id,
            CheckinEvent.date >= start,
            CheckinEvent.date <= end,
        )
        .group_by(CheckinEvent.behavior_id, title_co, kind_co)
    )
    rows = db.execute(q).all()
    items = []
    for r in rows:
        done = int(r.did or 0); skipped = int(r.didnt or 0); pss = int(r.pass_cnt or 0)
        total = int(r.events or 0)
        rate = (done / total) if total else 0.0
        items.append({
            "behavior_id": r.behavior_id,
            "title": r.title,
            "kind": r.kind,
            "did": done, "didnt": skipped, "pass": pss,
            "total": total,
            "rate": round(rate*100, 1),
            "grade": grade_by_rate(rate)
        })
    # rate降順で並べる
    items.sort(key=lambda x: (-x["rate"], -x["did"], x["title"]))
    return items

@app.get("/admin/monthly_summary")
def admin_monthly_summary(key: str = Query(...), ym: str = Query(...), user_id: str | None = Query(None), db: Session = Depends(get_db)):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="forbidden")
    if user_id:
        return {"ym": ym, "user_id": user_id, "items": summarize_user_month(db, user_id, ym)}
    # 全ユーザー
    users = db.execute(select(User.user_id)).scalars().all()
    return {"ym": ym, "users": {uid: summarize_user_month(db, uid, ym) for uid in users}}

def build_monthly_text(ym: str, items: List[dict]) -> str:
    if not items:
        return f"📅 {ym} の記録はありません。"
    # 全体集計（JOINの影響を避けるためイベントテーブル総和で）
    # ※ ここでDBを再参照できないので、items合計でもOKならそのままで構いません。
    total_events = sum(it["total"] for it in items)
    total_did = sum(it["did"] for it in items)
    overall_rate = round((total_did/total_events)*100, 1) if total_events else 0.0
    # 上位5件（達成率→達成回数→タイトルで並べ替え済み）
    icons = {"action": "🏃", "mind": "🧠"}
    grade_icon = {"S":"🏆", "A":"🎖️", "B":"👍", "C":"📝"}
    lines = [
        f"📅 {ym} の月次サマリ",
        f"合計応答：{total_events} 回　達成：{total_did} 回（{overall_rate}%）",
        "— 上位トピック —",
    ]
    for i, it in enumerate(items[:5], start=1):
        icon = icons.get(it.get("kind"), "•")
        gi = grade_icon.get(it["grade"], "•")
        lines.append(
            f"{i}. {icon}{it['title']} {gi}\n"
            f"   達成 {it['did']}/{it['total']}（{it['rate']}%）・できない {it['didnt']}・パス {it['pass']}"
        )
    return "\n".join(lines)

@app.post("/admin/push_monthly")
def admin_push_monthly(key: str = Query(...), ym: str = Query(...), db: Session = Depends(get_db)):
    if key != ADMIN_KEY:
        raise HTTPException(status_code=403, detail="forbidden")
    users = db.execute(select(User.user_id)).scalars().all()
    pushed, errors = 0, 0
    with ApiClient(configuration) as api_client:
        api = MessagingApi(api_client)
        for uid in users:
            items = summarize_user_month(db, uid, ym)
            try:
                api.push_message(PushMessageRequest(to=uid, messages=[TextMessage(text=build_monthly_text(ym, items))]))
                pushed += 1
            except ApiException as e:
                errors += 1; logger.error(f"push monthly failed: status={e.status} body={e.body}")
            except Exception as e:
                errors += 1; logger.exception(f"push monthly failed: {e}")
    return {"ym": ym, "users": len(users), "pushed": pushed, "errors": errors}

# --- ユーティリティ(月の範囲・評価) ---
def month_range(ym: str) -> tuple[date, date]:
    """'YYYY-MM' -> (first_date, last_date)"""
    y, m = map(int, ym.split("-"))
    last = calendar.monthrange(y, m)[1]
    return date(y, m, 1), date(y, m, last)

def grade_by_rate(rate: float) -> str:
    return "S" if rate >= 0.9 else ("A" if rate >= 0.7 else ("B" if rate >= 0.5 else "C"))
