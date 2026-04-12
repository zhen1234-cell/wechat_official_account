from flask import Flask, request, abort, make_response
import hashlib
import time
import xml.etree.ElementTree as ET
import requests
import traceback
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from collections import OrderedDict
import threading

app = Flask(__name__)



WECHAT_TOKEN = "wechat123456"

# ChatWiki配置Key（远端）
USE_CHATWIKI = True
CHATWIKI_API = "http://47.121.124.163:18080/open/chatMessage"
CHATWIKI_API_KEY = "MTczNTM0ODVkZmU5NWFhY2IzOTc4MDE0YzUwYzY1NThfcFJ4MThyMmR5MF8xNzc1NzEzOTI3"

# ChatWiki 最大等待时间
CHATWIKI_TIMEOUT = 40

# 微信单次请求超时边界，略大于 5 秒，让微信判定本次请求超时并重试
WECHAT_RETRY_TRIGGER_SECONDS = 5.2

# 最多吃 3 次微信重试
MAX_WECHAT_ATTEMPTS = 3

executor = ThreadPoolExecutor(max_workers=16)
_http = requests.Session()

#LOCATION_MAP = {
#    "图书馆": "图书馆位于学校东门附近，是主要学习场所。导航链接：https://www.amap.com/",
#    "食堂": "学校食堂位于宿舍区旁边，提供早中晚餐。导航链接：https://www.amap.com/",
#    "教学楼": "教学楼位于校园中心区域，靠近行政楼。导航链接：https://www.amap.com/",
#}

# 用户最终结果缓存
_result_lock = threading.Lock()
_result_cache = {}   # {openid: {"answer": "...", "expire_at": ts}}
RESULT_TTL_SECONDS = 300

# =========================
# 任务缓存
# key = dedupe_key
# value = {
#   "future": Future,
#   "attempts": int,
#   "openid": str,
#   "answer": str|None,
#   "error": str|None,
#   "created_at": int,
#   "expire_at": int
# }
# =========================
_task_lock = threading.Lock()
_task_cache = OrderedDict()
TASK_TTL_SECONDS = 20 * 60
TASK_MAX_SIZE = 10000


def log(msg):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


def check_signature(token: str, signature: str, timestamp: str, nonce: str) -> bool:
    arr = [token, timestamp, nonce]
    arr.sort()
    raw = "".join(arr)
    sha1 = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return sha1 == signature


def reply_text(to_user: str, from_user: str, content: str) -> str:
    now = int(time.time())
    return f"""<xml>
<ToUserName><![CDATA[{to_user}]]></ToUserName>
<FromUserName><![CDATA[{from_user}]]></FromUserName>
<CreateTime>{now}</CreateTime>
<MsgType><![CDATA[text]]></MsgType>
<Content><![CDATA[{content}]]></Content>
</xml>"""


def empty_response():
    """
    返回空串。若超过 5 秒才返回，微信通常会认为超时并重试。
    """
    resp = make_response("")
    resp.headers["Content-Type"] = "text/plain; charset=utf-8"
    return resp


def parse_wechat_xml(xml_bytes: bytes):
    root = ET.fromstring(xml_bytes)
    return {
        "ToUserName": root.findtext("ToUserName", default=""),
        "FromUserName": root.findtext("FromUserName", default=""),
        "CreateTime": root.findtext("CreateTime", default=""),
        "MsgType": root.findtext("MsgType", default=""),
        "MsgId": root.findtext("MsgId", default=""),
        "Content": root.findtext("Content", default=""),
        "PicUrl": root.findtext("PicUrl", default=""),
        "MediaId": root.findtext("MediaId", default=""),
        "Event": root.findtext("Event", default=""),
        "EventKey": root.findtext("EventKey", default="")
    }


def build_dedupe_key(msg):
    """
    优先用 MsgId。
    某些事件消息没有 MsgId，则退化为组合键。
    """
    msg_id = msg.get("MsgId")
    if msg_id:
        return f"msgid:{msg_id}"

    return "fallback:{from_user}:{create_time}:{msg_type}:{event}:{event_key}".format(
        from_user=msg.get("FromUserName", ""),
        create_time=msg.get("CreateTime", ""),
        msg_type=msg.get("MsgType", ""),
        event=msg.get("Event", ""),
        event_key=msg.get("EventKey", ""),
    )


def cleanup_ordered_cache(cache_obj, max_size):
    now_ts = int(time.time())

    expired_keys = []
    for k, v in cache_obj.items():
        expire_at = v.get("expire_at", 0)
        if expire_at < now_ts:
            expired_keys.append(k)
        else:
            break

    for k in expired_keys:
        cache_obj.pop(k, None)

    while len(cache_obj) > max_size:
        cache_obj.popitem(last=False)


def save_result(openid: str, answer: str):
    with _result_lock:
        _result_cache[openid] = {
            "answer": answer,
            "expire_at": int(time.time()) + RESULT_TTL_SECONDS
        }


def get_result(openid: str):
    with _result_lock:
        item = _result_cache.get(openid)
        if not item:
            return None
        if item["expire_at"] < int(time.time()):
            _result_cache.pop(openid, None)
            return None
        return item["answer"]


def get_or_create_task(task_key: str, msg: dict):
    now_ts = int(time.time())

    with _task_lock:
        cleanup_ordered_cache(_task_cache, TASK_MAX_SIZE)

        item = _task_cache.get(task_key)
        if item:
            item["attempts"] += 1
            item["expire_at"] = now_ts + TASK_TTL_SECONDS
            return item

        future = executor.submit(ask_chatwiki_sync, msg)
        item = {
            "future": future,
            "attempts": 1,
            "openid": msg.get("FromUserName", ""),
            "answer": None,
            "error": None,
            "created_at": now_ts,
            "expire_at": now_ts + TASK_TTL_SECONDS
        }
        _task_cache[task_key] = item
        return item


def set_task_answer(task_key: str, answer: str):
    with _task_lock:
        item = _task_cache.get(task_key)
        if item:
            item["answer"] = answer
            item["expire_at"] = int(time.time()) + TASK_TTL_SECONDS


def set_task_error(task_key: str, error_text: str):
    with _task_lock:
        item = _task_cache.get(task_key)
        if item:
            item["error"] = error_text
            item["expire_at"] = int(time.time()) + TASK_TTL_SECONDS


def remove_task(task_key: str):
    with _task_lock:
        _task_cache.pop(task_key, None)


def wait_until_retry_trigger(start_ts: float):
    elapsed = time.time() - start_ts
    remain = WECHAT_RETRY_TRIGGER_SECONDS - elapsed
    if remain > 0:
        time.sleep(remain)


def parse_chatwiki_answer(data: dict) -> str:
    inner = data.get("data", {}) if data.get("res") == 0 else data

    # 兼容Ct文档里的标准返回：data.answer / data.image
    if isinstance(inner, dict):
        answer = (
            inner.get("answer")
            or inner.get("raw_answer")
            or inner.get("content")
            or inner.get("text")
            or ""
        )

        images = inner.get("image") or inner.get("images") or []
        image_url = inner.get("image_url") or inner.get("img_url")

        links = []

        if image_url:
            links.append(str(image_url).strip())

        if isinstance(images, list):
            for item in images:
                if isinstance(item, str) and item.strip():
                    links.append(item.strip())
                elif isinstance(item, dict) and item.get("url"):
                    links.append(str(item["url"]).strip())

        if links:
            prefix = f"{str(answer).strip()}\n" if answer else ""
            return prefix + "图片结果：\n" + "\n".join(links)

        if answer:
            return str(answer).strip()

    if isinstance(inner, str) and inner.strip():
        return inner.strip()

    return "暂时没有查询到相关信息。"


def ask_chatwiki_sync(msg):
    headers = {
        "Authorization": f"Bearer {CHATWIKI_API_KEY}",
        "Content-Type": "application/json"
    }


    if msg["MsgType"] == "image":
        payload = {
            "content": [
                {
                    "type": "text",
                    "text": "请识别并回答这张图片相关内容"
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": msg.get("PicUrl", "")
                    }
                }
            ],
            "open_id": msg["FromUserName"],
            "stream": False,
            "global": {}
        }
    else:
        payload = {
            "content": (msg.get("Content") or "").strip(),
            "open_id": msg["FromUserName"],
            "stream": False,
            "global": {}
        }

    log(f"调用 ChatWiki payload={payload}")

    resp = _http.post(
        CHATWIKI_API,
        headers=headers,
        json=payload,
        timeout=CHATWIKI_TIMEOUT
    )
    resp.raise_for_status()

    data = resp.json()
    log(f"ChatWiki 响应: {data}")

    return parse_chatwiki_answer(data)


def wait_or_timeout_for_answer(task_key: str, task_item: dict, msg: dict, start_ts: float):
    """
    当前这次微信请求内尽量拿结果：
    - 第1/2次：拿不到就拖过 5 秒，让微信重试
    - 第3次：还拿不到就返回“查询结果”兜底
    """
    from_user = msg.get("FromUserName", "")
    to_user = msg.get("ToUserName", "")
    attempts = task_item["attempts"]
    future = task_item["future"]

    # 给 XML 回复预留一点时间，避免卡死在 5 秒边缘
    budget = max(0.0, 4.6 - (time.time() - start_ts))

    try:
        answer = future.result(timeout=budget)
        set_task_answer(task_key, answer)
        save_result(from_user, answer)
        remove_task(task_key)
        return reply_text(from_user, to_user, answer)

    except TimeoutError:
        if attempts < MAX_WECHAT_ATTEMPTS:
            log(f"任务 {task_key} 第 {attempts} 次未完成，等待微信重试")
            wait_until_retry_trigger(start_ts)
            return empty_response()

        # 第 3 次还没出，给用户兜底
        try:
            answer = future.result(timeout=0.3)
            set_task_answer(task_key, answer)
            save_result(from_user, answer)
            remove_task(task_key)
            return reply_text(from_user, to_user, answer)
        except Exception:
            fallback = "问题较复杂，正在处理中。\n请稍后发送“查询结果”获取答案。"
            return reply_text(from_user, to_user, fallback)

    except Exception as e:
        err = f"ChatWiki 调用失败: {e}\n{traceback.format_exc()}"
        log(err)
        set_task_error(task_key, str(e))
        remove_task(task_key)
        return reply_text(from_user, to_user, "问题较复杂，正在处理中。\n请稍后发送“查询结果”获取答案")


@app.route("/wx", methods=["GET", "POST"])
def wx():
    start_ts = time.time()

    signature = request.args.get("signature", "")
    timestamp = request.args.get("timestamp", "")
    nonce = request.args.get("nonce", "")
    echostr = request.args.get("echostr", "")

    # 微信原生验证
    if request.method == "GET":
        if check_signature(WECHAT_TOKEN, signature, timestamp, nonce):
            return echostr
        return abort(403)

    if not check_signature(WECHAT_TOKEN, signature, timestamp, nonce):
        return abort(403)

    try:
        msg = parse_wechat_xml(request.data)

        msg_type = msg.get("MsgType", "")
        from_user = msg.get("FromUserName", "")
        to_user = msg.get("ToUserName", "")
        content = (msg.get("Content") or "").strip()

        task_key = build_dedupe_key(msg)

        log(f"收到微信消息 key={task_key} type={msg_type} from={from_user} content={content}")

        # 订阅事件
        if msg_type == "event":
            event = (msg.get("Event") or "").lower()
            if event == "subscribe":
                return reply_text(
                    from_user,
                    to_user,
                    "欢迎关注校园智能问答服务平台。\n发送问题可进行智能问答。"
                )
            return "success"

        # 查询缓存结果
        if msg_type == "text" and content == "查询结果":
            cached = get_result(from_user)
            if cached:
                return reply_text(from_user, to_user, cached)
            return reply_text(from_user, to_user, "暂时还没有可领取的结果，请稍后再试。")

        # 本地短路
        if msg_type == "text":
            if not content:
                return reply_text(from_user, to_user, "请输入文字内容。")

            if content in ["人工", "客服", "帮助"]:
                return reply_text(
                    from_user,
                    to_user,
                    "当前账号暂不支持客服异步会话，请直接留言，我们会在系统内查看。"
                )

            for keyword, answer in LOCATION_MAP.items():
                if keyword in content:
                    return reply_text(from_user, to_user, answer)

        # 非文本/图片
        if msg_type not in ("text", "image"):
            return reply_text(from_user, to_user, "目前支持文本消息，也支持图片识别。")

        # 测试模式
        if not USE_CHATWIKI:
            if msg_type == "text":
                return reply_text(from_user, to_user, f"测试成功，你发送的是：{content}")
            return reply_text(from_user, to_user, "测试模式：已收到图片。")

        # 同一条微信消息只创建一个 ChatWiki 任务
        task_item = get_or_create_task(task_key, msg)

        # 如果已有完成结果，直接回
        if task_item.get("answer"):
            answer = task_item["answer"]
            save_result(from_user, answer)
            remove_task(task_key)
            return reply_text(from_user, to_user, answer)

        # 如果已有错误
        if task_item.get("error"):
            remove_task(task_key)
            return reply_text(from_user, to_user, "系统繁忙，请稍后再试一下吧。")

        # 当前请求尽量拿结果，否则吃微信重试
        return wait_or_timeout_for_answer(task_key, task_item, msg, start_ts)

    except Exception as e:
        log(f"微信回调处理异常: {e}\n{traceback.format_exc()}")
        return "success"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6000, threaded=True)