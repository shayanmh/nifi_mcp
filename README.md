# سرور MCP برای NiFi 2.7.2

سرور stdio برای Cursor که به NiFi امن‌شدهٔ شما روی `https://127.0.0.1:8443/nifi-api` و به NiFi Registry روی `http://127.0.0.1:18081/nifi-registry-api` وصل می‌شود.

مسیرهای REST و شکل ابزارها از [newen-systems/nifi-mcp](https://github.com/newen-systems/nifi-mcp) گرفته شده است. این نسخه برای compose تک‌نود شما تنظیم شده و این کارها را انجام نمی‌دهد:

- start و stop پروسسور، process group یا پورت
- enable و disable کردن controller service
- خالی کردن صف (`drop-requests`)
- Template و Variable Registry (در NiFi 2 حذف شده‌اند)
- نوشتن در Registry

ورود فقط با کاربر single-user است: `POST /nifi-api/access/token` و بعد `Authorization: Bearer`. اگر نسخهٔ NiFi با `2.7` شروع نشود، سرور خطا می‌دهد. `2.7.2` بدون هشدار قبول می‌شود و بقیهٔ پچ‌های `2.7` با هشدار.

## راه‌اندازی

نیاز به [uv](https://docs.astral.sh/uv/) و Python 3.11 یا جدیدتر است.

```powershell
cd e:\Projects\MCP\nifi_mcp
copy .env.example .env
uv sync --extra dev
```

در `.env` مقدار `NIFI_USERNAME` و `NIFI_PASSWORD` را از همان single-user در compose نایفای بگذارید. `NIFI_VERIFY_SSL=false` به‌خاطر گواهی self-signed ایمیج رسمی است. `NIFI_SENSITIVE_PROPS_KEY` مال خود NiFi است و این سرور آن را نمی‌خواند. فایل `.env` در git نمی‌آید و رمز را داخل تنظیم MCP کلاینت نگذارید.

اگر کانتینر `nifi-registry2` خاموش است، `NIFI_REGISTRY_ENABLED=false` بگذارید تا ابزارهای Registry نمایش داده نشوند. روی ویندوز `NIFI_USE_SYSTEM_PROXY=false` بماند، وگرنه پراکسی سیستم اتصال TLS به `127.0.0.1` را خراب می‌کند.

برای تست سرور بدون Cursor: `uv run python tests/stdio_smoke.py`

سرور در تنظیمات سراسری Cursor (`%USERPROFILE%\.cursor\mcp.json`) با پایتونِ `.venv` همین پروژه ثبت شده است:

```json
"nifi-mcp": {
  "command": "E:\\Projects\\MCP\\nifi_mcp\\.venv\\Scripts\\python.exe",
  "args": ["-m", "nifi_mcp"]
}
```

سرور `.env` را از ریشهٔ همین پروژه می‌خواند، فرقی ندارد Cursor آن را از کدام پوشه اجرا کند. بعد از ساخت `.env` یک بار MCP را در Cursor خاموش و روشن کنید.

## ساخت فلو

اول `nifi_about` را صدا بزنید. نوع پروسسور را با `nifi_list_processor_types` پیدا کنید و نام propertyهای 2.7 را از `nifi_get_processor_definition` بردارید. برای ساخت یک‌جای یک گروه، `nifi_apply_flow_spec` یک process group فرزند می‌سازد و پروسسورها را متوقف رها می‌کند. مقدار `@Name` در property به controller service همان spec اشاره می‌کند.

اگر کامپوننت در حال اجرا باشد، حذف همان خطای NiFi را برمی‌گرداند. سرور برای حذف، آن را متوقف نمی‌کند.

آپدیت Parameter Context از update-request ناهمگام NiFi رد می‌شود. خود NiFi ممکن است کامپوننت‌های وابسته را در طول این درخواست ری‌استارت کند. مقدار parameter حساس در پاسخ ابزارها برنمی‌گردد.

## تست

```powershell
uv run pytest
```
