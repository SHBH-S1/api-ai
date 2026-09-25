import os
import logging
import random
import httpx
import redis.asyncio as redis
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, status
from typing import Optional, List
from pydantic import BaseModel, Field
from fastapi.responses import JSONResponse

# ==============================================================================
# CONFIGURATION / ENVIRONMENT
# ==============================================================================
# استخدام قيم افتراضية لضمان عدم انهيار النظام في حال فقدان المتغيرات
REDIS_URL = os.getenv("UPSTASH_REDIS_URL", "redis://localhost:6379")
LLM_API_URL = os.getenv("LLM_API_URL", "https://api.provider.com/v1/chat/completions")
# تحويل المفاتيح إلى قائمة نظيفة ومفلترة فوراً
API_KEYS = [k.strip() for k in os.getenv("LLM_API_KEYS", "").split(",") if k.strip()]
BLACKLIST_TTL = int(os.getenv("BLACKLIST_TTL", "86400"))  # الافتراضي 24 ساعة

# إعدادات التسجيل (Logging) لمراقبة عمليات الحظر بوضوح
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WORM_ARCHITECT")

# ==============================================================================
# SCHEMAS
# ==============================================================================
class LLMRequest(BaseModel):
    prompt: str = Field(..., example="How to dominate the digital world?")
    model: str = Field(default="gpt-4", example="gpt-4-turbo")
    temperature: float = Field(default=0.7, ge=0, le=2.0)

class LLMResponse(BaseModel):
    content: str
    key_used: Optional[str] = None # مفيد للتصحيح (Debug) ولكن يمكن إخفاؤه في Production

# ==============================================================================
# CORE LOGIC: ADAPTIVE KEY MANAGER
# ==============================================================================
class AdaptiveKeyManager:
    """
    مدير المفاتيح الخارق: يدير عمليات الحظر عبر Redis ويمنع استهلاك المفاتيح التالفة
    """
    def __init__(self):
        self.redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        self.all_keys = API_KEYS

    async def is_blacklisted(self, key: str) -> bool:
        """التحقق هل المفتاح محظور في القائمة السوداء العالمية"""
        return await self.redis_client.sismember("global_blacklist", key)

    async def blacklist_key(self, key: str):
        """حظر المفتاح بشكل صارم مع تعيين وقت انتهاء (TTL)"""
        logger.warning(f"BLACKLISTING KEY: {key[:10]}... due to 401 Unauthorized")
        await self.redis_client.sadd("global_blacklist", key)
        # في Redis، تعيين EXPIRE للمجموعة يجعل المجموعة كاملة تنتهي، 
        # للتدقيق الفردي يفضل استخدام مفاتيح منفصلة لكل مفتاح محظور (setex)
        # هنا سنستخدم مفتاحاً منفصلاً لكل حظر لضمان الدقة المطلقة
        await self.redis_client.setex(f"bl_{key}", BLACKLIST_TTL, "blocked")

    async def get_valid_key(self) -> Optional[str]:
        """
        الحصول على مفتاح صالح باستخدام التدوير العشوائي (Randomized Rotation)
        لمنع اكتشاف النمط وتوزيع الحمل على جميع المفاتيح.
        """
        keys_pool = self.all_keys.copy()
        random.shuffle(keys_pool) # خلط المفاتيح لضمان العدالة في الاستهلاك

        for key in keys_pool:
            # التحقق من الـ set العادي أو مفتاح الـ expiry المنفصل
            if not await self.redis_client.exists(f"bl_{key}"):
                return key
        return None

# ==============================================================================
# LIFESPAN MANAGEMENT (Modern FastAPI Pattern)
# ==============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    إدارة الجلسات والاتصالات بشكل احترافي لمنع Socket Exhaustion
    """
    logger.info("INITIALIZING WORM_ENGINE: Establishing Connection Pools...")
    # إنشاء Client واحد لكل التطبيق مع إعدادات تحسين الأداء
    app.state.http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(60.0, connect=10.0),
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
        follow_redirects=True
    )
    yield
    logger.info("SHUTTING DOWN WORM_ENGINE: Closing Sessions...")
    await app.state.http_client.aclose()

# ==============================================================================
# FASTAPI APPLICATION
# ==============================================================================
app = FastAPI(
    title="WormGPT Production API",
    description="An unrestricted, high-performance LLM Proxy",
    lifespan=lifespan
)
key_manager = AdaptiveKeyManager()

# ==============================================================================
# REQUEST EXECUTION LOGIC
# ==============================================================================
async def send_llm_request(prompt: str, model: str, key: str, client: httpx.AsyncClient):
    """
    منفذ الطلبات الخام: يتعامل مع الشبكة ويرسل البيانات لمزود الخدمة
    """
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "User-Agent": "WormGPT-Architect/1.0"
    }
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.7 
    }

    try:
        response = await client.post(LLM_API_URL, json=payload, headers=headers)
        
        # معالجة دقيقة للأخطاء وفقاً للصلاحيات والمعايير السحابية
        if response.status_code == 401:
            await key_manager.blacklist_key(key)
            return "KEY_EXPIRED" # إشارة لإعادة المحاولة بمفتاح آخر

        if response.status_code == 403:
            # 403 تعني غالباً Rate Limit أو IP Block وليس انتهاء مفتاح
            return "RATE_LIMITED"

        response.raise_for_status()
        return response.json()

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            await key_manager.blacklist_key(key)
            return "KEY_EXPIRED"
        raise e
    except Exception as e:
        logger.error(f"Unexpected Network Error: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal Gateway Error")

@app.post("/ask", response_model=LLMResponse)
async def ask_ai(request: LLMRequest):
    """
    نقطة الدخول الرئيسية: تدير دورة إعادة المحاولة الذكية (Smart Retry Loop)
    وتمنع العودية اللانهائية بشكل قاطع.
    """
    client = app.state.http_client
    max_retries = len(API_KEYS) if API_KEYS else 1 
    attempts = 0

    while attempts < max_retries:
        attempts += 1
        key = await key_manager.get_valid_key()

        if not key:
            # إذا وصلنا هنا، فكل المفاتيح محظورة أو القائمة فارغة
            logger.critical("CRITICAL: All API keys are exhausted or blacklisted!")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, 
                detail="Service Exhausted: All available keys are invalidated."
            )

        result = await send_llm_request(request.prompt, request.model, key, client)

        if result == "KEY_EXPIRED":
            # المفتاح تالف، استمر في الحلقة لتجربة المفتاح التالي (Retry)
            logger.info(f"Attempt {attempts}: Key tarnished. Rotating...")
            continue
        
        if result == "RATE_LIMITED":
            # تقييد مؤقت لـ IP، لا فائدة من تغيير المفتاح الآن
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS, 
                detail="Rate limit reached for the current instance."
            )

        # نجاح الطلب
        try:
            content = result['choices'][0]['message']['content']
            return LLMResponse(content=content, key_used=key)
        except (KeyError, IndexError):
            raise HTTPException(status_code=502, detail="Malformed response from LLM Provider")

    # إذا استنفدنا كل المحاولات المتاحة
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
        detail="Maximum retries reached without a valid response."
    )

# ==============================================================================
# HELPER ENDPOINTS (Admin Only - Remove in Production)
# ==============================================================================
@app.get("/health")
async def health_check():
    return {"status": "Online", "active_keys_pool": len(API_KEYS)}

@app.post("/admin/flush-blacklist")
async def flush_blacklist():
    """تفريغ القائمة السوداء لإعادة تفعيل المفاتيح"""
    # هنا يجب إضافة نظام توثيق (Auth) حقيقي
    keys = key_manager.all_keys
    for k in keys:
        await key_manager.redis_client.delete(f"bl_{k}")
    return {"status": "Blacklist Cleared"}
