namespace HDistributedHybridCache.Abstraction.Models;

public enum CacheStoreType : byte
{
    /// <summary>
    /// هرگز در Memory Cache ذخیره نشود (فقط Redis)
    /// مثال: توکن‌های موقتی، OTP، داده‌های یکبارمصرف
    /// </summary>
    NeverInMemory = 1,

    /// <summary>
    /// فقط در صورت داغ شدن (HotKey) در Memory ذخیره شود
    /// مثال: سشن کاربر، محصولات پربازدید
    /// </summary>
    HotKeyOnly = 2,

    /// <summary>
    /// همیشه در Memory ذخیره شود (اولویت با حافظه‌ی موجود)
    /// مثال: تنظیمات سیستمی، داده‌های مرجع
    /// </summary>
    PreferInMemory = 3,

    /// <summary>
    /// حتماً در Memory ذخیره شود (حتی اگر کش پر باشد، دیگران را حذف کن)
    /// مثال: داده‌های حیاتی، تنظیمات امنیتی
    /// </summary>
    MustInMemory = 4
}