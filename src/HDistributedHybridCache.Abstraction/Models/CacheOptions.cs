using System.ComponentModel;

namespace HDistributedHybridCache.Abstraction.Models;

public record CacheOptions
{
    // ============ Memory Cache Settings ============

    /// <summary>
    /// حداکثر ظرفیت (تعداد آیتم‌های) Memory Cache.
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>1024</c></remarks>
    [DefaultValue(1024L)]
    public long MemoryCacheMaxSize { get; set; } = 1024;

    /// <summary>
    /// مدت زمان انقضای پیش‌فرض برای آیتم‌های کش در حافظه.
    /// </summary>
    /// <remarks>
    /// مقدار پیش‌فرض: <c>5 دقیقه</c>.
    /// در فایل appsettings.json به صورت string وارد شود: <c>"00:05:00"</c>
    /// </remarks>
    [DefaultValue("00:05:00")]
    public TimeSpan DefaultMemoryTtl { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// درصد فشرده‌سازی (Compaction) زمانی که کش پر می‌شود.
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>0.2</c> (به معنی 20%)</remarks>
    [DefaultValue(0.2)]
    public double MemoryCacheCompactionPercentage { get; set; } = 0.2;

    // ============ Redis Settings ============

    /// <summary>
    /// آدرس اتصال به سرور Redis.
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>localhost:6379</c></remarks>
    [DefaultValue("localhost:6379")]
    public string RedisConnectionString { get; set; } = "localhost:6379";

    /// <summary>
    /// مدت زمان انقضای پیش‌فرض برای آیتم‌های کش در Redis.
    /// </summary>
    /// <remarks>
    /// مقدار پیش‌فرض: <c>20 دقیقه</c>.
    /// در فایل appsettings.json به صورت string وارد شود: <c>"00:20:00"</c>
    /// </remarks>
    [DefaultValue("00:20:00")]
    public TimeSpan DefaultRedisTtl { get; set; } = TimeSpan.FromMinutes(20);

    /// <summary>
    /// شماره دیتابیس Redis برای استفاده.
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>0</c></remarks>
    [DefaultValue(0)]
    public int RedisDatabase { get; set; } = 0;

    /// <summary>
    /// پیشوند برای تمام کلیدهای ذخیره شده در Redis.
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>""</c> (بدون پیشوند)</remarks>
    [DefaultValue("")]
    public string KeyPrefix { get; set; } = "";

    // ============ Retry Settings ============

    /// <summary>
    /// تعداد دفعات تلاش مجدد در صورت شکست اتصال به Redis.
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>3</c></remarks>
    [DefaultValue(3)]
    public int RedisRetryCount { get; set; } = 3;

    /// <summary>
    /// تاخیر پایه (به میلی‌ثانیه) بین هر بار تلاش مجدد.
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>100</c> میلی‌ثانیه</remarks>
    [DefaultValue(100)]
    public int RedisRetryBaseDelayMs { get; set; } = 100;

    /// <summary>
    /// مهلت اتصال به Redis (به میلی‌ثانیه).
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>5000</c> میلی‌ثانیه</remarks>
    [DefaultValue(5000)]
    public int RedisConnectTimeoutMs { get; set; } = 5000;

    // ============ Pub/Sub Settings ============

    /// <summary>
    /// پیشوند نام کانال برای پیام‌های Pub/Sub جهت بی‌اعتبارسازی کش.
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>cache:invalidate</c></remarks>
    [DefaultValue("cache:invalidate")]
    public string PubSubChannelPrefix { get; set; } = "cache:invalidate";

    /// <summary>
    /// آیا قابلیت Pub/Sub برای همگام‌سازی کش بین چندین نمونه (Instance) فعال باشد؟
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>true</c></remarks>
    [DefaultValue(true)]
    public bool EnablePubSub { get; set; } = true;

    // ============ HotKey Settings ============

    /// <summary>
    /// تعداد درخواست‌هایی که یک کلید باید در بازه زمانی مشخص داشته باشد تا به عنوان HotKey شناسایی شود.
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>10</c></remarks>
    [DefaultValue(10)]
    public int HotKeyThreshold { get; set; } = 10;

    /// <summary>
    /// بازه زمانی برای بررسی و شناسایی HotKey ها.
    /// </summary>
    /// <remarks>
    /// مقدار پیش‌فرض: <c>5 دقیقه</c>.
    /// در فایل appsettings.json به صورت string وارد شود: <c>"00:05:00"</c>
    /// </remarks>
    [DefaultValue("00:05:00")]
    public TimeSpan HotKeyDecayWindow { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// حداکثر تعداد HotKey هایی که همزمان می‌توانند ردیابی شوند.
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>1000</c></remarks>
    [DefaultValue(1000)]
    public int MaxHotKeys { get; set; } = 1000;

    /// <summary>
    /// آیا قابلیت ردیابی HotKey ها فعال باشد؟
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>true</c></remarks>
    [DefaultValue(true)]
    public bool EnableHotKeyTracking { get; set; } = true;

    // ============ Statistics Settings ============

    /// <summary>
    /// آیا آمار و عملکرد کش (مثل Hit/Miss) ثبت شود؟
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>true</c></remarks>
    [DefaultValue(true)]
    public bool EnableStatistics { get; set; } = true;

    /// <summary>
    /// آیا آمار به صورت Rolling Window (پنجره لغزان) محاسبه شود؟
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>true</c></remarks>
    [DefaultValue(true)]
    public bool EnableRollingWindow { get; set; } = true;

    /// <summary>
    /// بازه زمانی برای پنجره لغزان آمار.
    /// </summary>
    /// <remarks>
    /// مقدار پیش‌فرض: <c>1 دقیقه</c>.
    /// در فایل appsettings.json به صورت string وارد شود: <c>"00:01:00"</c>
    /// </remarks>
    [DefaultValue("00:01:00")]
    public TimeSpan StatisticsRollingWindow { get; set; } = TimeSpan.FromMinutes(1);

    // ============ Performance Settings ============

    /// <summary>
    /// آیا محافظت در برابر Cache Stampede (تقاضای همزمان برای یک کلید منقضی شده) فعال باشد؟
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>true</c></remarks>
    [DefaultValue(true)]
    public bool EnableCacheStampedeProtection { get; set; } = true;

    /// <summary>
    /// حداکثر زمان (به میلی‌ثانیه) برای نگهداری قفل جهت جلوگیری از Stampede.
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>5000</c> میلی‌ثانیه</remarks>
    [DefaultValue(5000)]
    public int StampedeLockTimeoutMs { get; set; } = 5000;

    /// <summary>
    /// آیا داده‌های کش قبل از ذخیره در Redis فشرده‌سازی (Compress) شوند؟
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>false</c></remarks>
    [DefaultValue(false)]
    public bool EnableCompression { get; set; } = false;

    /// <summary>
    /// بازه زمانی برای پاکسازی قفل‌های منقضی شده Stampede.
    /// </summary>
    /// <remarks>
    /// مقدار پیش‌فرض: <c>10 دقیقه</c>.
    /// در فایل appsettings.json به صورت string وارد شود: <c>"00:10:00"</c>
    /// </remarks>
    [DefaultValue("00:10:00")]
    public TimeSpan StampedeLockCleanupInterval { get; set; } = TimeSpan.FromMinutes(10);

    // ============ Null Cache (Cache Poisoning Prevention) ============

    /// <summary>
    /// آیا مقادیر Null نیز در کش ذخیره شوند (برای جلوگیری از Cache Poisoning و حملات به دیتابیس)؟
    /// </summary>
    /// <remarks>مقدار پیش‌فرض: <c>true</c></remarks>
    [DefaultValue(true)]
    public bool EnableNullCaching { get; set; } = true;

    /// <summary>
    /// مدت زمان اعتبار برای مقادیر Null کش شده.
    /// </summary>
    /// <remarks>
    /// مقدار پیش‌فرض: <c>30 ثانیه</c>.
    /// در فایل appsettings.json به صورت string وارد شود: <c>"00:00:30"</c>
    /// </remarks>
    [DefaultValue("00:00:30")]
    public TimeSpan NullCacheTtl { get; set; } = TimeSpan.FromSeconds(30);
}