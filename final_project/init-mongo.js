// init-mongo.js
// Этот файл выполняется при первом запуске MongoDB контейнера

// Переключаемся на базу данных sourcedb
db = db.getSiblingDB('sourcedb');

// Создаем новые коллекции
db.createCollection('userSessions');
db.createCollection('eventLogs');
db.createCollection('supportTickets');
db.createCollection('userRecommendations');
db.createCollection('moderationQueue');

// 1. Добавляем тестовые данные в коллекцию userSessions
db.userSessions.insertMany([
    {
        session_id: "sess_001",
        user_id: "user_123",
        start_time: new Date("2024-01-10T09:00:00Z"),
        end_time: new Date("2024-01-10T09:30:00Z"),
        pages_visited: ["/home", "/products", "/products/42", "/cart"],
        device: "mobile",
        actions: ["login", "view_product", "add_to_cart", "logout"]
    },
    {
        session_id: "sess_002",
        user_id: "user_456",
        start_time: new Date("2024-01-10T10:15:00Z"),
        end_time: new Date("2024-01-10T10:45:00Z"),
        pages_visited: ["/home", "/search", "/products/101", "/checkout"],
        device: "desktop",
        actions: ["login", "search", "view_product", "purchase"]
    },
    {
        session_id: "sess_003",
        user_id: "user_789",
        start_time: new Date("2024-01-10T11:30:00Z"),
        end_time: new Date("2024-01-10T12:00:00Z"),
        pages_visited: ["/home", "/profile", "/settings", "/support"],
        device: "tablet",
        actions: ["login", "view_profile", "update_settings", "open_support_chat"]
    }
]);

// 2. Добавляем тестовые данные в коллекцию eventLogs
db.eventLogs.insertMany([
    {
        event_id: "evt_1001",
        timestamp: new Date("2024-01-10T09:05:20Z"),
        event_type: "click",
        details: { page: "/products/42", element: "add_to_cart_button" }
    },
    {
        event_id: "evt_1002",
        timestamp: new Date("2024-01-10T10:20:15Z"),
        event_type: "search",
        details: { query: "ноутбук", filters: { price_range: "50000-100000", brand: "Apple" } }
    },
    {
        event_id: "evt_1003",
        timestamp: new Date("2024-01-10T11:45:30Z"),
        event_type: "error",
        details: { error_code: "500", error_message: "Internal Server Error", page: "/checkout" }
    },
    {
        event_id: "evt_1004",
        timestamp: new Date("2024-01-10T12:10:00Z"),
        event_type: "purchase",
        details: { order_id: "ORD-1001", total_amount: 125000, items_count: 3 }
    }
]);

// 3. Добавляем тестовые данные в коллекцию supportTickets
db.supportTickets.insertMany([
    {
        ticket_id: "ticket_789",
        user_id: "user_123",
        status: "open",
        issue_type: "payment",
        messages: [
            {
                sender: "user",
                message: "Не могу оплатить заказ.",
                timestamp: new Date("2024-01-09T12:00:00Z")
            },
            {
                sender: "support",
                message: "Пожалуйста, уточните способ оплаты.",
                timestamp: new Date("2024-01-09T13:00:00Z")
            }
        ],
        created_at: new Date("2024-01-09T11:55:00Z"),
        updated_at: new Date("2024-01-09T13:00:00Z")
    },
    {
        ticket_id: "ticket_790",
        user_id: "user_456",
        status: "resolved",
        issue_type: "delivery",
        messages: [
            {
                sender: "user",
                message: "Когда будет доставлен заказ?",
                timestamp: new Date("2024-01-08T09:30:00Z")
            },
            {
                sender: "support",
                message: "Заказ будет доставлен завтра.",
                timestamp: new Date("2024-01-08T10:15:00Z")
            },
            {
                sender: "user",
                message: "Спасибо за информацию!",
                timestamp: new Date("2024-01-08T10:30:00Z")
            }
        ],
        created_at: new Date("2024-01-08T09:30:00Z"),
        updated_at: new Date("2024-01-08T10:30:00Z")
    },
    {
        ticket_id: "ticket_791",
        user_id: "user_789",
        status: "in_progress",
        issue_type: "technical",
        messages: [
            {
                sender: "user",
                message: "Не загружается страница товара.",
                timestamp: new Date("2024-01-10T08:00:00Z")
            },
            {
                sender: "support",
                message: "Проверяем проблему. Какой у вас браузер?",
                timestamp: new Date("2024-01-10T08:30:00Z")
            }
        ],
        created_at: new Date("2024-01-10T08:00:00Z"),
        updated_at: new Date("2024-01-10T08:30:00Z")
    }
]);

// 4. Добавляем тестовые данные в коллекцию userRecommendations
db.userRecommendations.insertMany([
    {
        user_id: "user_123",
        recommended_products: ["prod_101", "prod_205", "prod_333"],
        last_updated: new Date("2024-01-10T08:00:00Z")
    },
    {
        user_id: "user_456",
        recommended_products: ["prod_102", "prod_208", "prod_315", "prod_401"],
        last_updated: new Date("2024-01-10T07:30:00Z")
    },
    {
        user_id: "user_789",
        recommended_products: ["prod_103", "prod_206", "prod_322", "prod_408", "prod_415"],
        last_updated: new Date("2024-01-10T09:15:00Z")
    }
]);

// 5. Добавляем тестовые данные в коллекцию moderationQueue
db.moderationQueue.insertMany([
    {
        review_id: "rev_555",
        user_id: "user_123",
        product_id: "prod_101",
        review_text: "Отличный товар, работает как нужно!",
        rating: 5,
        moderation_status: "pending",
        flags: ["contains_images"],
        submitted_at: new Date("2024-01-08T10:20:00Z")
    },
    {
        review_id: "rev_556",
        user_id: "user_456",
        product_id: "prod_102",
        review_text: "Товар хороший, но доставка задержалась на 2 дня.",
        rating: 4,
        moderation_status: "approved",
        flags: [],
        submitted_at: new Date("2024-01-09T14:30:00Z")
    },
    {
        review_id: "rev_557",
        user_id: "user_789",
        product_id: "prod_103",
        review_text: "Не соответствует описанию!",
        rating: 1,
        moderation_status: "pending",
        flags: ["contains_profanity", "needs_review"],
        submitted_at: new Date("2024-01-10T11:45:00Z")
    },
    {
        review_id: "rev_558",
        user_id: "user_123",
        product_id: "prod_205",
        review_text: "Хорошее соотношение цены и качества",
        rating: 4,
        moderation_status: "rejected",
        flags: ["spam", "duplicate"],
        submitted_at: new Date("2024-01-07T09:10:00Z")
    }
]);

// Создаем индексы для оптимизации запросов
// Индексы для userSessions
db.userSessions.createIndex({ "session_id": 1 }, { unique: true });
db.userSessions.createIndex({ "user_id": 1 });
db.userSessions.createIndex({ "start_time": 1 });

// Индексы для eventLogs
db.eventLogs.createIndex({ "event_id": 1 }, { unique: true });
db.eventLogs.createIndex({ "timestamp": 1 });
db.eventLogs.createIndex({ "event_type": 1 });

// Индексы для supportTickets
db.supportTickets.createIndex({ "ticket_id": 1 }, { unique: true });
db.supportTickets.createIndex({ "user_id": 1 });
db.supportTickets.createIndex({ "status": 1 });
db.supportTickets.createIndex({ "created_at": 1 });

// Индексы для userRecommendations
db.userRecommendations.createIndex({ "user_id": 1 }, { unique: true });
db.userRecommendations.createIndex({ "last_updated": 1 });

// Индексы для moderationQueue
db.moderationQueue.createIndex({ "review_id": 1 }, { unique: true });
db.moderationQueue.createIndex({ "moderation_status": 1 });
db.moderationQueue.createIndex({ "submitted_at": 1 });

// Создаем пользователя для приложения (не root)
db.createUser({
    user: 'app_user',
    pwd: 'app_password',
    roles: [
        { role: 'readWrite', db: 'sourcedb' }
    ]
});

print('MongoDB инициализация завершена!');
print('Созданы коллекции: userSessions, eventLogs, supportTickets, userRecommendations, moderationQueue');
print('Добавлены тестовые данные в каждую коллекцию');
print('Созданы индексы для оптимизации запросов');
print('Создан пользователь app_user');