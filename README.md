# market-data-adapter

market-data-service/
├── leader/
│   └── LeaderElectionService        # Redisson lock, heartbeat scheduler, on-elected callback
│
├── lseg/
│   ├── LsegAuthService              # OAuth2 token refresh, stores to Redis
│   ├── LsegConsumerFactory          # Creates OmmConsumer on leader election
│   └── MarketDataCallbackHandler   # implements OmmConsumerClient → on RefreshMsg, UpdateMsg
│
├── subscription/
│   ├── RicRegistryService           # CRUD on ric_registry (PostgreSQL)
│   └── SubscriptionManager         # Tracks active RICs in Redis, drives resubscription
│
├── publisher/
│   ├── MarketDataKafkaProducer      # exactly-once, keyed by RIC symbol
│   └── MarketDataEvent              # Kafka message schema (RIC, bid, ask, timestamp, source)
│
├── recovery/
│   ├── GapDetectionService          # Compares ric:last-published vs current time on startup
│   └── RecoveryEventPublisher      # Publishes GAP_DETECTED to market-data-control topic
│
└── health/
    └── LsegConnectionHealthIndicator  # Spring Actuator — LSEG stream status for readiness probe


    
