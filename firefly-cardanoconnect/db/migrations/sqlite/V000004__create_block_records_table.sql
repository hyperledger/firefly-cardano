CREATE TABLE "block_records" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "listener_id" TEXT NOT NULL,
    "block_height" BIGINT NULL,
    "block_slot" BIGINT NULL,
    "block_hash" TEXT NOT NULL,
    "parent_hash" TEXT NULL,
    "transaction_hashes" TEXT NOT NULL,
    "rolled_back" TINYINT NOT NULL,
    FOREIGN KEY ("listener_id") REFERENCES "listeners" ("id")
)